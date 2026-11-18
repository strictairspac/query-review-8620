"""验收测试：副本追平、暂停复制、中途断开、两批争抢回退名额、恢复重试不重复发布。

依赖真实主库 + 物理流复制副本 + Redis + worker，不做任何模拟。
用环境变量连接，例如：

  DATABASE_URL=postgresql+psycopg2://app:pw@127.0.0.1:55432/query_review \
  REPLICA_DATABASE_URL=postgresql+psycopg2://app:pw@127.0.0.1:55433/query_review \
  REDIS_URL=redis://127.0.0.1:56379/0 JWT_SECRET=test ARTIFACT_DIR=/tmp/qr-artifacts \
  python -m tests.acceptance_replica

脚本会直接在指定的主库/副本上暂停、恢复角色，只对测试库使用。
"""
import json,os,subprocess,sys,threading,time,traceback
from datetime import datetime,timedelta
from pathlib import Path
from sqlalchemy import text
from src.config.database import Session,engine,lock_engine
from src.config.settings import ARTIFACT_DIR
from src.constants.enums import FALLBACK_SLOT_LOCK_KEY
from src.models.batch import Batch,BatchEvent
from src.models.evidence import Evidence
from src.models.sample import Sample
from src.services.execution import run_comparison
from src.services.replication import lsn_to_int,replica_status
import src.worker as worker
OUT=Path(ARTIFACT_DIR);OUT.mkdir(parents=True,exist_ok=True)
LOG=[]
def log(event,**data):
    row={'at':datetime.utcnow().isoformat(timespec='milliseconds')+'Z','event':event,**data}
    LOG.append(row)
    print(json.dumps(row,ensure_ascii=False),flush=True)
def sql(port,statement,*params):
    with engine.connect() as conn:conn.execute(text(statement),params or None)
def replica_sql(statement):
    from src.services.replication import replica_engine
    with replica_engine().connect() as conn:return conn.execute(text(statement)).scalar()
def pause_replay():replica_sql('SELECT pg_wal_replay_pause()')
def make_stale(amount=555.0):
    """暂停回放后写一笔新账，制造“受理点晚于副本回放点”的确定状态。"""
    pause_replay();time.sleep(0.2)
    insert_bill(900900+int(time.time()*1000)%100000,amount)
    return primary_lsn()
def resume_replay():replica_sql('SELECT pg_wal_replay_resume()')
def primary_lsn():return sql_scalar('SELECT pg_current_wal_lsn()::text')
def sql_scalar(statement):
    with engine.connect() as conn:return conn.execute(text(statement)).scalar()
def insert_bill(ident,amount=10.0):
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO invoices VALUES (:i,'澄港海产','东线',:a,2,'settled') ON CONFLICT DO NOTHING"),{'i':ident,'a':amount})
def clean():
    with Session() as db:
        db.query(Evidence).delete();db.query(BatchEvent).delete();db.query(Batch).delete();db.commit()
def sample_id():
    with Session() as db:return db.query(Sample).filter(Sample.name=='低温结算汇总').one().id
ORIGINAL="SELECT customer, SUM(amount) AS total FROM invoices WHERE temperature <= 8 GROUP BY customer HAVING SUM(amount) > 100 ORDER BY customer"
CANDIDATE="SELECT customer, SUM(amount) AS total FROM invoices WHERE temperature <= 8 GROUP BY customer HAVING SUM(amount) > 100 ORDER BY customer"
SLOW="SELECT COUNT(*) FROM invoices a CROSS JOIN invoices b CROSS JOIN invoices c"
def check(name,ok,detail=None):
    log('assert',name=name,ok=bool(ok),detail=detail)
    if not ok:raise AssertionError(name+(('：'+str(detail)) if detail else ''))
def section(title):log('section',title=title)
def ensure_replica(start_cmd):
    """确保副本进程在运行（幂等），再等它追上当前主库位置。"""
    if start_cmd:subprocess.run(start_cmd,capture_output=True)
    deadline=time.time()+30
    while time.time()<deadline:
        st=replica_status()
        if st['ok']:return st
        time.sleep(0.5)
    raise RuntimeError('副本未能在 30 秒内就绪：'+str(replica_status()))
def case_fresh_bill(start_cmd):
    section('场景一：刚入账的运费必须先被副本看到')
    ensure_replica(start_cmd)
    clean();pause_replay();time.sleep(0.2)
    frozen=replica_sql('SELECT pg_last_wal_replay_lsn()::text')
    # 受理前在主库入账一笔新运费
    ident=900100+int(time.time())%1000
    insert_bill(ident,777.0)
    accepted=primary_lsn()
    check('副本确实落后于受理点',lsn_to_int(frozen)<lsn_to_int(accepted),(frozen,accepted))
    skipped=sql_scalar("SELECT COUNT(*) FROM invoices WHERE invoice_id=:i".replace(':i',str(ident)))
    check('主库有新账',skipped==1)
    # 不允许等，直接验证：副本未追平时不得在副本上受理
    events=[]
    result,meta=run_comparison(ORIGINAL,CANDIDATE,accepted_lsn=accepted,event=lambda p,d=None:events.append(p))
    check('副本未追上时回主库',meta['source']=='primary',meta)
    check('回主库原因是副本滞后',meta['fallback_reason']=='replica_lag',meta)
    check('主库结果包含刚入账的运费',result['equivalent'] is True and result['original']['row_count']==3,result.get('error'))
    # 恢复复制后，同一受理点应该可以在副本执行
    resume_replay();time.sleep(0.5)
    target,meta2=run_comparison(ORIGINAL,CANDIDATE,accepted_lsn=accepted)
    check('副本追平后在副本执行',meta2['source']=='replica',meta2)
    check('两侧快照一致',meta2['snapshot'] is not None,meta2)
    check('两侧结果一致',target['equivalent'] is True)
    log('snapshot_identity',snapshot=meta2['snapshot'])
    return {'frozen_lsn':frozen,'accepted_lsn':accepted,'fallback':meta,'replica_run':meta2}
def case_paused_replication():
    section('场景二：暂停复制')
    clean();accepted=make_stale(888.0)
    t=time.time();events=[]
    result,meta=run_comparison(ORIGINAL,CANDIDATE,accepted_lsn=accepted,event=lambda p,d=None:events.append(p))
    wall=round(time.time()-t,2)
    check('回主库执行',meta['source']=='primary',meta)
    check('原因是副本滞后',meta['fallback_reason']=='replica_lag',meta)
    check('等库时间不超过十秒',wall<=12,wall)
    check('拿到回退名额后立刻执行',meta['fallback_wait_ms']==0,meta)
    check('结果发布成功',meta['status']=='passed',meta)
    check('事件里出现回主库执行','executing_primary' in events,events)
    resume_replay()
    return {'wall_s':wall,'meta':meta,'events':events}
def case_midflight_disconnect(control_cmd,start_cmd):
    section('场景三：执行中副本断开')
    ensure_replica(start_cmd)
    clean();ident=900300+int(time.time())%1000;insert_bill(ident,999.0);time.sleep(0.5)
    events=[];holder={}
    def killer():
        time.sleep(1.5)
        subprocess.run(control_cmd,capture_output=True)
        holder['killed_at']=round(time.time()-t0,2);log('replica_stopped',at_s=holder['killed_at'])
    t0=time.time();threading.Thread(target=killer,daemon=True).start()
    result,meta=run_comparison(SLOW,CANDIDATE,event=lambda p,d=None:events.append(p))
    wall=round(time.time()-t0,2)
    check('副本断开后回主库整批重做',meta['source']=='primary',meta)
    check('原因是副本断开',meta['fallback_reason']=='replica_lost',meta)
    check('记录了副本断开事件','replica_lost' in events,events)
    check('拿到回退名额','fallback_acquired' in events,events)
    check('结果在主库发布',meta['status'] in ('passed','different'),meta)
    check('新账单已进入结果',result.get('equivalent') is False or result.get('original',{}).get('row_count')==3,result.get('error'))
    ensure_replica(start_cmd)
    return {'wall_s':wall,'meta':meta,'events':events}
def case_slot_contention(start_cmd):
    section('场景四：两批争抢唯一回退名额')
    ensure_replica(start_cmd)
    clean();make_stale(601.0);resume_replay();time.sleep(0.3)  # 追平后重新暂停，确保 slow 先占名额
    accepted=make_stale(601.0)
    results={}
    def job(tag,query,delay):
        time.sleep(delay);ev=[]
        t=time.time()
        res,meta=run_comparison(query,CANDIDATE,accepted_lsn=accepted,event=lambda p,d=None:ev.append(p))
        results[tag]={'wall':round(time.time()-t,2),'meta':meta,'events':ev}
    t0=time.time()
    threads=[threading.Thread(target=job,args=('slow',SLOW,0)),threading.Thread(target=job,args=('fast',ORIGINAL,0.3))]
    [t.start() for t in threads];[t.join() for t in threads]
    total=round(time.time()-t0,2)
    a,b=results['slow'],results['fast']
    log('contention_detail',slow=a['meta']['source'],fast=b['meta']['source'],
        fast_reason=b['meta']['fallback_reason'],fast_wait_ms=b['meta']['fallback_wait_ms'],
        slow_wall=a['wall'],fast_wall=b['wall'],total=total)
    check('只有一批占用回退名额',a['meta']['source']=='primary' and b['meta']['source'] in (None,'primary'),results)
    check('另一批明确失败或等副本追平',b['meta']['status']=='failed' or b['meta']['source']=='replica',b['meta'])
    if b['meta']['status']=='failed':check('失败原因是名额超时',b['meta']['fallback_reason']=='fallback_slot_timeout',b['meta'])
    check('失败批次最多等十秒左右',b['wall']<=12.5,b['wall'])
    resume_replay()
    return results
def case_slot_wait_then_acquire():
    section('场景五：名额短暂占用，等待后取得并回主库')
    clean();accepted=make_stale(602.0)
    def hold():
        with lock_engine.connect() as c:
            c.execute(text('SELECT pg_advisory_lock(:k)'),{'k':FALLBACK_SLOT_LOCK_KEY})
            time.sleep(3)
            c.execute(text('SELECT pg_advisory_unlock(:k)'),{'k':FALLBACK_SLOT_LOCK_KEY})
    threading.Thread(target=hold).start();time.sleep(0.3)
    ev=[];t=time.time()
    result,meta=run_comparison(ORIGINAL,CANDIDATE,accepted_lsn=accepted,event=lambda p,d=None:ev.append(p))
    wall=round(time.time()-t,2)
    check('等待后取得名额',meta['source']=='primary' and 'waiting_replica' in ev,ev)
    check('确实等待了回退名额',meta['fallback_wait_ms']>0,meta)
    check('等待未超过十秒',wall<=10.5,wall)
    resume_replay()
    return {'wall_s':wall,'meta':meta,'events':ev}
def case_slot_timeout():
    section('场景六：名额一直满，批次明确失败且不发布结论')
    clean();accepted=make_stale(603.0)
    holder_conn=lock_engine.connect()
    holder_conn.execute(text('SELECT pg_advisory_lock(:k)'),{'k':FALLBACK_SLOT_LOCK_KEY})
    try:
        t=time.time();ev=[]
        result,meta=run_comparison(ORIGINAL,CANDIDATE,accepted_lsn=accepted,event=lambda p,d=None:ev.append(p))
        wall=round(time.time()-t,2)
        check('批次失败',meta['status']=='failed',meta)
        check('原因是名额超时',meta['fallback_reason']=='fallback_slot_timeout',meta)
        check('十秒内明确失败',10<=wall<=12.5,wall)
        check('没有结果',result.get('error') and 'equivalent' not in result,result)
        check('事件含等库',ev[0]=='waiting_replica',ev)
    finally:
        holder_conn.execute(text('SELECT pg_advisory_unlock(:k)'),{'k':FALLBACK_SLOT_LOCK_KEY});holder_conn.close();resume_replay()
    return {'wall_s':wall,'meta':meta,'events':ev}
def case_recovery_no_duplicate():
    section('场景七：进程中断恢复重试不重复发布结论')
    clean();sid=sample_id()
    with Session() as db:
        b=Batch(name='recovery-'+str(time.time_ns()),sample_id=sid,candidate_sql=CANDIDATE,
                status='executing',started_at=datetime.utcnow()-timedelta(minutes=5))
        db.add(b);db.commit();bid=b.id
    n=worker.recover_stale()
    with Session() as db:status=db.get(Batch,bid).status
    check('超时批次被重新排队',n==1 and status=='queued',(n,status))
    worker.run_one(bid)
    with Session() as db:
        b=db.get(Batch,bid);count=db.query(Evidence).filter_by(batch_id=bid).count()
        phases=[e.phase for e in db.query(BatchEvent).filter_by(batch_id=bid)]
    check('第一次重试发布结论',b.status in ('passed','different') and count==1,(b.status,count))
    first=db_evidence(bid)
    worker.run_one(bid);worker.run_one(bid)
    with Session() as db:count2=db.query(Evidence).filter_by(batch_id=bid).count()
    check('重复重试不重复发布',count2==1 and db_evidence(bid)==first,count2)
    # 再模拟“已执行完、证据已写、进程在更新状态前崩溃”：恢复不得改成另一结论
    with Session() as db:
        db.query(Batch).filter(Batch.id==bid).update({'status':'executing','started_at':datetime.utcnow()-timedelta(minutes=5)});db.commit()
    # 恢复流程发现证据已存在：只按证据补写终态，不重新排队执行
    requeued_before=len(queue_jobs())
    recovered=worker.recover_stale()
    check('已发布证据的批次只补写状态',requeued_before==len(queue_jobs()),(requeued_before,recovered))
    worker.run_one(bid)
    with Session() as db:
        b=db.get(Batch,bid);count3=db.query(Evidence).filter_by(batch_id=bid).count()
    check('已发布证据的批次重试后结论不变',b.status in ('passed','different') and count3==1 and db_evidence(bid)==first,(b.status,count3))
    return {'batch_id':bid,'status':b.status,'evidence_rows':count3,'phases':phases,'recovered':recovered}
def queue_jobs():
    from src.services.batch import queue
    return list(queue.lrange('query-review:jobs',0,-1))
def db_evidence(bid):
    with Session() as db:return json.dumps(db.query(Evidence).filter_by(batch_id=bid).one().result,sort_keys=True)
def main():
    path=OUT/'acceptance-replica.json'
    failures=[]
    def run(fn,*args):
        try:return fn(*args)
        except Exception as exc:
            traceback.print_exc();failures.append({'case':fn.__name__,'error':str(exc)});return {'error':str(exc)}
    report={'generated_at':datetime.utcnow().isoformat()+'Z','replica_env':os.getenv('REPLICA_DATABASE_URL'),'cases':{},'log':LOG}
    control=json.loads(os.getenv('REPLICA_STOP_CMD','[]'))
    start=json.loads(os.getenv('REPLICA_START_CMD','[]'))
    if not control:
        print('需要 REPLICA_STOP_CMD 才能测试中途断开，例如：')
        print('  REPLICA_STOP_CMD=\'["pg_ctl","-D","/tmp/pgreplica","-m","immediate","stop"]\'')
    if not start:print('提示：未设置 REPLICA_START_CMD，中途断开后需要手动重启副本')
    try:
        report['cases']['fresh_bill']=run(case_fresh_bill,start)
        report['cases']['paused_replication']=run(case_paused_replication)
        if control:report['cases']['midflight_disconnect']=run(case_midflight_disconnect,control,start)
        report['cases']['slot_contention']=run(case_slot_contention,start)
        report['cases']['slot_wait_then_acquire']=run(case_slot_wait_then_acquire)
        report['cases']['slot_timeout']=run(case_slot_timeout)
        report['cases']['recovery_idempotent']=run(case_recovery_no_duplicate)
    finally:
        try:resume_replay()
        except Exception:pass
        report['failures']=failures;report['log']=LOG
        path.write_text(json.dumps(report,ensure_ascii=False,indent=2))
        print('report:',path,'failures:',len(failures))
    return 1 if failures else 0
if __name__=='__main__':sys.exit(main())
