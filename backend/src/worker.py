import json,signal,threading,time,traceback
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from sqlalchemy import select,update,text
from .config.database import Session,lock_engine
from .config.settings import ARTIFACT_DIR,RECOVERY_STALE_SECONDS,WORKER_CONCURRENCY
from .models.batch import Batch,BatchEvent
from .models.sample import Sample
from .models.evidence import Evidence
from .services.batch import queue
from .services.execution import run_comparison
from .constants.enums import CLAIMABLE_STATUSES,FALLBACK_SLOT_LOCK_KEY
heartbeat=Path(ARTIFACT_DIR)/'worker.heartbeat'
_stopped=threading.Event()
def _now():return datetime.utcnow()
def _record_event(db,batch_id,phase,detail=None):
    db.add(BatchEvent(batch_id=batch_id,phase=phase,detail=detail or {}));db.commit()
def _claim(batch_id):
    """抢占批次：只有排队中的批次才会被标记 executing。

    条件更新是原子的，只成功一次；重复投递或多 worker 竞争时其余调用直接放弃。
    `executing` 不在可抢占集合内——正在跑的批次不会被并发重复执行；
    确属进程中断遗留的 executing 由 recover_stale 先改回 queued 再处理。
    """
    with Session() as db:
        batch=db.get(Batch,batch_id)
        if not batch or batch.status not in CLAIMABLE_STATUSES:return None
        if db.scalar(select(Evidence.id).where(Evidence.batch_id==batch_id)):return None
        claimed=db.execute(update(Batch).where(Batch.id==batch_id,Batch.status.in_(CLAIMABLE_STATUSES))
                           .values(status='executing',started_at=_now())).rowcount
        db.commit()
        if not claimed:return None
        sample=db.get(Sample,batch.sample_id)
        batch=db.get(Batch,batch_id)
        return {'id':batch.id,'original_sql':sample.original_sql,'candidate_sql':batch.candidate_sql,
                'accepted_primary_lsn':batch.accepted_primary_lsn}
def _finish(batch_id,result,meta):
    """发布结果与证据；evidence.batch_id 唯一约束兜底，重复发布会被数据库拒绝。"""
    status=meta.get('status','failed')
    with Session() as db:
        if db.scalar(select(Evidence.id).where(Evidence.batch_id==batch_id)):return False
        batch=db.get(Batch,batch_id)
        if not batch:return False
        batch.status=status;batch.source=meta.get('source');batch.fallback_reason=meta.get('fallback_reason')
        batch.accepted_primary_lsn=meta.get('accepted_primary_lsn');batch.replica_lsn=meta.get('replica_lsn')
        batch.snapshot=meta.get('snapshot');batch.replica_wait_ms=meta.get('replica_wait_ms')
        batch.fallback_wait_ms=meta.get('fallback_wait_ms');batch.finished_at=_now()
        result.setdefault('execution',dict(meta))
        db.add(Evidence(batch_id=batch_id,result=result));db.flush()
        # completed/failed 由 _finish 统一落库，run_comparison 内的事件不再重复写终态
        db.add(BatchEvent(batch_id=batch_id,phase='completed' if status!='failed' else 'failed',
                          detail={'source':meta.get('source'),'status':status,'fallback_reason':meta.get('fallback_reason')}))
        db.commit()
    Path(ARTIFACT_DIR,f'review-{batch_id}.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
    return True
def _batch_lock(batch_id):
    """会话级单批次执行锁。即使恢复流程误判，同一批次也不会被两个 worker 并行执行。"""
    key=FALLBACK_SLOT_LOCK_KEY+batch_id
    conn=lock_engine.connect()
    try:
        got=conn.execute(text('SELECT pg_try_advisory_lock(:k)'),{'k':key}).scalar()
    except Exception:
        conn.close();raise
    if not got:conn.close();return None
    return conn
def run_one(batch_id):
    job=_claim(batch_id)
    if not job:return False
    guard=_batch_lock(batch_id)
    if guard is None:return False
    def event(phase,detail=None):
        try:
            with Session() as db:_record_event(db,batch_id,phase,detail)
        except Exception:traceback.print_exc()
    try:
        try:
            result,meta=run_comparison(job['original_sql'],job['candidate_sql'],
                                       accepted_lsn=job.get('accepted_primary_lsn'),event=event)
        except Exception as exc:
            traceback.print_exc()
            result={'error':str(exc)[:500]};meta={'status':'failed','source':None,'fallback_reason':None}
            event('failed',{'message':str(exc)[:500]})
        return _finish(batch_id,result,meta)
    finally:
        try:guard.close()
        except Exception:pass
def _recoverable(batch_id):
    """有证据可直接补写；没有证据但批次锁空闲，说明原进程确实已中断。"""
    conn=lock_engine.connect()
    try:
        return bool(conn.execute(text('SELECT pg_try_advisory_lock(:k)'),
                                 {'k':FALLBACK_SLOT_LOCK_KEY+batch_id}).scalar())
    finally:
        try:
            conn.execute(text('SELECT pg_advisory_unlock(:k)'),{'k':FALLBACK_SLOT_LOCK_KEY+batch_id})
        except Exception:pass
        conn.close()
def recover_stale():
    """进程中断恢复：把超时的 executing/running 批次重新处理。

    - 已有证据但状态没写成终态（写完证据后进程崩溃）：直接按证据补写终态，不重跑。
    - 没有证据的：重新入队，重试仍要经过 _claim 与唯一证据校验，不会重复发布。
    - 超时失败但没有证据的批次：也是未终态，会重新入队，符合“中断恢复后重试”。
    queued 批次本来就在 Redis 队列里，不在这里重复入队。
    """
    cutoff=datetime.utcfromtimestamp(time.time()-RECOVERY_STALE_SECONDS)
    recovered=0
    with Session() as db:
        # 只恢复“已有证据”或“没有 worker 持有批次锁”的遗留任务，
        # 避免把仍在另一个 worker 中正常执行的批次误判为中断
        stale=[b for b in db.scalars(select(Batch).where(Batch.status.in_(('executing','running')),
                Batch.started_at.is_not(None),Batch.started_at<cutoff)) if _recoverable(b.id)]
        for batch in stale:
            evidence=db.scalar(select(Evidence).where(Evidence.batch_id==batch.id))
            if evidence is not None:
                # 证据已发布：结论固定为证据里的结果，只补写批次状态，避免重复发布
                result=evidence.result or {}
                status='failed' if result.get('error') else ('passed' if result.get('equivalent') else 'different')
                batch.status=status;batch.finished_at=_now()
                db.add(BatchEvent(batch_id=batch.id,phase='completed' if status!='failed' else 'failed',
                                  detail={'recovered_from':'evidence','status':status}))
                recovered+=1
                continue
            db.add(BatchEvent(batch_id=batch.id,phase='requeued',detail={'reason':'进程中断恢复','status':'queued'}))
            db.execute(update(Batch).where(Batch.id==batch.id,Batch.status.in_(('executing','running')))
                       .values(status='queued',started_at=None))
            queue.rpush('query-review:jobs',batch.id)
            recovered+=1
        db.commit()
        return recovered
def main():
    pool=ThreadPoolExecutor(max_workers=WORKER_CONCURRENCY)
    signal.signal(signal.SIGTERM,lambda *_:_stopped.set())
    signal.signal(signal.SIGINT,lambda *_:_stopped.set())
    last_recovery=0
    try:
        while not _stopped.is_set():
            try:
                heartbeat.write_text(str(time.time()))
                if time.time()-last_recovery>5:
                    last_recovery=time.time()
                    try:recover_stale()
                    except Exception:traceback.print_exc()
                item=queue.blpop('query-review:jobs',timeout=2)
                if item:pool.submit(run_one,int(item[1]))
            except Exception:traceback.print_exc();time.sleep(2)
    finally:
        pool.shutdown(wait=True)
if __name__=='__main__':main()
