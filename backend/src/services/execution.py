"""单个复核批次的执行编排：选库、等库、整批回退与结果发布。

执行目标只有两种：
- replica：副本已回放到受理时的主库 WAL 位置，在副本的只读快照上比较两侧。
- primary：副本落后/不可用/中途断开时，占用主库全局唯一的回退名额后整批重做。

`run_comparison` 只返回结果与执行元数据，不写数据库；过程通过 event(phase, detail)
上报，由 worker 落库，页面据此展示“等库/执行/完成/失败 + 用的哪个库 + 为何回主库”。
"""
import time
from .query_engine import compare_pair
from .replication import (FallbackSlot,ReplicaLost,ensure_replica_alive,is_connection_error,
                          primary_lsn,replica_caught_up,replica_engine,replica_status,wait_for_replica)
from ..config.database import engine
from ..config.settings import FALLBACK_WAIT_SECONDS,REPLICA_CATCHUP_GRACE_SECONDS,REPLICA_POLL_SECONDS
REASON_LABELS={
    'replica_lag':'副本未追上受理前的账单变更',
    'replica_lost':'副本执行中连接断开',
    'replica_unavailable':'副本不可用或未配置',
    'fallback_slot_timeout':'等待主库回退名额超过十秒',
}
SLOT_TIMEOUT_MESSAGE='副本未追上且等待主库回退名额超过十秒；本批次未执行，未发布结论'
LOST_AND_TIMEOUT_MESSAGE='副本执行中连接断开，且等待主库回退名额超过十秒；本批次未发布结论'
REPLICA_FAILED_MESSAGE='副本执行中断，且未能取得主库回退名额；本批次未发布结论'
def _remaining(deadline):return max(0.0,deadline-time.monotonic())
def _fail(message,meta,event,reason,source=None):
    meta['elapsed_ms']=round((time.monotonic()-meta.pop('_started'))*1000)
    meta.update(status='failed',error=message,reason=reason)
    if source:meta['source']=source
    return {'error':message},meta
def _wait_for_slot_or_replica(accepted_lsn,deadline,event,fallback_reason):
    """等主库回退名额空出，或等副本自己追平；到 deadline 明确失败。"""
    slot=FallbackSlot();announced=False
    while True:
        status=replica_status()
        if replica_caught_up(accepted_lsn,status):
            return 'replica',None,status
        if slot.try_acquire():
            return 'primary',slot,status
        if not announced:
            announced=True
            event('waiting_replica',{'wait_for':'fallback_slot','reason':fallback_reason,
                                     'replica_ok':status['ok'],'replica_replay_lsn':status['replay_lsn'],
                                     'accepted_lsn':accepted_lsn})
        if _remaining(deadline)<=0:return None,None,status
        time.sleep(min(REPLICA_POLL_SECONDS,_remaining(deadline)))
def choose_target(accepted_lsn,deadline,event):
    """先给副本一个追平窗口；未追平即尝试主库回退名额，名额满则等到 deadline。

    等待预算由调用方给出（受理起最多 FALLBACK_WAIT_SECONDS 秒），
    因此“每批最多等十秒”同时覆盖等副本与等名额两段。
    """
    # 未配置副本时不必空等追平窗口，直接按“副本不可用”回主库
    status=replica_status()
    grace=0 if not status['configured'] else min(REPLICA_CATCHUP_GRACE_SECONDS,_remaining(deadline))
    caught,status,replica_wait_ms=wait_for_replica(accepted_lsn,budget=grace)
    waits={'replica_wait_ms':replica_wait_ms,'fallback_wait_ms':0}
    if caught:return 'replica',None,None,status,waits
    reason='replica_unavailable' if not status['ok'] else 'replica_lag'
    slot=FallbackSlot()
    if slot.try_acquire():return 'primary',slot,reason,status,waits
    fallback_started=time.monotonic()
    target,slot,status=_wait_for_slot_or_replica(accepted_lsn,deadline,event,reason)
    waits['fallback_wait_ms']=round((time.monotonic()-fallback_started)*1000)
    if target=='replica':return 'replica',None,None,status,waits
    if target is None:return 'timeout',None,'fallback_slot_timeout',status,waits
    return 'primary',slot,reason,status,waits
def run_comparison(original_sql,candidate_sql,accepted_lsn=None,event=None,statement_timeout_ms=None):
    """accepted_lsn 是受理（POST /batches）时记录的主库位置。

    固定用受理点而不是 worker 启动点，否则高峰期主库持续写账时副本永远追不上，
    会无谓地触发回退。未提供时（直接调用）才现场取主库 LSN。
    """
    event=event or (lambda phase,detail=None:None)
    started=time.monotonic()
    deadline=started+FALLBACK_WAIT_SECONDS
    accepted=accepted_lsn or primary_lsn()
    # 受理事件在 POST /batches 时已落库；worker 只在直接调用、没有受理点时补记一次
    if not accepted_lsn:event('accepted',{'accepted_primary_lsn':accepted})
    target,slot,reason,status,waits=choose_target(accepted,deadline,event)
    meta={'accepted_primary_lsn':accepted,'source':None,
          'fallback_reason':reason if target in ('primary','timeout') else None,
          'fallback_reason_label':REASON_LABELS.get(reason) if target in ('primary','timeout') else None,
          'snapshot':None,'replica_lsn':status['replay_lsn'] if status else None,
          'replica_wait_ms':waits['replica_wait_ms'],'fallback_wait_ms':waits['fallback_wait_ms'],
          'elapsed_ms':None,'_started':started}
    if target=='timeout':
        return _fail(SLOT_TIMEOUT_MESSAGE,meta,event,'fallback_slot_timeout')
    result=None
    try:
        if target=='replica':
            event('executing_replica',{'accepted_lsn':accepted,'replica_replay_lsn':status['replay_lsn']})
            try:
                result=compare_pair(replica_engine(),original_sql,candidate_sql,
                                    statement_timeout_ms=statement_timeout_ms,after=ensure_replica_alive)
                meta.update(source='replica',snapshot=result.pop('snapshot'))
            except Exception as exc:
                if not isinstance(exc,ReplicaLost) and not is_connection_error(exc):raise
                event('replica_lost',{'error':str(exc)[:300]})
                slot=FallbackSlot()
                event('waiting_replica',{'wait_for':'fallback_slot','reason':'replica_lost'})
                recheck_started=time.monotonic()
                new_target,slot,status=_wait_for_slot_or_replica(accepted,deadline,event,'replica_lost')
                meta['fallback_wait_ms']=round((time.monotonic()-recheck_started)*1000)
                if new_target is None:
                    meta.update(fallback_reason='replica_lost',fallback_reason_label=REASON_LABELS['replica_lost'])
                    return _fail(LOST_AND_TIMEOUT_MESSAGE,meta,event,'replica_lost',source='replica')
                if new_target=='replica':
                    # 副本重启并已追平：整批仍在副本重做
                    event('replica_recovered',{'replica_replay_lsn':status['replay_lsn']})
                    target='replica';reason=None
                    meta.update(fallback_reason=None,fallback_reason_label=None)
                else:
                    target='primary';reason='replica_lost'
                    meta.update(fallback_reason=reason,fallback_reason_label=REASON_LABELS[reason])
                    event('fallback_acquired',{'reason':reason,'target':'primary'})
        if result is None:
            try:
                if target=='primary':
                    event('executing_primary',{'reason':reason})
                    result=compare_pair(engine,original_sql,candidate_sql,statement_timeout_ms=statement_timeout_ms)
                else:
                    event('executing_replica',{'retry':True})
                    result=compare_pair(replica_engine(),original_sql,candidate_sql,
                                        statement_timeout_ms=statement_timeout_ms,after=ensure_replica_alive)
            except Exception as exc:
                if target!='replica' or (not isinstance(exc,ReplicaLost) and not is_connection_error(exc)):raise
                event('replica_lost',{'retry_failed':True,'error':str(exc)[:300]})
                return _fail(REPLICA_FAILED_MESSAGE,meta,event,'replica_lost',source='replica')
            meta.update(source=target,snapshot=result.pop('snapshot'))
            if target=='primary' and not meta.get('fallback_reason'):
                meta.update(fallback_reason=reason,fallback_reason_label=REASON_LABELS.get(reason))
        meta['elapsed_ms']=round((time.monotonic()-started)*1000)
        meta['status']='passed' if result['equivalent'] else 'different'
        meta['reason']=meta.get('fallback_reason')
        result['execution']=meta
        return result,meta
    finally:
        meta.pop('_started',None)
        if slot is not None:slot.release()
