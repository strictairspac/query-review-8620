from sqlalchemy import select
from fastapi import HTTPException
from redis import Redis
from ..config.settings import REDIS_URL
from ..models.batch import Batch,BatchEvent
from ..models.sample import Sample
from ..constants.enums import PENDING_STATUSES
from .query_engine import validate_sql
from .replication import primary_lsn,replica_status
queue=Redis.from_url(REDIS_URL)
SOURCE_LABELS={'replica':'只读副本','primary':'主库回退'}
REASON_LABELS={'replica_lag':'副本未追上受理前的账单变更','replica_lost':'副本执行中连接断开',
               'replica_unavailable':'副本不可用或未配置','fallback_slot_timeout':'等待主库回退名额超过十秒'}
def serialize(x):return {'id':x.id,'name':x.name,'sample_id':x.sample_id,'candidate_sql':x.candidate_sql,
    'status':x.status,'source':x.source,'source_label':SOURCE_LABELS.get(x.source),
    'fallback_reason':x.fallback_reason,'fallback_reason_label':REASON_LABELS.get(x.fallback_reason),
    'accepted_primary_lsn':x.accepted_primary_lsn,'replica_lsn':x.replica_lsn,'snapshot':x.snapshot,
    'replica_wait_ms':x.replica_wait_ms,'fallback_wait_ms':x.fallback_wait_ms,
    'started_at':str(x.started_at) if x.started_at else None,'finished_at':str(x.finished_at) if x.finished_at else None,
    'created_at':str(x.created_at)}
def _events(db,batch_id):
    return [{'phase':e.phase,'detail':e.detail,'created_at':str(e.created_at)}
            for e in db.scalars(select(BatchEvent).where(BatchEvent.batch_id==batch_id).order_by(BatchEvent.id))]
def list_batches(db):return [{**serialize(x),'events':_events(db,x.id)} for x in db.scalars(select(Batch).order_by(Batch.id.desc()))]
def get_batch(db,batch_id):
    x=db.get(Batch,batch_id)
    if not x:raise HTTPException(404,'批次不存在')
    return {**serialize(x),'events':_events(db,x.id)}
def replica_health():
    status=replica_status()
    return {'configured':status['configured'],'ok':status['ok'],'in_recovery':status['in_recovery'],
            'replay_lsn':status['replay_lsn'],'receive_lsn':status['receive_lsn'],'paused':status['paused'],'error':status['error']}
def create_batch(db,data):
    validate_sql(data.candidate_sql)
    if not db.get(Sample,data.sample_id):raise HTTPException(404,'样本不存在')
    if db.scalar(select(Batch).where(Batch.name==data.name)):raise HTTPException(409,'批次名称已存在')
    if len(list(db.scalars(select(Batch).where(Batch.status.in_(PENDING_STATUSES)))))>=20:raise HTTPException(429,'待执行任务达到20个，请稍后重试')
    accepted=primary_lsn()
    obj=Batch(**data.model_dump(),status='queued',accepted_primary_lsn=accepted);db.add(obj);db.flush()
    db.add(BatchEvent(batch_id=obj.id,phase='accepted',detail={'accepted_primary_lsn':accepted}))
    db.commit();queue.rpush('query-review:jobs',obj.id);return serialize(obj)
