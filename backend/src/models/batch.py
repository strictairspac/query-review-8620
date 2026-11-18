from sqlalchemy.orm import Mapped,mapped_column
from sqlalchemy import String,Text,ForeignKey,DateTime,JSON,Integer,func
from datetime import datetime
from ..config.database import Base
class Batch(Base):
    __tablename__='batches'
    id:Mapped[int]=mapped_column(primary_key=True)
    name:Mapped[str]=mapped_column(String(120),unique=True)
    sample_id:Mapped[int]=mapped_column(ForeignKey('samples.id'))
    candidate_sql:Mapped[str]=mapped_column(Text)
    status:Mapped[str]=mapped_column(String(20),default='queued')
    # 本次执行读取的数据库：replica 只读副本 / primary 主库回退；未开始前为空
    source:Mapped[str|None]=mapped_column(String(20),nullable=True)
    # 回主库的原因：replica_lag / replica_lost / replica_unavailable / fallback_slot_timeout
    fallback_reason:Mapped[str|None]=mapped_column(String(40),nullable=True)
    # 受理时记录的主库 LSN 与副本已回放 LSN；副本追平后在本快照上取数
    accepted_primary_lsn:Mapped[str|None]=mapped_column(String(40),nullable=True)
    replica_lsn:Mapped[str|None]=mapped_column(String(40),nullable=True)
    # 实际执行快照标识（pg_current_snapshot 文本），两侧查询共用同一快照
    snapshot:Mapped[str|None]=mapped_column(String(80),nullable=True)
    replica_wait_ms:Mapped[int|None]=mapped_column(Integer,nullable=True)
    fallback_wait_ms:Mapped[int|None]=mapped_column(Integer,nullable=True)
    started_at:Mapped[datetime|None]=mapped_column(DateTime,nullable=True)
    finished_at:Mapped[datetime|None]=mapped_column(DateTime,nullable=True)
    created_at:Mapped[datetime]=mapped_column(DateTime,server_default=func.now())
class BatchEvent(Base):
    __tablename__='batch_events'
    id:Mapped[int]=mapped_column(primary_key=True)
    batch_id:Mapped[int]=mapped_column(ForeignKey('batches.id'),index=True)
    phase:Mapped[str]=mapped_column(String(40))
    detail:Mapped[dict]=mapped_column(JSON)
    created_at:Mapped[datetime]=mapped_column(DateTime,server_default=func.now())
