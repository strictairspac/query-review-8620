"""只读副本的追平判断、执行目标选择与主库单一回退名额。

关键口径：
- 受理批次时记录主库当前 WAL 位置；副本回放到该位置后，才允许在副本上受理。
- 两侧 SQL 在同一个只读事务快照里执行（见 query_engine），不跨库各读一次。
- 副本落后、暂停或中途断开时，整批回主库重做；主库全局只容一批回退。
"""
import time
from sqlalchemy import create_engine,text
from sqlalchemy.exc import DBAPIError
from psycopg2 import InterfaceError as _PgInterfaceError, OperationalError as _PgOperationalError
from sqlalchemy.exc import InterfaceError as BrokenConnection
from ..config.database import engine,lock_engine
from ..config.settings import REPLICA_DATABASE_URL,FALLBACK_WAIT_SECONDS,REPLICA_POLL_SECONDS
from ..constants.enums import FALLBACK_SLOT_LOCK_KEY
_replica_engine=None
def replica_engine():
    global _replica_engine
    if not REPLICA_DATABASE_URL:return None
    if _replica_engine is None:
        _replica_engine=create_engine(REPLICA_DATABASE_URL,pool_pre_ping=True,pool_size=4,max_overflow=4)
    return _replica_engine
def lsn_to_int(value):
    if not value:return 0
    hi,lo=str(value).split('/');return (int(hi,16)<<32)+int(lo,16)
def primary_lsn():
    with engine.connect() as conn:return conn.execute(text('SELECT pg_current_wal_lsn()::text')).scalar()
def replica_status():
    """读取副本健康状态。任何连接/查询失败都视为副本不可用。"""
    target=replica_engine()
    if target is None:return {'configured':False,'ok':False,'in_recovery':None,'replay_lsn':None,'receive_lsn':None,'paused':None,'error':'未配置副本连接'}
    try:
        with target.connect() as conn:
            row=conn.execute(text("SELECT pg_is_in_recovery(), pg_last_wal_replay_lsn()::text, pg_last_wal_receive_lsn()::text, pg_is_wal_replay_paused()")).one()
        in_recovery,replay_lsn,receive_lsn,paused=row
        if not in_recovery:
            return {'configured':True,'ok':False,'in_recovery':False,'replay_lsn':replay_lsn,'receive_lsn':receive_lsn,'paused':None,'error':'副本连接指向的实例不处于恢复模式'}
        return {'configured':True,'ok':True,'in_recovery':True,'replay_lsn':replay_lsn,'receive_lsn':receive_lsn,'paused':bool(paused),'error':None}
    except Exception as exc:
        return {'configured':True,'ok':False,'in_recovery':None,'replay_lsn':None,'receive_lsn':None,'paused':None,'error':str(exc)[:300]}
def replica_caught_up(accepted_lsn,status=None):
    status=status or replica_status()
    if not status['ok'] or not accepted_lsn:return False
    return lsn_to_int(status['replay_lsn'])>=lsn_to_int(accepted_lsn)
def is_connection_error(exc):
    """只把真正的连接层错误当作“副本中途断开”。

    psycopg2 的 OperationalError 覆盖面很广（例如服务端缺库文件），
    语句级错误带 SQLSTATE，不应触发整批回主库；连接丢失没有 SQLSTATE。
    """
    if isinstance(exc,BrokenConnection):return True
    if not isinstance(exc,DBAPIError):return False
    if getattr(exc,'connection_invalidated',False):return True
    orig=getattr(exc,'orig',None)
    if not isinstance(orig,(_PgInterfaceError,_PgOperationalError)):return False
    # 连接丢失没有 SQLSTATE；语句/权限/缺文件等错误带 pgcode，按普通执行失败处理
    return getattr(orig,'pgcode',None) is None
class ReplicaLost(RuntimeError):pass
def ensure_replica_alive():
    """执行期间的健康检查；连接断开或副本不再可读时中止整个只读事务。"""
    status=replica_status()
    if not status['ok']:raise ReplicaLost('副本中途不可用：'+(status['error'] or '未知错误'))
    return status
def wait_for_replica(accepted_lsn,deadline=None,budget=None):
    """等待副本回放到受理时的主库位置；返回 (是否追平, 最后状态, 等待毫秒)。"""
    budget=FALLBACK_WAIT_SECONDS if budget is None else budget
    deadline=time.monotonic()+budget if deadline is None else deadline
    started=time.monotonic();status=replica_status()
    while True:
        if replica_caught_up(accepted_lsn,status):
            return True,status,round((time.monotonic()-started)*1000)
        if time.monotonic()>=deadline:
            return False,status,round((time.monotonic()-started)*1000)
        time.sleep(min(REPLICA_POLL_SECONDS,max(0.01,deadline-time.monotonic())))
        status=replica_status()
class FallbackSlot:
    """主库全局唯一的回退名额，使用会话级 advisory lock。

    锁连接来自 NullPool，关闭即断开，异常时也不会把带锁的物理连接还回连接池。
    """
    def __init__(self):self.conn=None;self.acquired=False
    def try_acquire(self):
        if self.acquired:return True
        conn=lock_engine.connect()
        try:
            got=conn.execute(text('SELECT pg_try_advisory_lock(:k)'),{'k':FALLBACK_SLOT_LOCK_KEY}).scalar()
        except Exception:
            conn.close();raise
        if got:self.conn=conn;self.acquired=True;return True
        conn.close();return False
    def release(self):
        if not self.acquired:return
        try:self.conn.execute(text('SELECT pg_advisory_unlock(:k)'),{'k':FALLBACK_SLOT_LOCK_KEY})
        except Exception:pass
        try:self.conn.close()
        except Exception:pass
        self.conn=None;self.acquired=False
