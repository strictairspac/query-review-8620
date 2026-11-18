import os
DATABASE_URL=os.environ['DATABASE_URL']
REPLICA_DATABASE_URL=os.getenv('REPLICA_DATABASE_URL') or None
REDIS_URL=os.getenv('REDIS_URL','redis://redis:6379/0')
JWT_SECRET=os.environ['JWT_SECRET']
ARTIFACT_DIR=os.getenv('ARTIFACT_DIR','/artifacts')
REPLICATION_PASSWORD=os.getenv('REPLICATION_PASSWORD','local_repl_4101')
# 每批最多等待副本追平或主库回退名额的秒数
FALLBACK_WAIT_SECONDS=float(os.getenv('FALLBACK_WAIT_SECONDS','10'))
# 先给副本一小段追平窗口，避免刚受理的批次因毫秒级滞后就回主库
REPLICA_CATCHUP_GRACE_SECONDS=float(os.getenv('REPLICA_CATCHUP_GRACE_SECONDS','2'))
REPLICA_POLL_SECONDS=float(os.getenv('REPLICA_POLL_SECONDS','0.1'))
WORKER_CONCURRENCY=max(1,int(os.getenv('WORKER_CONCURRENCY','2')))
# 进程中断后，未终态批次超过该秒数才允许恢复重试。
# 必须大于“等库上限 + 两侧各两次 2.5 秒超时”的最坏执行时间，默认给 60 秒；
# 即使判断过早，单批次 advisory lock 也会阻止同一批次被并发执行。
RECOVERY_STALE_SECONDS=float(os.getenv('RECOVERY_STALE_SECONDS','60'))
STATEMENT_TIMEOUT_MS=max(1,int(os.getenv('STATEMENT_TIMEOUT_MS','2500')))
MAX_RESULT_ROWS=max(1,int(os.getenv('MAX_RESULT_ROWS','1000')))
