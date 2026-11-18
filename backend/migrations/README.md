Schema version 1: SQLAlchemy metadata creates users/samples/batches/evidence/audit. seed_demo initializes the separate invoices fixture table and NOLOGIN query_reader. invoices is the only permitted SQL source. Run `python -m scripts.seed_demo` for repeatable initialization; natural keys preserve user edits.

Schema version 2（只读副本执行）:
- batches 新增 source（replica/primary）、fallback_reason、accepted_primary_lsn、replica_lsn、snapshot、replica_wait_ms、fallback_wait_ms、started_at、finished_at。
- 新增 batch_events（batch_id, phase, detail, created_at），记录受理、等库、副本断开、取得回退名额、执行、完成/失败、中断恢复重排。
- seed_demo 用 `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` 幂等升级旧库；同时创建 REPLICATION 账号 replicator（密码 REPLICATION_PASSWORD），供 db-replica 的 pg_basebackup 使用。
- 不修改证据表结构与唯一约束；`evidence.batch_id` 唯一仍是“不重复发布结论”的数据库兜底。
