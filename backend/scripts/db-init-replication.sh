#!/bin/sh
# 主库首次初始化时执行（官方镜像在 initdb 之后、服务启动前运行）：
# 1) 创建流复制账号，供 db-replica 的 pg_basebackup 使用；
# 2) 允许复制连接，官方镜像默认只放行普通连接的 host all 规则。
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  DO \$\$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='replicator') THEN
      CREATE ROLE replicator LOGIN REPLICATION PASSWORD '${REPLICATION_PASSWORD:-local_repl_4101}';
    END IF;
  END \$\$;
EOSQL
if ! grep -q '^host  *replication' "$PGDATA/pg_hba.conf"; then
  echo 'host replication replicator all scram-sha-256' >> "$PGDATA/pg_hba.conf"
fi
