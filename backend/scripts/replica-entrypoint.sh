#!/bin/sh
# 演示用物理流复制：从主库 pg_basebackup 克隆后作为 hot standby 启动。
# 已经初始化过则直接启动；每次重启都会继续从主库接收 WAL。
set -e
REPLICA_DIR=${REPLICA_DIR:-/var/lib/postgresql/replica}
PRIMARY_HOST=${PRIMARY_HOST:-db}
REPLICATION_USER=${REPLICATION_USER:-replicator}
PRIMARY_CONNINFO="host=$PRIMARY_HOST port=5432 user=$REPLICATION_USER password=${REPLICATION_PASSWORD} application_name=qr-replica"

if [ ! -s "$REPLICA_DIR/PG_VERSION" ]; then
  echo "replica: 初次克隆主库 $PRIMARY_HOST"
  rm -rf "$REPLICA_DIR"
  mkdir -p "$REPLICA_DIR"
  chown postgres:postgres "$REPLICA_DIR"
  # 不带 -R：避免 pg_basebackup 写入依赖 passfile 的 primary_conninfo，
  # 密码在每次启动时通过 -c primary_conninfo 显式传入。
  until PGPASSWORD="$REPLICATION_PASSWORD" pg_basebackup \
      -h "$PRIMARY_HOST" -p 5432 -U "$REPLICATION_USER" \
      -D "$REPLICA_DIR" -Fp -Xs -P; do
    echo "replica: 主库尚未就绪，10 秒后重试"
    sleep 10
  done
fi

# 上次容器可能被强杀，残留 postmaster.pid 会阻止启动
if [ -s "$REPLICA_DIR/postmaster.pid" ]; then
  gosu postgres pg_ctl -D "$REPLICA_DIR" -m fast stop 2>/dev/null || rm -f "$REPLICA_DIR/postmaster.pid"
fi
chown -R postgres:postgres "$REPLICA_DIR"
chmod 700 "$REPLICA_DIR"
touch "$REPLICA_DIR/standby.signal"
chown postgres:postgres "$REPLICA_DIR/standby.signal"

exec gosu postgres postgres -D "$REPLICA_DIR" \
  -c hot_standby=on \
  -c port=5432 \
  -c max_wal_senders=10 \
  -c primary_conninfo="$PRIMARY_CONNINFO"
