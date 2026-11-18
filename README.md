# 冷链结算 SQL 复核台

为澄港冷链结算组保存 SQL 基准样本、独立候选改写和实库执行证据。原生 HTML/CSS/JS 前端，Python 后端执行解析、只读查询和多重集合比对。

## 真实工作场景

澄港冷链结算组共 18 人，每天从运费账单中按客户、路线、温控合格条件汇总结算。目前通过邮件传递 `coldchain-settlement.sql` 和表格复核，三个痛点是版本难定位、口径变动容易漏查、单次查询截图无法还原执行计划。上游是冷链计费账单，本站替代 SQL 改写复核和证据归档，下游是结算复核人。演示数据固定 600 条账单、3 客户、2 路线，字段与页面说明一致。

## 业务模块

1. 样本库：登记名称、口径说明与原始 SELECT，防止重名；提供温控合格、路线费用、异常温控三个起始样本。
2. SQL 复核：选择基准，编辑候选，校验后创建独立批次并入 Redis 队列。
3. 运行历史：跟踪 queued/executing/passed/different/failed，并展示受理、等库、执行、回退、完成的过程时间线，标注本次用的库与回主库原因。
4. 证据详情：真实 EXPLAIN ANALYZE、两侧结果行、SHA-256 摘要、遗漏/新增行、执行库/快照/等库耗时和 JSON 下载。
5. 管理员操作审计：记录写请求、执行账号和 HTTP 结果，包括拒绝请求。

| 角色 | 主要页面 | 权限与典型动作 |
|---|---|---|
| admin 管理员 | 全部页面、审计 | 创建样本/批次、查看/导出结果、查看写操作审计 |
| operator 操作员 | 四个业务页面 | 登记 SQL、提交后台复核、查看/导出结果；不可查看管理员审计 |
| viewer 观察员 | 四个业务页面 | 只读与导出；API 层拒绝所有业务写请求 |

## 快速启动

```bash
cp .env.example .env
docker compose config --quiet
docker compose up -d --build
# 启动自动 seed；手动重跑不新增且不覆盖用户修改
docker compose exec -T backend python -m scripts.seed_demo
```

访问 [前端](http://localhost:18240)、[后端 OpenAPI](http://localhost:19240/docs)。数据库映射端口 17740。`db-replica` 是主库的物理流复制只读副本，首次启动自动 `pg_basebackup`，不映射宿主端口。项目的日志、截图、测试脚本在批次根目录 `test-artifacts/gb-4101/`，不放入源码。

演示账号邮箱为 `admin@chenggang.test`、`operator@chenggang.test`、`viewer@chenggang.test`，统一密码 `ColdChain!17`。这些是本地固定演示凭据，上线前应修改 `.env` 和账号密码。

```bash
# 仅清理本项目及其演示数据
docker compose down -v --remove-orphans
```

## 一天工作剧本

1. 09:00 operator 登录，打开“低温结算汇总”样本。
2. 09:05 查看原始 SQL：按客户执行两处相关 SUM 子查询，过滤温度不高于 8 度的运费。
3. 09:10 创建 `shift-20261117` 批次，候选改写为下面的单次分组查询。
4. 09:15 提交到 worker，在运行历史刷新查看状态；过程列依次显示“等库 → 副本执行 → 完成”，执行库为“只读副本”。
5. 09:20 打开证据，确认两侧均为 3 行、摘要相同、结果一致，且两侧快照一致；展开真实计划比较循环次数与代价。
6. 09:25 把候选条件改为 `temperature > 8`，用新批次执行并查看新增/遗漏行。
7. 09:30 下载 `review-<批次号>.json` 给结算复核人。
8. 09:40 viewer 登录复核，API 创建样本返回 403；10:00 admin 查看审计。

```sql
SELECT customer, SUM(amount) AS total
FROM invoices
WHERE temperature <= 8
GROUP BY customer
HAVING SUM(amount) > 100
ORDER BY customer
```

## 算法、输入与口径

输入是原始/候选单条 SELECT。sqlglot 解析 AST，仅准许 invoices 表、指定业务字段和 SUM/COUNT/AVG/MIN/MAX/ROUND/COALESCE/CAST。禁止多语句、INTO、锁、CTE、跨表、schema 和任意函数。解析后重新生成 PostgreSQL SQL，执行在 READ ONLY 事务中，切换 NOLOGIN `query_reader` 角色；该角色仅有 invoices 的 SELECT 权限。每条 statement 超时 2500ms，最多返回 1000 行，AST 最多 250 节点，候选 SQL 最多 6000 字符，最多 20 个排队/运行任务。

两侧均实际执行 `EXPLAIN (ANALYZE, FORMAT JSON, BUFFERS)` 与 SELECT。结果每行按列位置规范为数据库文本值，`Counter` 统计行的重复次数；列数相同且计数集合相同即 equivalent。忽略行顺序及列别名、保留重复行，NULL 保留为空值。按有序的“规范行/计数”生成 SHA-256。数值不同文本表示（如 1 与 1.0）会判不同；这是明确的保守口径。不能据固定数据集通过证明所有可能输入上的 SQL 语义等价；一次耗时不能证明优化收益。

失败态包括语法/白名单失败（提交前 422）、缺少样本（404）、重名（409）、任务配额（429）、数据库超时/运行错误（批次 failed，保存 error）。正常差异是 different，不当成异常。

### 只读副本与主库回退

月结高峰把复核查询放到 `db-replica`。受理批次（`POST /batches`）时记录主库当前 WAL 位置 `accepted_primary_lsn`；worker 先给副本 `REPLICA_CATCHUP_GRACE_SECONDS`（默认 2 秒）追平到该位置，追平后才在副本上执行，因此刚入账的运费一定会被算进去。固定用受理点而非 worker 启动点，避免高峰持续写账时副本永远追不上。

两侧查询在副本上的同一个 `REPEATABLE READ READ ONLY` 事务内执行，读取同一份快照；证据里的 `snapshot` 即该事务的 `pg_current_snapshot()`，两侧结果来自同一快照，不是各读一次。

回主库的条件与口径：

- 副本落后（`replica_lag`）、副本未配置/不可用（`replica_unavailable`）、执行中连接断开（`replica_lost`）：整批改到主库重做，不拼接副本的部分结果。
- 主库全局只容一批回退：回退名额是主库上的会话级 advisory lock（`FALLBACK_SLOT_LOCK_KEY`），占用期间其他批次不会同时压主库。
- 名额被占用时，每批最多再等 `FALLBACK_WAIT_SECONDS`（默认 10 秒），期间副本先追平就继续用副本；超时则批次 `failed`，原因为 `fallback_slot_timeout`，明确不执行、不发布结论。
- 日常写账不受影响：回退名额只在复核批次回主库期间占用，不阻塞主库写入。

worker 用单批次 advisory lock 防止同一批次被并发执行；进程中断后，`recover_stale` 对超时的 `executing` 批次重排。重试仍经过状态抢占与 `evidence.batch_id` 唯一约束，已经发布过结论的批次只会按已有证据补写状态，不会重复执行或改结论。运行历史的过程列展示 受理 → 等库 →（副本执行 | 主库执行）→ 完成/失败；证据页注明执行库、回主库原因、受理/回放 LSN、快照与等库耗时。

验收脚本 `backend/tests/acceptance_replica.py` 在真实主库+物理副本上实测：刚入账运费、暂停复制、执行中强停副本、两批争抢回退名额、名额短暂占用后取得、名额一直满而明确失败、恢复重试不重复发布。

## 技术栈、结构与本地开发

| 部分 | 实现 |
|---|---|
| frontend | 原生 ES modules、CSS、Nginx 1.27，olive paper 风格 |
| backend | Python 3.11、FastAPI、SQLAlchemy、sqlglot、PyJWT |
| worker | 独立 Python 进程，Redis BLPOP + DB 状态恢复队列 + 单批次执行锁，心跳文件 |
| db / cache | PostgreSQL 16 主库 + 物理流复制只读副本、Redis 7 |
| auth | JWT 8 小时有效，PBKDF2 SHA-256 加盐密码 |

`backend/src/` 按 config/models/schemas/services/controllers/routes/middlewares/utils/constants 分工；`backend/scripts/seed_demo.py` 管理幂等初始化；`backend/migrations/README.md` 说明 schema v1。`frontend/src/` 按 api/stores/types/components/common/hooks/pages/router/utils/constants 分工。实体链分别为 sample、batch、evidence，各自有模型、schema、服务、路由、API、store 和页面。

后端本地：创建虚拟环境并安装 `backend/requirements.txt`，先用 Compose 启动 db/db-replica/redis，设置本地可达的 DATABASE_URL/REPLICA_DATABASE_URL/REDIS_URL/JWT_SECRET，进入 backend 执行 `uvicorn src.main:app --reload`；worker 需 `/artifacts` 可写目录并执行 `python -m src.worker`。最省事的开发方式是修改源码后 `docker compose up -d --build`。前端通过 Nginx 的 `/api/` 去前缀代理访问后端，禁止双重 `/api/api`。

环境变量：COMPOSE_PROJECT_NAME（容器命名）、FRONTEND_PORT/BACKEND_PORT/DB_PORT（宿主端口）、POSTGRES_PASSWORD（演示库密码）、REPLICATION_PASSWORD（流复制账号）、JWT_SECRET（签名密钥）、SEED_DEMO（默认 true）、FALLBACK_WAIT_SECONDS（每批等库/等回退名额上限，默认 10 秒）。后端容器另接收 DATABASE_URL、REPLICA_DATABASE_URL 和 REDIS_URL。6 个服务均有 healthcheck；主库数据、副本数据、Redis、共享证据各自使用命名卷。

共享枚举位置：角色与任务状态在 `backend/src/constants/enums.py` 和 `frontend/src/constants/enums.js`；模型/鉴权服务/worker 消费后端值，状态标签与前端权限消费前端值。源数据 invoice status 为账单业务值 settled/pending，与任务状态独立。

## 已知边界

- 小型工作流没有样本删除和版本编辑；新口径新建样本/批次，旧记录保持独立。
- 使用固定业务数据和单实例 worker；不对接生产数据库，不部署生产高可用。
- 只允许受限 SQL 子集；不支持 CTE、窗口、跨表联接和用户自定义函数。
- 查询会执行两次（一次 EXPLAIN ANALYZE，一次取结果），但两侧查询在同一个 REPEATABLE READ 只读事务里共用同一快照，不跨库各读一次。
- 主库只容一批回退：回退名额是主库上的会话级 advisory lock，名额被占用时其他批次最多等十秒，超时明确失败且不发布结论。
- 固定业务数据与单副本演示；副本用物理流复制，不做逻辑复制、不保证跨版本升级。
- 后端迁移/seed 角色为本地数据库管理员；执行用户 SQL 时强制切换受限只读角色。生产应拆分独立迁移凭据。
- Web 字体来自 Google Fonts，离线会使用 serif/monospace 回退；不影响功能。

License: MIT，见 LICENSE。
