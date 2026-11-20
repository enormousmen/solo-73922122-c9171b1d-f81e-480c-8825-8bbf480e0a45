# restart-permit-api

存储集群值班员用的**分批重启许可 API**。FastAPI + SQLite，单容器即可运行。

核心规则：

- 服务首次启动时读取**分片投票副本清单**（`manifest.json`）作为初始清单；
  之后可通过 API **不停机切换**，最新已提交的清单落盘，重启后沿用。
- 节点探活报告携带**采样时间、健康状态、启动标识**；每个节点以**采样时间最新**的一份为准。
- 值班员提交**申请号 + 拟重启的一批节点**，满足以下条件才批准整批：
  1. 本批每个节点最新探活均健康；
  2. 本批节点未被任何**未结束许可**占用；
  3. 扣除 *本批节点 + 未结束许可占用节点 + 健康未知/异常节点* 后，本批涉及的**每个分片**剩余可用投票副本均**过半**；否则拒绝并列出受影响分片。
- **清单切换**携带当前版本号与完整新清单，满足条件才生效：
  1. 版本号必须等于当前生效版本（防止基于过期清单覆盖）；
  2. 每个**未结束许可**占用的节点必须仍在新清单中；
  3. 每个**新增分片**或**投票成员发生变动的分片**，扣除未结束许可占用节点及
     健康未知/异常节点后，剩余可用投票副本仍过半，否则列出对应分片。
  版本不符、条件不满足或保存失败时事务回滚，**旧清单继续生效**。
- **并发申请/切换**在写事务（`BEGIN IMMEDIATE`）中串行判定，守住多数票。
- **同号同批重发**返回原许可（HTTP 200），即使该节点已不在当前清单中；
  同号不同批返回 409。
- 许可与探活记录落盘 SQLite，**服务重启后保留**。
- **结束许可**要求本批每个节点的最新探活：在获批后采集、健康、启动标识与获批时不同。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 服务监听 http://localhost:8000 ，交互文档 http://localhost:8000/docs
```

- SQLite 数据在命名卷 `permit-data`（容器内 `/data/permits.db`），重启不丢；
  **已提交的清单也保存在同一数据库**，重启后继续生效。
- 分片清单 `./manifest.json` 只读挂载进容器，**只用于首次初始化**（数据库还没有
  任何已提交清单时）。若库里已有清单，修改挂载文件并重启**不会**覆盖它；
  需要改清单请调用 `POST /manifest/switch`。

本地开发（不用 Docker）：

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
DB_PATH=./data/permits.db MANIFEST_PATH=./manifest.json \
  uvicorn app.main:app --host 0.0.0.0 --port 8000
```

运行测试：

```bash
pytest tests/ -q
```

## 分片清单格式

挂载文件（初始清单，`version` 可选，缺省为 1）：

```json
{
  "version": 1,
  "shards": {
    "shard-a": ["n1", "n2", "n3"],
    "shard-b": ["n2", "n3", "n4"],
    "shard-c": ["n3", "n4", "n5"]
  }
}
```

每个分片列出全部投票副本节点；过半线 = `floor(票数/2)+1`（3 票需 2 可用）。
节点名去重排序后参与比较，成员顺序不影响“是否变动”的判定。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/healthz` | 服务健康检查 |
| GET | `/manifest` | 查看**当前生效**清单：版本号、各分片及过半线 |
| POST | `/manifest/switch` | 切换清单（201 生效 / 409 版本不符或条件不满足 / 500 保存失败；失败时旧清单继续生效） |
| POST | `/liveness` | 批量上报探活（201；按当前生效清单校验节点） |
| GET | `/liveness` | 全部节点最新探活 |
| GET | `/liveness/{node}` | 单节点最新探活 |
| POST | `/permits` | 申请许可（201 新批 / 200 幂等重发 / 409 拒绝） |
| GET | `/permits?status=open\|finished` | 许可列表 |
| GET | `/permits/{request_id}` | 单个许可 |
| POST | `/permits/{request_id}/finish` | 结束许可（200 / 409 条件不满足） |

## 调用示例

### 1. 上报探活

```bash
curl -X POST http://localhost:8000/liveness \
  -H 'Content-Type: application/json' -d '{
    "reports": [
      {"node_id":"n1","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"},
      {"node_id":"n2","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"},
      {"node_id":"n3","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"},
      {"node_id":"n4","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"},
      {"node_id":"n5","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"}
    ]}'
# => {"accepted":5}
```

### 2. 申请许可

```bash
curl -X POST http://localhost:8000/permits \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-001","nodes":["n1"]}'
# 201 =>
# {"request_id":"req-001","nodes":["n1"],"status":"open",
#  "approved_at":"...","finished_at":null,
#  "boot_ids_at_approval":{"n1":"boot-1"}}
```

同号同批重发，返回原许可（200，`approved_at` 不变）；同号不同批返回 409。

### 3. 多数票不足被拒（列出受影响分片）

`req-001` 已占用 n1 时再申请 n2（同属 shard-a，3 票最多扣 1 票）：

```bash
curl -X POST http://localhost:8000/permits \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-002","nodes":["n2"]}'
# 409 =>
# {"detail":{"ok":false,"error":"permit_rejected",
#   "affected_shards":[{"shard":"shard-a","voters":3,"required_available":2,
#     "available":1,"available_nodes":["n3"],
#     "excluded":{"n1":"occupied_by:req-001","n2":"in_batch"}}]}}
```

其它拒绝类别：`health_unknown_nodes`（健康未知）、`unhealthy_nodes`（异常）、
`occupied_nodes`（被未结束许可占用）。清单外节点一律 422。

### 4. 查询并切换清单（不停机）

先查询当前版本：

```bash
curl http://localhost:8000/manifest
# 200 => {"version":1,
#   "shards":{"shard-a":{"voters":["n1","n2","n3"],"majority_needed":2},
#             "shard-b":{"voters":["n2","n3","n4"],"majority_needed":2},
#             "shard-c":{"voters":["n3","n4","n5"],"majority_needed":2}}}
```

把 n4 加入 shard-a，并新增 shard-d（携带**完整**新清单）：

```bash
curl -X POST http://localhost:8000/manifest/switch \
  -H 'Content-Type: application/json' -d '{
    "current_version": 1,
    "shards": {
      "shard-a": ["n1","n2","n3","n4"],
      "shard-b": ["n2","n3","n4"],
      "shard-c": ["n3","n4","n5"],
      "shard-d": ["n1","n2","n5"]
    }}'
# 201 =>
# {"version":2,
#  "shards":{"shard-a":{"voters":["n1","n2","n3","n4"],"majority_needed":3},
#            "shard-d":{"voters":["n1","n2","n5"],"majority_needed":2}, ...}}
```

切换成功返回 **201**，版本号递增；此后探活上报、许可申请立即按新清单受理，
无需重启。并发提交同一版本时只有一个成功，另一个收到 409 版本冲突。

### 5. 切换被拒（版本不符 / 条件不满足）

版本号过期（例如另一值班员刚切换过）：

```bash
# 再提交 current_version=1
# 409 => {"detail":{"error":"manifest_version_conflict",
#                   "current_version":2,"submitted_version":1}}
```

欲移除仍被未结束许可占用的 n1：

```bash
curl -X POST http://localhost:8000/manifest/switch \
  -H 'Content-Type: application/json' -d '{
    "current_version": 2,
    "shards": {
      "shard-a": ["n2","n3","n4"],
      "shard-b": ["n2","n3","n4"],
      "shard-c": ["n3","n4","n5"],
      "shard-d": ["n1","n2","n5"]
    }}'
# 409 => {"detail":{"ok":false,"error":"manifest_switch_rejected",
#   "checked_shards":["shard-a","shard-d"],
#   "missing_occupied_nodes":[{"node":"n1","permits":["req-001"]}]}}
```

新分片/变动分片扣除后票数不足：

```bash
# 409 => {"detail":{"ok":false,"error":"manifest_switch_rejected",
#   "checked_shards":["shard-d"],
#   "affected_shards":[{"shard":"shard-d","voters":3,"required_available":2,
#     "available":1,"available_nodes":["n4"],
#     "excluded":{"n1":"occupied_by_open_permit","n2":"unhealthy"}}]}}
```

保存失败（如磁盘错误）返回 500 `manifest_persist_failed`。以上任何失败均回滚，
`GET /manifest` 仍返回旧版本，旧清单继续生效。

### 6. 节点重启后上报新探活，结束许可

```bash
# 节点重启完成：采样时间晚于获批时间、健康、启动标识已变
curl -X POST http://localhost:8000/liveness \
  -H 'Content-Type: application/json' -d '{
    "reports":[{"node_id":"n1","sampled_at":"2026-11-19T10:20:00Z",
                "healthy":true,"boot_id":"boot-2"}]}'

curl -X POST http://localhost:8000/permits/req-001/finish
# 200 => {"request_id":"req-001",...,"status":"finished","finished_at":"..."}
```

结束条件不满足时 409 并给出逐节点原因，例如：

```json
{"detail":{"error":"finish_conditions_not_met",
           "nodes":{"n1":"boot_id_unchanged"}}}
```

可能原因：`no_liveness_report`、`not_sampled_after_approval`、`unhealthy`、
`boot_id_unchanged`。许可结束后节点被释放，可被后续申请占用。

## 设计说明

- **清单生效顺序**：已提交清单存 `manifests` 表（版本号主键）。启动时若有记录，
  直接沿用最高版本；没有才用挂载文件初始化并写入第 1 版（或文件中的 `version`）。
  切换的“版本校验 → 条件判定 → 落库 → 切换内存清单”在同一把应用锁 + 写事务里完成，
  内存清单只在 COMMIT 成功后更新；提交失败则保持旧清单。
- **探活与切换**：探活上报在应用锁内读取当前生效清单做节点校验，再写入；
  不会出现按旧清单放行、按新清单落库的交错。许可申请/结束同样在写事务中
  读取当前清单、探活与占用状态后再判定。
- **幂等**：`request_id` 是许可表主键。节点集合规范化（去重排序）后比较，
  同号同批重放直接返回原记录（无论节点是否仍在清单中、许可是否已结束）；
  同号不同批 409。被拒绝的申请不落库，整改后可用同号重试。
- **并发**：所有写路径走单连接 + 可重入锁 + `BEGIN IMMEDIATE` 事务，
  "检查多数票 → 写入"是原子的；并发申请/切换串行判定，不会超批或覆盖。
- **持久化**：许可、全部探活报告与清单历史存 SQLite（WAL 模式），最新探活按
  `max(sampled_at)` 实时计算，服务重启后状态完整恢复。
- **结束判定**：批准时快照各节点 `boot_id`；结束时要求最新报告
  `sampled_at > approved_at` 且健康且 `boot_id` 不同。

## 已知限制 / 未完成项

- 单 worker 运行（SQLite 单写者）；如需多副本需换外部数据库，并把内存清单
  改为每次请求从数据库读取（当前内存清单以进程为单位）。
- 未做认证鉴权，假定部署在受信内网；`/manifest/switch` 权限较大，上线前应加鉴权。
- 切换只校验**新增或成员变动**的分片；未变动的分片沿用既有状态，不因切换被拒。
  若希望全量重校验，可改为校验所有分片。
- 清单切换不校验“分片名被删除/改名的历史许可”：未结束许可按节点校验，只要节点
  仍在新清单任一分片中即可。分片层面的归属变化不做额外约束。
- 探活无"过期"概念：只要最新一份健康即视为健康，不判断采样时间新旧。
- 结束许可比较的是节点上报的采样时钟与服务端获批时钟，依赖各节点时钟基本同步（NTP）。
- 许可无过期/自动回收机制；值班员需显式 finish。
