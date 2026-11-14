# 站点逐日降水批量采集系统

气象数据工程师批量补取站点逐日降水、送入干旱分析库的采集 API。FastAPI + SQLite，
两个采集进程共享同一数据库，在不触发源站封禁的前提下并行采集。

## 限速与可靠性规则（已全部实现）

| 规则 | 实现方式 |
| --- | --- |
| 同源全局在途请求 ≤ 2（两进程共享） | `inflight_slots` 表计数，认领条目时在 `BEGIN IMMEDIATE` 短事务内原子检查 |
| 同源相邻请求开始间隔 ≥ 1 秒 | `source_states.last_started_at` 墙钟时间戳，跨进程共享判定 |
| 收到 429 后按 `Retry-After` 整数秒冷却，从**收到响应时刻**起算 | `source_states.cooldown_until = received_at + retry_after`，持久化在库中 |
| 后续响应不能缩短冷却 | 新冷却只取 `max(新值, 旧值)`；200 响应不写冷却字段 |
| 冷却只针对该源，其他源继续 | 冷却按 `source_base` 隔离，worker 循环按源逐个尝试 |
| 429 项自动重试 | 条目回 `pending`、释放名额，下一轮重新认领 |
| 其他 HTTP 错误 / 字段不符 → 该项失败 | `failed` + `error_code/error_message`，不重试 |
| 等待网络或冷却不占库写事务 | 仅在认领/落库瞬间开短事务，HTTP 请求与 sleep 期间无事务；SQLite WAL 保证查询始终可响应 |
| **响应登记遇写锁：保留响应并自动接续** | 响应到手后先落“未登记标记”，登记失败（写锁被占）时原地指数退避重试；重试只复用已到手的响应，**绝不重新取源**。专用登记连接 + 250ms 等锁上限，撞锁快速让出，不拖住心跳/认领/其它源 |
| **成功值 / 普通错误与名额释放同事务** | `_apply_outcome` 在同一个 `BEGIN IMMEDIATE` 里更新条目并删 `inflight_slots`，不会出现“有名额无结果”或反之 |
| **429：回待采 + 释放名额 + 冷却同事务** | 三者写在同一个写事务；冷却 `cooldown_until = received_at + Retry-After`，从**收到响应的瞬间**起算，登记延迟多久都不后移；已有更晚冷却时取 `max`，不缩短 |
| **解锁后另一进程不得抢发** | 未登记标记（`COORD_DIR`，与库同卷）在登记成功前一直存在；任何采集进程认领该源前都会看到并跳过它。写锁刚释放的窗口也被封住 |
| **登记等待不拖住其它源** | 看到标记只跳过该源、不阻塞主循环；查询（进度/结果）走独立只读连接，始终可响应 |
| 等待登记不占写事务、进度和结果查询保持响应 | 见上；`GET /sources` 额外展示 `cooldown_remaining` 与 `pending_registrations` |
| 进程退出后自动接续、不重复保存 | 优雅退出把在途项回 `pending` 并删名额；重启后续跑；成功项更新带 `status='running' AND worker_id=<自己>` 条件，重复结束不覆盖结果 |
| 不误回收存活进程名额 | 只回收心跳超过租约期（15s）的 worker 名下名额；**响应正在等登记的条目不回收**（标记保护），标记 TTL 过期确认无人接续后才解除保护 |
| 重启不能绕过剩余冷却 | 冷却结束时刻是库内墙钟绝对值，新进程启动直接读取，无内存重置入口 |
| 降水量校验 | 必须是数字且非负（毫米），并核对返回站号、日期与请求一致 |

## 目录结构

```
app/
  config.py   运行参数（环境变量可覆盖）
  db.py       SQLite 连接与表结构（WAL）
  api.py      采集 API（提交/进度/错误/结果/限速状态）
  worker.py   采集进程（限速、冷却、租约、优雅退出、死亡回收、响应登记重试）
  coord.py    未登记响应标记（跨进程、不依赖数据库写锁的协调通道）
  source.py   本地模拟气象源（可延迟、可发 429、记录请求时刻）
  holdlock.py 故障注入：以另一连接身份持写锁，复现登记冲突
scripts/
  demo.sh               端到端演示（含写锁/响应登记场景）
  worker_healthcheck.sh 容器健康检查
tests/
  conftest.py           受控响应、独立连接写锁、请求时刻记录
  test_registration.py  同进程双 worker：登记遇锁/429/查询响应/限额/重启
  test_cross_process.py 真实两个采集子进程：标记协调与跨进程冷却
  test_e2e.py           source + api + 两 worker 全链路（无 Docker 版）
docker-compose.yml      source + api + worker1 + worker2
```

## 启动

```bash
docker compose up --build
```

- 采集 API 与交互文档：http://localhost:8080/docs
- 模拟气象源与控制接口文档：http://localhost:8081/docs
- 两个采集进程 `worker1`、`worker2` 共享卷 `collector-data` 中的 `collector.db`

## 交互式演示（Swagger UI）

1. 打开气象源文档 http://localhost:8081/docs ，调用 `POST /control/policy`，
    body 填 `{"mode":"nth429","n":3,"retry_after":3}` —— 每 3 个请求返回一次 429。
2. 打开采集 API 文档 http://localhost:8080/docs ，调用 `POST /jobs`：

   ```json
   {
     "source_base": "http://source:8081",
     "items": [
       {"station_id": "S1001", "date": "2026-11-10"},
       {"station_id": "S1001", "date": "2026-11-11"},
       {"station_id": "S1002", "date": "2026-11-10"}
     ]
   }
   ```

   单次最多 20 组站号/日期。
3. 调用 `GET /jobs/{job_id}/progress` 看进度（pending/running/succeeded/failed）。
4. 调用 `GET /jobs/{job_id}/results` 读取站日降水结果；
   `GET /jobs/{job_id}/errors` 看失败项；`GET /jobs/{job_id}/items` 看全部条目，
   429 重试可在 `attempts` 与 `error_message` 中看到痕迹。
5. 调用 `GET /sources` 查看各源在途数、最近请求开始时刻、冷却剩余秒数
   （`cooldown_remaining`）、待登记响应（`pending_registrations`）与 worker 心跳。
6. 回到气象源文档调用 `GET /control/stats`：`min_gap_seconds` 应 ≥ 1，
   `max_concurrent_observed` 应 ≤ 2；`GET /control/requests` 可逐条核对请求到达时刻。

### 演示“响应已返回、登记撞写锁”

无需额外准备，按上面启动后：

1. 在气象源文档 `POST /control/policy` 填 `{"mode":"nth429","n":1,"retry_after":4}`
   —— 下一个请求必定 429。
2. 采集 API 文档 `POST /jobs` 提交一条站日，等约 2 秒（worker 已收到 429）。
3. 在 API 容器里持写锁 8 秒，模拟“另一连接持锁超过 busy_timeout”：

   ```bash
   docker compose exec api python -m app.holdlock --seconds 8
   ```

   持锁期间：
   - `GET /jobs/{id}/progress` 与 `GET /jobs/{id}/results` **照常立即返回**；
   - `GET /sources` 的 `pending_registrations` 能看到这条响应正在等登记；
   - 条目保持 `running`、名额仍占用、库中尚无冷却（响应被保留，没有重新取源）。
4. 解锁后不干预：worker 自动接续登记，429 条目回 `pending`，
   `cooldown_until ≈ 收到响应时刻 + 4s`（**不会**因为登记晚了而后移）。
   冷却到期后自动重采并成功；`GET /jobs/{id}/items` 的 `attempts` 为 2。
5. `GET /control/requests` 可确认：429 之后的下一次同源请求一定晚于冷却截止时刻，
   另一个采集进程也没有抢发。

脚本版（第 8 段自动完成上述步骤）：

```bash
docker compose exec api bash /app/scripts/demo.sh   # 容器内（源用 compose 服务名）
# 或宿主机（容器已映射端口，需把任务里的源地址也指向本机）：
BASE=http://localhost:8080 SRC=http://localhost:8081 \
  JOB_SOURCE=http://localhost:8081 bash scripts/demo.sh
```

## 其他可演示场景

- **延迟响应观察在途上限**：`GET /daily?...&delay=5`，或提交后用
  `GET /sources` 观察 `inflight` 不超过 2。
- **字段不符 → 失败项**：`POST /control/policy` body `{"mode":"bad_payload"}`，
  新任务的条目会标记为 `failed / FIELD_MISMATCH`。
- **普通 HTTP 错误**：把任务的 `source_base` 指向一个返回 500 的地址（或不存在的路径），
  条目标记 `failed / HTTP_500`，不重试。
- **进程退出接续**：`docker compose kill worker1`（SIGKILL 模拟宕机），
  worker2 的心跳回收逻辑会在租约到期后把它的在途项接走（响应正在等登记的条目
  除外，它们受标记保护，等登记完成或标记 TTL 到期后才解除）；
  `docker compose restart worker1` 后冷却仍在库中继续生效。
  正常 `docker compose stop` 走优雅退出，在途项立即回 `pending`。

## 测试

```bash
pip install -r requirements-dev.txt
pytest            # 11 个用例，约 35 秒
```

覆盖：响应返回后写锁被占超过 busy_timeout（响应保留、自动接续、不重取源）、
登记重试幂等、429 冷却按收到响应时刻起算且不被缩短、解锁后另一进程不抢发、
等待登记时进度/结果查询仍可响应、单源冷却不阻塞其它源、在途上限与请求间隔、
重启接续，以及两个真实采集子进程的跨进程协调和全链路（source+api+2 worker）。

## 配置（环境变量）

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `MAX_INFLIGHT_PER_SOURCE` | 2 | 每源全局在途上限 |
| `MIN_REQUEST_GAP_SECONDS` | 1 | 同源相邻请求开始最小间隔 |
| `LEASE_SECONDS` | 15 | 心跳/租约时长，超过即判定进程死亡 |
| `HEARTBEAT_INTERVAL_SECONDS` | 2 | 心跳与续租间隔 |
| `WORKER_CONCURRENCY` | 5 | 单进程并发处理条目数 |
| `HTTP_TIMEOUT_SECONDS` | 20 | 上游请求超时（超时记 `NETWORK_ERROR` 失败） |
| `POLL_INTERVAL_SECONDS` | 0.25 | 空转轮询间隔 |
| `DB_BUSY_TIMEOUT_MS` | 10000 | 认领/心跳等主连接等写锁上限 |
| `REGISTRATION_BUSY_TIMEOUT_MS` | 250 | 登记专用连接等写锁上限（撞锁快速让出） |
| `REGISTRATION_RETRY_MIN_SECONDS` | 0.05 | 登记重试初始退避 |
| `REGISTRATION_RETRY_MAX_SECONDS` | 2 | 登记重试退避上限（持续重试，无总次数上限） |
| `REGISTRATION_MARK_TTL_SECONDS` | 10 | 未登记标记的判定有效期；持有者每次重试都会刷新 |
| `COORD_DIR` | `$DATA_DIR/coord` | 未登记响应标记目录（两个采集进程必须共享同一卷） |
