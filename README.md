# 轨道交通振动数据湖：到期清理 + 故障调查保全

FastAPI + SQLite（WAL）+ 本地 JSONL 分区的振动数据湖 API。支持：

- **首次载入**随附清单（车号 / 采样时段 / 到期时间）与 JSONL 波形数据；重启**不会**重新加入已清理分区；
- 调查人员按**车号 + 时间闭区间**保全所有重叠分区；任一分区已开始删除或已清理则**整次拒绝**；解除前不得删除；重叠保全**分别生效**；
- **后台清理**只处理已到期且无生效保全的分区，**逐分区认领删除资格**；清理期间其他分区照常可操作；
- **两个 API 进程**共享同一 SQLite 与分区目录，保全与删除资格有**确定先后**；删除失败**绝不报已回收**；中断后重启**继续未完成删除**；索引与文件**最终一致**；
- 按车号、时段查询**尚存**的采样时间与值；另有保全与清理状态接口。

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

- api-a: http://localhost:8001/docs
- api-b: http://localhost:8002/docs

两个容器各运行一个 API 进程，挂载同一数据卷 `lake-data`（SQLite 库 + JSONL 分区文件）。
在任一进程的 `/docs` 里即可完成保全、清理、波形查询演示；也可以直接跑：

```bash
./scripts/demo.sh                 # 完整演示：保全 -> 清理 -> 查询 -> 解除 -> 回收
```

### 演示流程（/docs 内手动操作）

1. `GET /partitions` —— 8 个种子分区，P0001/P0002/P0004/P0006/P0008 已到期；
2. `GET /samples?vehicle_id=T203&start=...00:30:50Z&end=...00:31:10Z` —— 看故障冲击段波形；
3. `POST /preservations` 车号 `T203`、区间 `[2026-10-03T00:30:00Z, 2026-10-03T00:35:00Z]` —— 覆盖 P0006；
4. `POST /cleanup/run` —— 回收其余到期分区，P0006 因保全跳过；
5. 对 T101 `[00:10, 00:20]` 再发 `POST /preservations` —— 返回 **409**（P0001 已清理，整次拒绝）；
6. `POST /preservations/{id}/release` 解除后再次 `POST /cleanup/run` —— P0006 被回收；
7. `GET /cleanup/status` —— 状态统计、在途删除、失败记录。

### 演示删除失败语义（可选）

```bash
DELETE_FAIL_PARTITIONS=P0004 docker compose up --build
# P0004 进入 delete_failed（带错误信息），绝不会被报为已回收；
# 去掉该环境变量重启后，后台循环自动重试并成功回收。
```

### 验证重启幂等

```bash
docker compose restart api-a
# 已清理分区保持 cleaned，文件不会被种子数据重新拷回。
```

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/partitions` | 分区清单（状态、到期、生效保全数、文件是否在盘） |
| GET | `/samples` | 按车号 + 时间闭区间查询尚存样本（仅 active 分区） |
| POST | `/preservations` | 保全所有重叠分区（原子；409/404 语义见下） |
| GET | `/preservations` / `/{id}` | 保全列表 / 详情 |
| POST | `/preservations/{id}/release` | 解除保全（幂等） |
| POST | `/cleanup/run` | 立即执行一轮清理 |
| GET | `/cleanup/status` | 清理状态总览 |
| GET | `/health`, `/meta/info` | 健康与进程信息 |

**保全拒绝语义**：无重叠分区 → `404`；任一重叠分区为 `deleting` / `delete_failed` / `cleaned` → `409` 整次拒绝（不产生任何保全记录）。

## 设计要点

### 分区与索引

- 每个分区 = 一个 JSONL 文件（每行一个样本：`ts, vehicle_id, ax, ay, az`）+ 一行 `partitions` 索引；
- 分区状态机：`active → deleting → cleaned`，失败分支 `deleting → delete_failed →（重试）deleting …`；
- `cleaned` 行作为**墓碑**永久保留：重启时种子载入以 `partition_id` 主键 `INSERT OR IGNORE`，已清理分区既不重新插行、也不回拷文件。

### 两个进程间“保全 vs 删除资格”的确定先后

所有“检查 + 变更”的关键段（保全创建、删除资格认领、种子载入、reconcile）都包在
`BEGIN IMMEDIATE` 事务里。SQLite 写锁把这些关键段**全序化**：

- 保全事务先提交 → 清理认领时的 `NOT EXISTS(生效保全)` 看到它，跳过该分区；
- 清理认领先提交（`active→deleting`）→ 保全事务看到非 `active` 状态，整次 409；
- 认领用条件 `UPDATE` + 行数校验，两个清理进程不会同时拿到同一分区。

清理认领后**先释放数据库锁再删文件**：清理某分区期间，其他分区的查询、保全不受影响。

### 崩溃恢复与最终一致

- 认领带**租约**（`DELETE_LEASE_SECONDS`，默认 30s）：持有进程崩溃后，租约到期才允许其他进程接管，避免双删；
- 删文件是幂等的：文件已不存在视为删除成功（覆盖“文件已删、状态未落库”的崩溃窗口），随后置 `cleaned`；
- 删除失败置 `delete_failed` 并记录错误，后续 pass 自动重试，**绝不误报已回收**；
- 每次启动执行 `reconcile`：删除“无索引行或行已 cleaned”的孤儿文件，索引与磁盘最终一致。

### 种子载入

`seed/manifest.json` + `seed/partitions/*.jsonl` 随镜像分发（`scripts/generate_seed.py` 可重新生成）。
每次启动幂等执行：新分区插行 + 同事务拷文件；已存在（含已清理）分区跳过。

## 本地开发

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
DATA_DIR=./var .venv/bin/uvicorn app.main:app --port 8000   # 单进程
# 两个进程（模拟 compose 拓扑，共享 ./var）：
DATA_DIR=./var PROCESS_NAME=a .venv/bin/uvicorn app.main:app --port 8001 &
DATA_DIR=./var PROCESS_NAME=b .venv/bin/uvicorn app.main:app --port 8002 &
```

## 测试

```bash
python3 tests/e2e_test.py    # 真实拉起双进程，覆盖全部需求语义（7 个场景）
```

场景：重启幂等 / 崩溃续删 / 删除失败不误报+重试 / 双进程保全-清理竞态（6 轮不变量）/
reconcile 孤儿文件 / 保全闭区间与原子拒绝与独立生效 / 清理期间可操作性。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATA_DIR` | `/data`（容器） | SQLite 与分区文件目录 |
| `SEED_DIR` | `/app/seed`（容器） | 种子清单与数据 |
| `CLEANUP_INTERVAL_SECONDS` | `15`（compose 中为 `120`） | 后台清理周期；首轮在启动一个周期后执行，给演示留出保全时间窗 |
| `DELETE_LEASE_SECONDS` | `30` | 删除资格租约 |
| `DELETE_RETRY_SECONDS` | `30` | 删除失败后的重试间隔（防止持续失败分区饿死其他分区） |
| `CLEANUP_MAX_PER_PASS` | `100` | 单轮清理分区上限 |
| `BACKGROUND_CLEANUP_ENABLED` | `true` | 后台循环开关（手动 `/cleanup/run` 不受影响） |
| `DELETE_FAIL_PARTITIONS` | 空 | 故障注入：指定分区删除必失败（演示/测试用） |
