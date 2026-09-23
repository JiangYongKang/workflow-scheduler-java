# DAG 作业调度服务（workflow-scheduler-java）

一个可本地运行的 DAG 作业调度能力：通过 HTTP 提交 / 查询 / 取消作业，支持依赖编排、
三维并发配额、幂等提交、失败重试与进程崩溃后的自动恢复。持久化基于 H2 文件库，
状态机与计数以数据库为权威来源，在并发竞争、重复提交、进程重启下结果稳定且可解释。

---

## 1. 快速开始

```bash
# 构建 + 运行全部测试（需要 JDK 21+；已在 JDK 25 上验证）
mvn clean package
# 26 个测试会全部执行，其中恢复用例以子进程启动构建出的可执行 jar，无需手工预处理

# 仅启动服务（默认端口 8080，数据文件位于 ./data/scheduler.mv.db）
java -jar target/workflow-scheduler-java-1.0-SNAPSHOT.jar
```

---

## 2. HTTP 接口

| 方法 & 路径 | 说明 |
|---|---|
| `POST /api/jobs` | 提交单个作业（可带 `dependencies` 引用**已存在**的作业） |
| `POST /api/jobs/batch` | 批量提交一个 DAG（内部可互相引用同批次的 id） |
| `GET /api/jobs/{id}` | 按作业 id 查询 |
| `GET /api/jobs?idempotencyKey=xxx` | 按幂等键查询 |
| `DELETE /api/jobs/{id}` | 取消作业 |
| `GET /api/quota` | 查看三维配额与当前运行数快照 |

### 请求体字段

| 字段 | 说明 | 默认 |
|---|---|---|
| `id` | 作业 id；缺省服务端生成 `job-uuid` | 自动生成 |
| `idempotencyKey` | 幂等键，相同键重复提交返回同一作业 | 无（允许重复） |
| `jobType` | 任务类型：`NOOP` / `SLEEP` / `FAIL` / `FLAKY` | 必填 |
| `payload` | 任务参数（见内置任务说明） | 空 |
| `dependencies` | 上游作业 id 列表 | `[]` |
| `queue` | 队列名（配额维度） | `default` |
| `tenant` | 租户名（配额维度） | `default` |
| `maxAttempts` | 最大执行次数（含首次），`>=1` | `scheduler.retry.max-attempts=3` |
| `backoffBaseMs` | 首次失败后退避毫秒 | `scheduler.retry.backoff-base-ms=500` |
| `backoffMultiplier` | 退避指数倍率 | `scheduler.retry.backoff-multiplier=2.0` |

### 内置任务类型

- `NOOP`：立即成功。
- `SLEEP`：`payload` 为睡眠毫秒数（默认 1000），执行中每 50ms 轮询取消标记，可被取消。
- `FAIL`：恒定失败，`payload` 即失败原因（用于失败传播与重试验证）。
- `FLAKY`：`payload` 形如 `failFirst=2`，同一作业前 N 次失败、其后成功（验证重试成功）。

### 错误码（稳定可区分，随 HTTP 状态码返回）

```json
{ "code": "CYCLE_DETECTED", "message": "...", "path": "/api/jobs/batch", "time": "..." }
```

| code | HTTP | 触发条件 |
|---|---|---|
| `UNKNOWN_DEPENDENCY` | 400 | 引用了不存在的上游作业（接受前拒绝，不落任何作业） |
| `CYCLE_DETECTED` | 400 | 依赖图存在环（含自环） |
| `UNKNOWN_JOB_TYPE` | 400 | `jobType` 未注册 |
| `INVALID_REQUEST` | 400 | 字段缺失、批内 id / 幂等键重复、`maxAttempts<1` 等 |
| `JOB_NOT_FOUND` | 404 | 作业 id / 幂等键不存在 |
| `ALREADY_TERMINAL` | 409 | 对已终态作业执行取消 |
| `INTERNAL_ERROR` | 500 | 未预期异常 |

幂等命中不是错误：返回 HTTP 200，响应体 `deduplicated=true`，且 `id` 指向首个作业。

### 快速示例

```bash
# 批量提交 DAG: a -> b -> c
curl -s -X POST localhost:8080/api/jobs/batch -H 'Content-Type: application/json' -d '{
  "jobs":[
    {"id":"a","jobType":"NOOP"},
    {"id":"b","jobType":"SLEEP","payload":"300","dependencies":["a"]},
    {"id":"c","jobType":"NOOP","dependencies":["b"]}
  ]}'
curl -s localhost:8080/api/jobs/c

# 环依赖（被拒绝，400 CYCLE_DETECTED）
curl -i -X POST localhost:8080/api/jobs/batch -H 'Content-Type: application/json' -d '{
  "jobs":[{"id":"x","jobType":"NOOP","dependencies":["y"]},
          {"id":"y","jobType":"NOOP","dependencies":["x"]}]}'
```

---

## 3. 调度与依赖模型

### 状态机

```
                有上游未成功
  (提交) QUEUED ───────────────► PENDING
            ▲   ◄───────────────  │（上游全部 SUCCESS 后的下一轮 tick）
            │ 退避结束             │
            │                     │ 依赖满足 + 配额允许 + CAS 成功
            ├─────── RUNNING ◄────┘
            │      │   │   │
   失败可重试│     成功│  失败重试耗尽 执行中取消
            ▼      ▼   ▼       ▼
         QUEUED  SUCCESS FAILED CANCELLED
```

此外：
- 任一上游进入 `FAILED / CANCELLED / UNREACHABLE`，仍处于 `PENDING / QUEUED` 的下游
  **递归**标记为 `UNREACHABLE`，`failureReason` 沿链携带阻断它的直接上游与根因。
- `SUCCESS / FAILED / CANCELLED / UNREACHABLE` 为终态，不再迁移。
- 已在 `RUNNING` 的作业不受新发生的上游失败影响（它启动时上游均已成功）。

### 准入判定（每个 tick）

调度器由**单线程 tick** + 固定大小**工作线程池**组成。每个候选作业依次经过：

1. **退避判定**：`run_after > now` 的作业跳过（重试退避窗口未结束）。
2. **依赖判定**：
   - 全部上游 `SUCCESS` → `READY`；
   - 存在 `FAILED / CANCELLED / UNREACHABLE` 上游 → `BLOCKED`（兜底级联 UNREACHABLE）；
   - 其余（PENDING/QUEUED/RUNNING）→ `WAIT`，继续等待；
3. **配额判定**：全局、队列、租户三维当前 `RUNNING` 计数均未超限；
4. **CAS 准入**：`UPDATE jobs SET status='RUNNING', attempts=attempts+1
   WHERE id=? AND status IN ('PENDING','QUEUED')`。

**为什么并发下不会重复执行/超发：**

- 准入只在单线程 tick 发生，“看配额→CAS”之间没有第二个准入者；
- 状态迁移全部是带旧状态条件的 CAS，多轮 tick 重叠也只有一个赢家；
- `attempts` 仅在 CAS 到 RUNNING 时自增，`job_attempts` 表逐次落审计行，
  “执行次数”由数据库权威计数，不依赖进程内变量。

---

## 4. 并发与配额规则

三维上限**同时生效**，任一维已满则作业保持 `QUEUED`（排队等待，**不失败、不丢弃**），
下一轮 tick 重新参与准入；候选作业按 `createdAt, id` 做 FIFO。

| 维度 | 配置 | 默认 |
|---|---|---|
| 全局 | `scheduler.quota.global` | 4 |
| 队列（默认值） | `scheduler.quota.default-queue` | 2 |
| 租户（默认值） | `scheduler.quota.default-tenant` | 3 |
| 指定队列 | `scheduler.quota.queue.<队列名>=N` | 用默认 |
| 指定租户 | `scheduler.quota.tenant.<租户名>=N` | 用默认 |

运行数按数据库中 `status='RUNNING'` 的作业实时 `COUNT(*)`（可按 queue/tenant 分组），
因此**崩溃恢复后配额计数依然准确**。可用 `GET /api/quota` 观察实时占用。

---

## 5. 幂等与重复提交

- `jobs.idempotency_key` 上有唯一索引；提交先按键查询，未命中再插入，
  插入撞唯一键时回查既有记录返回——**先查-后插-撞键回查**三段保证并发提交也收敛为一个作业。
- 幂等响应：`deduplicated=true`，作业 `id` 为首个请求创建的 id，`attempts` 不增加。
- 批量提交中允许复用既有幂等键（该条返回去重结果，其余正常建 DAG）；
  批内新幂等键重复则整批 400 拒绝。
- 批次 DAG 校验在**所有插入之前**完成：环 / 未知上游 / 批内重复 id 任一出现，
  整批被拒绝，不会产生部分作业。

---

## 6. 失败重试与取消语义

### 重试

- 每次失败：`attemptNo < maxAttempts` → 回到 `QUEUED`，置
  `run_after = now + backoffBaseMs * backoffMultiplier^(attemptNo-1)`，退避结束后重新准入。
- 重试耗尽：→ `FAILED`，`failureReason` 形如
  `FAILED: 重试耗尽（共 N 次），末次原因: ...`，随后递归把下游标记 `UNREACHABLE`。
- 任务抛异常同样按失败处理（记录异常类型与消息），不会丢失作业。
- 每次尝试在 `job_attempts` 留一行（attempt_no、起止时间、SUCCESS/FAILED/CANCELLED、消息）。

### 取消（`DELETE /api/jobs/{id}`）

- `PENDING / QUEUED`：CAS 直接收口为 `CANCELLED`，其下游递归 `UNREACHABLE`，自身 `attempts=0`。
- `RUNNING`：置进程内取消标记，执行体在可中断点检测后以 `CANCELLED` 收口（best-effort），
  再级联标记下游。
- 已终态：`409 ALREADY_TERMINAL`；不存在：`404 JOB_NOT_FOUND`。

---

## 7. 重启恢复语义（at-least-once）

持久化使用 H2 文件库，数据源 URL 带 **`WRITE_DELAY=0`**（事务提交同步落盘，
避免 kill -9 时最近 1s 的提交滞留在内存而丢失）。启动时 `@PostConstruct` 执行：

```sql
UPDATE jobs SET status='QUEUED' WHERE status='RUNNING'
```

| 崩溃时状态 | 重启后行为 |
|---|---|
| `SUCCESS` | **原样保留，绝不重复执行**（attempts 不变） |
| `FAILED / CANCELLED / UNREACHABLE` | 终态原样保留，下游结论不变 |
| `QUEUED / PENDING` | 继续等待依赖 / 配额 / 退避，**不被跳过** |
| `RUNNING` | 回滚为 `QUEUED` 重新执行 → 该作业体现 at-least-once（attempts 比成功一次多 1） |

因此崩溃点正在执行的作业可能被执行两次（at-least-once 语义），而绝不会“执行成功却被重跑”
——成功状态本身是同步落盘的提交。执行中作业的任务体应自行保证幂等/可补偿。

---

## 8. 配置项一览（`application.properties`）

```properties
# 数据源（文件库；WRITE_DELAY=0 保证崩溃不丢已提交状态）
spring.datasource.url=jdbc:h2:file:./data/scheduler;AUTO_SERVER=TRUE;WRITE_DELAY=0
spring.sql.init.mode=always                 # schema.sql 幂等建表（IF NOT EXISTS）

scheduler.tick-interval-ms=200              # 调度 tick 间隔（另有事件唤醒，退化为最多等一个 tick）
scheduler.worker-threads=8                  # 执行工作线程数
scheduler.quota.global=4                    # 全局并发
scheduler.quota.default-queue=2             # 队列默认并发
scheduler.quota.default-tenant=3            # 租户默认并发
scheduler.quota.queue.fast=4                # 指定队列
scheduler.quota.queue.batch=1
scheduler.quota.tenant.tenant-a=2           # 指定租户
scheduler.quota.tenant.tenant-b=1
scheduler.retry.max-attempts=3
scheduler.retry.backoff-base-ms=500
scheduler.retry.backoff-multiplier=2.0
```

---

## 9. 可观测性（日志）

关键路径均打印**作业标识 + 状态迁移 + 判定依据**，便于排障：

- `job=xxx CAS 状态迁移成功 -> RUNNING (判定依据: 旧状态∈[PENDING, QUEUED])`
- `job=xxx 第2/3次尝试失败 -> QUEUED（退避 100ms…）`
- `job=xxx 第3次尝试失败且重试耗尽 -> FAILED reason=…`
- `job=yyy 状态迁移 -> UNREACHABLE，判定依据: 上游 xxx 不可达（…）`
- `job=xxx 重启恢复: RUNNING -> QUEUED（崩溃前在执行，按 at-least-once 重新执行）`
- `配额判定拒绝: 队列 batch 1/1 已满`
- `幂等命中 idempotencyKey=… -> 返回既有作业 …（不重复执行/计数）`

---

## 10. 验证方法（测试）

`src/test/.../tests` 下：

| 测试类 | 覆盖场景 |
|---|---|
| `DagValidatorTest` | 未知上游、环、自环、批内重复 id 的可区分拒绝（纯单测） |
| `DagFlowIntegrationTest` | 线性 / 菱形 DAG 顺序与单次执行；失败沿链传播 UNREACHABLE；重试成功 |
| `QuotaAndConcurrencyIntegrationTest` | 全局/队列/租户配额排队不丢弃、峰值不超限；20 线程并发同一幂等键只执行 1 次 |
| `CancelIntegrationTest` | 排队取消、PENDING 取消及级联、终态取消 409、404、执行中取消 best-effort |
| `JobHttpApiTest` | HTTP 状态码与三类 400 错误码可区分、幂等去重标记、409 |
| `RestartRecoverySubprocessTest` | **真实子进程**：提交后 `kill -9`，同库重启，断言成功不重跑 / 在执行重跑 / 下游不跳过 |

测试日志会打印每个作业在轮询中的状态、attempts 与最终判定依据（见 `TestAwait`）。

```bash
mvn clean package    # 测试前会自动生成可执行 jar，恢复用例直接运行
# 或只跑测试：mvn test（可执行 jar 会在 process-test-classes 阶段自动准备）
```
