# DAG 作业调度器（Spring Boot 3 / Java 21）

一个可本地运行的 DAG 作业调度服务：通过 HTTP 提交 / 查询 / 取消作业，支持依赖编排、
全局 + 队列 + 租户三级并发配额、幂等提交、失败重试、失败/取消沿依赖链传播，以及基于
JSON 文件持久化的进程重启恢复。

## 1. 快速开始

```bash
mvn spring-boot:run            # 启动，默认 http://localhost:8080
mvn test                       # 运行全部测试（24 个用例）
```

持久化目录默认 `data/scheduler/jobs.json`，可用配置项覆盖。

## 2. HTTP 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/jobs` | 批量提交作业（原子准入：整批校验通过才落盘） |
| GET  | `/api/jobs` | 查询全部作业 |
| GET  | `/api/jobs/{id}` | 查询单个作业（含状态、尝试次数、失败原因、阻塞上游、判定依据） |
| POST | `/api/jobs/{id}/cancel` | 取消作业 |
| GET  | `/api/health` | 探活 |

### 提交请求

```json
{
  "jobs": [
    {"id": "a", "taskType": "noop"},
    {"id": "b", "taskType": "sleep", "taskParam": 500},
    {"id": "c", "taskType": "flaky", "taskParam": 2,
     "queue": "q1", "tenant": "t1",
     "idempotencyKey": "biz-key-001",
     "retryPolicy": {"maxAttempts": 3, "backoffMillis": 100},
     "dependencies": ["a", "b"]}
  ]
}
```

字段说明：

- `id`：可缺省，缺省由服务端生成；批次内 / 与存量冲突返回 `JOB_ALREADY_EXISTS` / `DUPLICATE_JOB_ID`。
- `taskType`：内置 `noop`（立即成功）、`sleep`（睡眠 `taskParam` 毫秒，响应中断）、
  `flaky`（前 `taskParam` 次尝试抛错，之后成功，用于验证重试与传播）。
- `dependencies`：上游作业 id 列表。
- `queue` / `tenant`：配额维度，缺省均为 `default`。
- `idempotencyKey`：幂等键。
- `retryPolicy.maxAttempts`：**含首次执行**的总尝试次数，`1` 表示不重试；`backoffMillis` 为固定退避。

### 提交响应

```json
[{"jobId": "c", "state": "PENDING", "duplicate": false, "detail": "已接受"}]
```

`duplicate=true` 表示命中幂等键、返回既有作业，不会重复执行或重复计数。

## 3. 调度与依赖模型

### 状态机

```
PENDING ──获得配额──► RUNNING ──成功──────────► SUCCESS（终态）
   │                   │
   │配额超限           ├──失败且仍有重试额度──► PENDING（退避后重试）
   ▼                   ├──失败且重试耗尽──────► FAILED（终态）
 QUEUED ◄─┐            └──取消/中断──────────► CANCELLED（终态）
   │      │
   └─配额释放/上游推进后回到 PENDING
 PENDING/QUEUED 且任一上游 FAILED/CANCELLED/UNREACHABLE ──► UNREACHABLE（终态）
```

- 作业仅当**全部上游均为 SUCCESS** 时才具备执行资格；调度循环（单线程）每 `tick-millis`
  毫秒做一次就绪判定与配额分配。
- 上游 `FAILED` / `CANCELLED` 时，所有尚未执行的下游被标记为 `UNREACHABLE`，并沿依赖链
  逐层传播；`blockedBy` 字段记录直接阻塞的上游 id，下游**不会被执行**（`attempts` 保持 0）。
- 所有状态迁移都以 INFO/WARN 日志打印 `jobId、from->to、判定依据`；作业视图的 `detail`
  字段同样保存最近一次判定的人类可读依据。

### 准入校验（接受前拒绝，原因可区分）

整批提交在任何持久化之前依次校验，失败时整批拒绝，错误码：

| 错误码 | HTTP 状态 | 触发条件 |
|--------|-----------|----------|
| `INVALID_REQUEST` | 400 | 空批次、空 id 等 |
| `DUPLICATE_JOB_ID` | 400 | 同一批次内出现重复作业 id |
| `UPSTREAM_NOT_FOUND` | 400 | 引用了不存在的上游（消息含具体上游 id） |
| `CYCLIC_DEPENDENCY` | 400 | 依赖图存在环（消息含环路径） |
| `JOB_ALREADY_EXISTS` | 409 | 作业 id 与存量冲突 |
| `IDEMPOTENCY_KEY_CONFLICT` | 409 | 相同幂等键但语义内容不一致 |
| `JOB_NOT_FOUND` | 404 | 查询 / 取消未知作业 |
| `CANCEL_NOT_ALLOWED` | 409 | 取消已 SUCCESS 的作业 |

## 4. 并发与配额规则

三道配额**同时**生效，任一超限作业进入 `QUEUED` 排队——**不失败、不丢弃**：

1. 全局：`scheduler.global-concurrency`（RUNNING 作业总数上限，同时也是工作线程数）；
2. 队列：`scheduler.queue-concurrency.<name>`，未配置的队列取 `default-queue-concurrency`；
3. 租户：`scheduler.tenant-concurrency.<name>`，未配置的租户取 `default-tenant-concurrency`。

当任意作业执行结束（成功 / 失败 / 取消）或作业被取消时，排队作业被唤醒并在下一个 tick
重新做依赖与配额判定，因此槽位释放后排队作业会继续推进。所有账本读写都在调度器锁内，
配合单线程调度循环，竞争条件下 RUNNING 数量永不超配额（见 `ConcurrencyRaceTest`）。

## 5. 幂等语义

- 相同 `idempotencyKey` 的并发 / 顺序重复提交解析到**同一个作业 id**：首次正常接受，
  其余响应 `duplicate=true`；作业只执行一次，只计数一次。
- 幂等键相同但关键语义（taskType / taskParam / dependencies / queue / tenant）不一致时，
  返回 `IDEMPOTENCY_KEY_CONFLICT`（409），避免错误复用。
- 幂等索引随持久化文件保存，重启后仍然生效。

## 6. 失败重试语义

- `maxAttempts` 为含首次的总尝试次数；失败后若 `attempts < maxAttempts`，作业回到
  `PENDING` 并记录 `retryAfter = now + backoffMillis`，退避结束后重新参与正常的依赖与
  配额判定（重试不绕过配额）。
- 重试耗尽进入 `FAILED`，随后下游按依赖链标记 `UNREACHABLE`；`failureReason` 保留最近
  一次失败的异常类型与消息。
- flaky 任务在重试预算内成功后即正常结束，下游照常执行。

## 7. 取消语义

- `PENDING` / `QUEUED`：立即变为 `CANCELLED`，下游传播为 `UNREACHABLE`。
- `RUNNING`：发送协作中断（线程 interrupt），任务（如 sleep）响应中断后作业变 `CANCELLED`；
  若“成功”与“取消”发生竞争，取消请求优先，结果稳定可解释。
- `SUCCESS`：拒绝取消（`CANCEL_NOT_ALLOWED`）；其他终态下取消是幂等的空操作。

## 8. 重启恢复语义

每次状态迁移都会把全部作业以 JSON 原子写入（临时文件 + atomic move）
`<store-dir>/jobs.json`。启动时：

1. 重建作业表、下游索引、幂等索引；
2. 崩溃时处于 `RUNNING` 的作业回置为 `PENDING` 重新调度（at-least-once；其已持久化的
   `attempts` 计入重试预算，因此不会获得额外重试次数）；
3. `QUEUED` 作业回置为 `PENDING`，重新等待依赖与配额；
4. `SUCCESS` / `FAILED` / `CANCELLED` / `UNREACHABLE` 终态原样保留——**已成功作业绝不
   重复执行，未完成且可运行的作业绝不被跳过**；
5. 重新执行一次不可达传播，修复崩溃发生在传播中途的情况。

## 9. 配置项（`application.properties`，前缀 `scheduler`）

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `global-concurrency` | 4 | 全局 RUNNING 上限 / 工作线程数 |
| `default-queue-concurrency` | 2 | 队列默认上限 |
| `default-tenant-concurrency` | 2 | 租户默认上限 |
| `queue-concurrency.<name>` | — | 按队列覆盖上限 |
| `tenant-concurrency.<name>` | — | 按租户覆盖上限 |
| `tick-millis` | 20 | 调度循环周期 |
| `store-dir` | `data/scheduler` | JSON 持久化目录 |
| `default-max-attempts` | 1 | 作业未指定时的默认最大尝试次数 |
| `default-backoff-millis` | 0 | 默认重试退避毫秒 |

## 10. 验证方法

```bash
mvn test
```

测试清单（日志中会打印作业标识、状态迁移与判定依据）：

| 测试类 | 覆盖场景 |
|--------|----------|
| `DagValidatorTest` | 缺失上游 / 环 / 批次重复 id，错误码可区分 |
| `DagFlowTest` | 上游全成功才执行；失败链、取消链传播为 UNREACHABLE |
| `QuotaTest` | 全局 / 队列 / 租户超限时 QUEUED 排队，释放后推进完成 |
| `IdempotencyTest` | 12 线程并发同键提交只执行一次；同键不同内容冲突 |
| `RetryTest` | 重试后成功、重试耗尽失败并传播下游 |
| `RecoveryTest` | 重启后 SUCCESS 不重跑、RUNNING/QUEUED 恢复继续、FAILED 稳定 |
| `ConcurrencyRaceTest` | 30 作业并发提交不超配额不丢失；8 线程并发取消收敛到单一终态 |
| `JobApiTest` | HTTP 端到端提交 / 查询 / 取消与全部准入错误码 |

手工验证示例：

```bash
curl -s -XPOST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"jobs":[{"id":"a","taskType":"sleep","taskParam":300},{"id":"b","dependencies":["a"]}]}'
curl -s localhost:8080/api/jobs/b
curl -s -XPOST localhost:8080/api/jobs/a/cancel
```
