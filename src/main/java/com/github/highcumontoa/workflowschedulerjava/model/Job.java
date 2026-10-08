package com.github.highcumontoa.workflowschedulerjava.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.ArrayList;
import java.util.List;

/**
 * 作业实体：调度状态、依赖、配额维度、重试策略与可解释的判定信息均持久化于此。
 *
 * <p>字段全部可变并提供无参构造，以便 Jackson 做 JSON 序列化；所有状态迁移由
 * {@code SchedulerEngine} 在锁内统一完成。</p>
 */
public class Job {

    /** 作业唯一标识（由客户端指定或服务端生成）。 */
    private String id;

    /** 人类可读名称，缺省取 id。 */
    private String name;

    /** 幂等键：相同键的重复提交命中同一作业，不重复执行、不重复计数。 */
    private String idempotencyKey;

    /** 队列维度（并发配额维度之一），缺省 default。 */
    private String queue = "default";

    /** 租户维度（并发配额维度之一），缺省 default。 */
    private String tenant = "default";

    /** 上游作业 id 列表：全部 SUCCESS 后本作业才可执行。 */
    private List<String> dependencies = new ArrayList<>();

    /** 载荷类型：noop / sleep / flaky。 */
    private String taskType = "noop";

    /** 载荷参数（如 sleep 的毫秒数、flaky 的失败次数）。 */
    private long taskParam = 0L;

    /** 失败重试策略。 */
    private RetryPolicy retryPolicy = new RetryPolicy();

    /** 单次执行的最长时长（毫秒）；null 或 <=0 表示不限制（保持原有行为）。仅在 RUNNING 期间计时。 */
    private Long timeoutMillis;

    /**
     * 按尝试序号覆盖的最长执行时长（毫秒），key 为尝试序号（从 1 开始）。仅用于测试精确编排
     * 各次尝试的超时预算；不持久化、不经过 HTTP 提交。生产路径恒为 null，行为与 timeoutMillis 一致。
     */
    @JsonIgnore
    private transient java.util.Map<Integer, Long> attemptTimeoutOverrides;

    /** 当前状态。 */
    private JobState state = JobState.PENDING;

    /** 已开始的尝试次数（首次执行前为 0，首次执行后置为 1）。 */
    private int attempts = 0;

    /** 最近一次失败原因。 */
    private String failureReason;

    /** 变为 UNREACHABLE 时记录的阻塞上游作业 id。 */
    private String blockedBy;

    /** 状态/判定的人类可读说明（便于排查与测试断言）。 */
    private String detail;

    /** 提交时间（epoch ms）。 */
    private long submittedAt;

    /** 进入 RUNNING 的时间（最近一次尝试）。 */
    private long startedAt;

    /** 进入终态的时间。 */
    private long finishedAt;

    public Job() {
    }

    /** RUNNING 中收到取消请求的瞬时标记（不持久化，重启后 RUNNING 作业按失败恢复）。 */
    @JsonIgnore
    private transient boolean cancelRequested;

    @JsonIgnore
    public boolean isCancelRequested() { return cancelRequested; }

    @JsonIgnore
    public void setCancelRequested(boolean cancelRequested) { this.cancelRequested = cancelRequested; }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }

    public String getQueue() { return queue; }
    public void setQueue(String queue) { this.queue = queue; }

    public String getTenant() { return tenant; }
    public void setTenant(String tenant) { this.tenant = tenant; }

    public List<String> getDependencies() { return dependencies; }
    public void setDependencies(List<String> dependencies) {
        this.dependencies = (dependencies == null) ? new ArrayList<>() : dependencies;
    }

    public String getTaskType() { return taskType; }
    public void setTaskType(String taskType) { this.taskType = taskType; }

    public long getTaskParam() { return taskParam; }
    public void setTaskParam(long taskParam) { this.taskParam = taskParam; }

    public RetryPolicy getRetryPolicy() { return retryPolicy; }
    public void setRetryPolicy(RetryPolicy retryPolicy) { this.retryPolicy = retryPolicy; }

    public Long getTimeoutMillis() { return timeoutMillis; }
    public void setTimeoutMillis(Long timeoutMillis) { this.timeoutMillis = timeoutMillis; }

    /** 测试用：按尝试序号读取超时预算覆盖；无覆盖返回 null。 */
    @JsonIgnore
    public Long getTimeoutForAttempt(int attempt) {
        return attemptTimeoutOverrides == null ? null : attemptTimeoutOverrides.get(attempt);
    }

    /** 测试用：设置按尝试序号的超时预算覆盖。 */
    @JsonIgnore
    public void setAttemptTimeoutOverride(int attempt, long millis) {
        if (attemptTimeoutOverrides == null) {
            attemptTimeoutOverrides = new java.util.HashMap<>();
        }
        attemptTimeoutOverrides.put(attempt, millis);
    }

    public JobState getState() { return state; }
    public void setState(JobState state) { this.state = state; }

    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }

    public String getFailureReason() { return failureReason; }
    public void setFailureReason(String failureReason) { this.failureReason = failureReason; }

    public String getBlockedBy() { return blockedBy; }
    public void setBlockedBy(String blockedBy) { this.blockedBy = blockedBy; }

    public String getDetail() { return detail; }
    public void setDetail(String detail) { this.detail = detail; }

    public long getSubmittedAt() { return submittedAt; }
    public void setSubmittedAt(long submittedAt) { this.submittedAt = submittedAt; }

    public long getStartedAt() { return startedAt; }
    public void setStartedAt(long startedAt) { this.startedAt = startedAt; }

    public long getFinishedAt() { return finishedAt; }
    public void setFinishedAt(long finishedAt) { this.finishedAt = finishedAt; }
}
