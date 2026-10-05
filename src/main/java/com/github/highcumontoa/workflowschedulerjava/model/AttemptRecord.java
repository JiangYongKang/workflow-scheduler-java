package com.github.highcumontoa.workflowschedulerjava.model;

/**
 * 一次执行尝试的持久化记录：每次进入 RUNNING 都会追加一条（以 jobId+attempt 唯一），
 * 执行结束时回填结束时间与结果。记录随 {@code attempts.json} 持久化，进程重启不丢。
 *
 * <p>记录条数恒等于该作业真实进入 RUNNING 的次数：重试、重启后重跑、被取消的执行
 * 都各算一条；幂等命中、排队等待、取消未执行的作业不会产生记录。</p>
 */
public class AttemptRecord {

    /** 所属作业 id。 */
    private String jobId;

    /** 第几次尝试（从 1 开始，与 Job.attempts 对应）。 */
    private int attempt;

    /** 进入 RUNNING 的时间（epoch ms）。 */
    private long startedAt;

    /** 执行结束时间（epoch ms）；0 表示尚未结束。 */
    private long finishedAt;

    /** 本次执行结果。 */
    private AttemptResult result = AttemptResult.STARTED;

    /** 失败/取消/超时原因；成功时为 null。 */
    private String error;

    public AttemptRecord() {
    }

    public AttemptRecord(String jobId, int attempt, long startedAt) {
        this.jobId = jobId;
        this.attempt = attempt;
        this.startedAt = startedAt;
    }

    public String getJobId() { return jobId; }
    public void setJobId(String jobId) { this.jobId = jobId; }

    public int getAttempt() { return attempt; }
    public void setAttempt(int attempt) { this.attempt = attempt; }

    public long getStartedAt() { return startedAt; }
    public void setStartedAt(long startedAt) { this.startedAt = startedAt; }

    public long getFinishedAt() { return finishedAt; }
    public void setFinishedAt(long finishedAt) { this.finishedAt = finishedAt; }

    public AttemptResult getResult() { return result; }
    public void setResult(AttemptResult result) { this.result = result; }

    public String getError() { return error; }
    public void setError(String error) { this.error = error; }
}
