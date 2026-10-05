package com.github.highcumontoa.workflowschedulerjava.model;

/**
 * 一次执行尝试的留痕记录：开始/结束时间、结果与失败原因。
 *
 * <p>记录由调度线程在执行结果落账时追加并持久化，只增不改；同一作业的多次尝试
 * （重试、重启后重跑、被取消）各产生一条，按 {@link #attempt} 升序可查。</p>
 */
public class AttemptRecord {

    /** 所属作业 id。 */
    private String jobId;

    /** 第几次尝试（与 Job.attempts 对应，从 1 开始）。 */
    private int attempt;

    /** 本次尝试进入 RUNNING 的时间（epoch ms）。 */
    private long startedAt;

    /** 本次尝试落账结束的时间（epoch ms）。 */
    private long finishedAt;

    /** 本次尝试结果。 */
    private AttemptResult result;

    /** 失败/取消原因；成功时为 null。 */
    private String error;

    public AttemptRecord() {
    }

    public AttemptRecord(String jobId, int attempt, long startedAt, long finishedAt,
                         AttemptResult result, String error) {
        this.jobId = jobId;
        this.attempt = attempt;
        this.startedAt = startedAt;
        this.finishedAt = finishedAt;
        this.result = result;
        this.error = error;
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
