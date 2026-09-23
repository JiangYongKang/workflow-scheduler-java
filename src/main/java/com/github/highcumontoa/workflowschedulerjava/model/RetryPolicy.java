package com.github.highcumontoa.workflowschedulerjava.model;

/**
 * 失败重试策略。
 *
 * <p>{@code maxAttempts} 为包含首次执行在内的总尝试次数（1 表示不重试）；
 * {@code backoffMillis} 为相邻两次尝试之间的固定退避时间。
 */
public class RetryPolicy {

    /** 包含首次执行在内的最大尝试次数。 */
    private int maxAttempts = 1;

    /** 重试退避毫秒数。 */
    private long backoffMillis = 0L;

    public RetryPolicy() {
    }

    public RetryPolicy(int maxAttempts, long backoffMillis) {
        this.maxAttempts = maxAttempts;
        this.backoffMillis = backoffMillis;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public long getBackoffMillis() {
        return backoffMillis;
    }

    public void setBackoffMillis(long backoffMillis) {
        this.backoffMillis = backoffMillis;
    }
}
