package com.github.highcumontoa.workflowschedulerjava.dto;

/**
 * 单个作业的提交结果。
 *
 * @param duplicate 为 true 表示命中幂等键、返回的是既有作业（未重复执行/计数）。
 */
public class SubmitJobResponse {

    private String jobId;
    private String state;
    private boolean duplicate;
    private String detail;

    public SubmitJobResponse() {
    }

    public SubmitJobResponse(String jobId, String state, boolean duplicate, String detail) {
        this.jobId = jobId;
        this.state = state;
        this.duplicate = duplicate;
        this.detail = detail;
    }

    public String getJobId() { return jobId; }
    public void setJobId(String jobId) { this.jobId = jobId; }

    public String getState() { return state; }
    public void setState(String state) { this.state = state; }

    public boolean isDuplicate() { return duplicate; }
    public void setDuplicate(boolean duplicate) { this.duplicate = duplicate; }

    public String getDetail() { return detail; }
    public void setDetail(String detail) { this.detail = detail; }
}
