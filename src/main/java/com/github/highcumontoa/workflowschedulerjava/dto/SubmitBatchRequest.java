package com.github.highcumontoa.workflowschedulerjava.dto;

import java.util.List;

/** 批量提交请求（骨架占位）。 */
public class SubmitBatchRequest {
    private List<SubmitJobRequest> jobs;
    public List<SubmitJobRequest> getJobs() { return jobs; }
    public void setJobs(List<SubmitJobRequest> jobs) { this.jobs = jobs; }
}
