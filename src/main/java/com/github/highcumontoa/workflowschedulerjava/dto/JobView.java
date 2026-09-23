package com.github.highcumontoa.workflowschedulerjava.dto;

import com.github.highcumontoa.workflowschedulerjava.model.Job;
import java.util.List;

/** 作业对外只读视图。 */
public class JobView {

    private String id;
    private String name;
    private String state;
    private String queue;
    private String tenant;
    private List<String> dependencies;
    private String taskType;
    private int attempts;
    private int maxAttempts;
    private String failureReason;
    private String blockedBy;
    private String detail;
    private long submittedAt;
    private long startedAt;
    private long finishedAt;

    public static JobView of(Job job) {
        JobView v = new JobView();
        v.id = job.getId();
        v.name = job.getName();
        v.state = job.getState().name();
        v.queue = job.getQueue();
        v.tenant = job.getTenant();
        v.dependencies = List.copyOf(job.getDependencies());
        v.taskType = job.getTaskType();
        v.attempts = job.getAttempts();
        v.maxAttempts = job.getRetryPolicy().getMaxAttempts();
        v.failureReason = job.getFailureReason();
        v.blockedBy = job.getBlockedBy();
        v.detail = job.getDetail();
        v.submittedAt = job.getSubmittedAt();
        v.startedAt = job.getStartedAt();
        v.finishedAt = job.getFinishedAt();
        return v;
    }

    public String getId() { return id; }
    public String getName() { return name; }
    public String getState() { return state; }
    public String getQueue() { return queue; }
    public String getTenant() { return tenant; }
    public List<String> getDependencies() { return dependencies; }
    public String getTaskType() { return taskType; }
    public int getAttempts() { return attempts; }
    public int getMaxAttempts() { return maxAttempts; }
    public String getFailureReason() { return failureReason; }
    public String getBlockedBy() { return blockedBy; }
    public String getDetail() { return detail; }
    public long getSubmittedAt() { return submittedAt; }
    public long getStartedAt() { return startedAt; }
    public long getFinishedAt() { return finishedAt; }
}
