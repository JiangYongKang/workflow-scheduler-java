package com.github.highcumontoa.workflowschedulerjava.model;

import java.util.ArrayList;
import java.util.List;

/** 作业持久化实体（dependencies 为非持久化的便捷字段，由 repo 单独装载）。 */
public class Job {
    private String id;
    private String idempotencyKey;
    private String jobType;
    private String payload;
    private String queueName;
    private String tenant;
    private JobStatus status;
    private int attempts;
    private int maxAttempts;
    private long backoffBaseMs;
    private double backoffMultiplier;
    private long runAfter;
    private String failureReason;
    private long createdAt;
    private long updatedAt;
    private List<String> dependencies = new ArrayList<>();

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }
    public String getJobType() { return jobType; }
    public void setJobType(String jobType) { this.jobType = jobType; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public String getQueueName() { return queueName; }
    public void setQueueName(String queueName) { this.queueName = queueName; }
    public String getTenant() { return tenant; }
    public void setTenant(String tenant) { this.tenant = tenant; }
    public JobStatus getStatus() { return status; }
    public void setStatus(JobStatus status) { this.status = status; }
    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public long getBackoffBaseMs() { return backoffBaseMs; }
    public void setBackoffBaseMs(long backoffBaseMs) { this.backoffBaseMs = backoffBaseMs; }
    public double getBackoffMultiplier() { return backoffMultiplier; }
    public void setBackoffMultiplier(double backoffMultiplier) { this.backoffMultiplier = backoffMultiplier; }
    public long getRunAfter() { return runAfter; }
    public void setRunAfter(long runAfter) { this.runAfter = runAfter; }
    public String getFailureReason() { return failureReason; }
    public void setFailureReason(String failureReason) { this.failureReason = failureReason; }
    public long getCreatedAt() { return createdAt; }
    public void setCreatedAt(long createdAt) { this.createdAt = createdAt; }
    public long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(long updatedAt) { this.updatedAt = updatedAt; }
    public List<String> getDependencies() { return dependencies; }
    public void setDependencies(List<String> dependencies) { this.dependencies = dependencies; }
}
