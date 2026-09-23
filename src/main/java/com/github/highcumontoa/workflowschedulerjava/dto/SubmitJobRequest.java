package com.github.highcumontoa.workflowschedulerjava.dto;

import com.github.highcumontoa.workflowschedulerjava.model.RetryPolicy;
import java.util.List;

/**
 * 单个作业的提交请求。所有字段均可缺省（id 缺省时服务端生成）。
 */
public class SubmitJobRequest {

    private String id;
    private String name;
    private String idempotencyKey;
    private String queue;
    private String tenant;
    private List<String> dependencies;
    private String taskType;
    private Long taskParam;
    private RetryPolicy retryPolicy;

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
    public void setDependencies(List<String> dependencies) { this.dependencies = dependencies; }

    public String getTaskType() { return taskType; }
    public void setTaskType(String taskType) { this.taskType = taskType; }

    public Long getTaskParam() { return taskParam; }
    public void setTaskParam(Long taskParam) { this.taskParam = taskParam; }

    public RetryPolicy getRetryPolicy() { return retryPolicy; }
    public void setRetryPolicy(RetryPolicy retryPolicy) { this.retryPolicy = retryPolicy; }
}
