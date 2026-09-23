package com.github.highcumontoa.workflowschedulerjava.handler;

import com.github.highcumontoa.workflowschedulerjava.error.ErrorCode;
import com.github.highcumontoa.workflowschedulerjava.error.SchedulerException;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** 按 jobType 查找处理器；未知类型在提交期即被拒绝。 */
@Component
public class HandlerRegistry {

    private final Map<String, TaskHandler> handlers = new HashMap<>();

    public HandlerRegistry(List<TaskHandler> handlerList) {
        for (TaskHandler h : handlerList) {
            handlers.put(h.type(), h);
        }
    }

    public TaskHandler require(String jobType) {
        TaskHandler h = handlers.get(jobType);
        if (h == null) {
            throw new SchedulerException(ErrorCode.UNKNOWN_JOB_TYPE, "未知任务类型: " + jobType);
        }
        return h;
    }

    public boolean exists(String jobType) {
        return handlers.containsKey(jobType);
    }
}
