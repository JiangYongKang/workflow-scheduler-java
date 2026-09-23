package com.github.highcumontoa.workflowschedulerjava.engine;

import com.github.highcumontoa.workflowschedulerjava.model.Job;

/** 作业载荷执行器（骨架占位）。 */
public interface TaskHandler {
    void execute(Job job) throws Exception;
}
