package com.github.highcumontoa.workflowschedulerjava.handler;

/** 任务处理器接口，按 jobType 注册。 */
public interface TaskHandler {
    String type();

    HandlerResult handle(TaskContext context) throws Exception;
}
