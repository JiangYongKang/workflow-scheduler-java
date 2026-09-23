package com.github.highcumontoa.workflowschedulerjava.handler;

/** 任务执行结果。success=false 时 message 作为失败原因落库。 */
public record HandlerResult(boolean success, String message) {
    public static HandlerResult ok() {
        return new HandlerResult(true, null);
    }

    public static HandlerResult fail(String message) {
        return new HandlerResult(false, message);
    }
}
