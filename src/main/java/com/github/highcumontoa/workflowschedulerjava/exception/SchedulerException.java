package com.github.highcumontoa.workflowschedulerjava.exception;

/** 可解释的业务异常（骨架占位）。 */
public class SchedulerException extends RuntimeException {
    private final ErrorCode code;

    public SchedulerException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public ErrorCode getErrorCode() {
        return code;
    }
}
