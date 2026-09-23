package com.github.highcumontoa.workflowschedulerjava.error;

/** 业务异常，携带稳定的错误码用于区分原因。 */
public class SchedulerException extends RuntimeException {
    private final ErrorCode code;

    public SchedulerException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public ErrorCode getCode() {
        return code;
    }
}
