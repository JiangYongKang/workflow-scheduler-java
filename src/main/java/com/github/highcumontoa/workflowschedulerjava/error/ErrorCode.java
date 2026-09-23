package com.github.highcumontoa.workflowschedulerjava.error;

/** 可区分的失败原因，稳定映射到 HTTP 状态码。 */
public enum ErrorCode {
    UNKNOWN_DEPENDENCY,
    CYCLE_DETECTED,
    UNKNOWN_JOB_TYPE,
    INVALID_REQUEST,
    JOB_NOT_FOUND,
    ALREADY_TERMINAL,
    INTERNAL_ERROR
}
