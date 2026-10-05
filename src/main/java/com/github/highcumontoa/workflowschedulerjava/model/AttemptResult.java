package com.github.highcumontoa.workflowschedulerjava.model;

/**
 * 单次执行尝试的结果。
 */
public enum AttemptResult {
    /** 本次尝试成功。 */
    SUCCESS,
    /** 本次尝试失败（含超时按失败处理）。 */
    FAILED,
    /** 本次尝试被取消（含执行中收到取消请求、进程重启中断）。 */
    CANCELLED
}
