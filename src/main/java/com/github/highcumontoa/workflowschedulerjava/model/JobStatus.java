package com.github.highcumontoa.workflowschedulerjava.model;

/**
 * 作业状态机：
 * PENDING（有依赖、等待上游）→ QUEUED（可调度、等待配额/退避）→ RUNNING → SUCCESS / FAILED
 * 任意非终态 → CANCELLED；上游 FAILED/CANCELLED/UNREACHABLE 时下游 → UNREACHABLE。
 */
public enum JobStatus {
    PENDING,
    QUEUED,
    RUNNING,
    SUCCESS,
    FAILED,
    CANCELLED,
    UNREACHABLE;

    /** 终态：状态不再发生迁移 */
    public boolean isTerminal() {
        return this == SUCCESS || this == FAILED || this == CANCELLED || this == UNREACHABLE;
    }
}
