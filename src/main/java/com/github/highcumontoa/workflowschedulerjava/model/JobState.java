package com.github.highcumontoa.workflowschedulerjava.model;

/**
 * 作业生命周期状态。
 */
public enum JobState {
    PENDING,
    QUEUED,
    RUNNING,
    SUCCESS,
    FAILED,
    CANCELLED,
    UNREACHABLE
}
