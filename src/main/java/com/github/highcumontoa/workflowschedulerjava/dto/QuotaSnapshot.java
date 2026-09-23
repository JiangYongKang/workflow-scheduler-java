package com.github.highcumontoa.workflowschedulerjava.dto;

/** 当前配额与占用快照，便于观察与排障。 */
public record QuotaSnapshot(
        int globalRunning,
        int globalLimit,
        java.util.Map<String, Integer> queueRunning,
        java.util.Map<String, Integer> queueLimits,
        java.util.Map<String, Integer> tenantRunning,
        java.util.Map<String, Integer> tenantLimits
) {
}
