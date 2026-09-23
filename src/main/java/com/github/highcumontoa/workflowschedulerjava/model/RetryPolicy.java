package com.github.highcumontoa.workflowschedulerjava.model;

/** 失败重试策略：最多执行 maxAttempts 次，第 n 次失败后退避 base * multiplier^(n-1) 毫秒。 */
public record RetryPolicy(int maxAttempts, long backoffBaseMs, double backoffMultiplier) {
}
