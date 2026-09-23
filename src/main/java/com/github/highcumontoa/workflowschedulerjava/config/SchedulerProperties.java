package com.github.highcumontoa.workflowschedulerjava.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** 调度器配置项（application.properties 中 scheduler.* 前缀）。 */
@Component
public class SchedulerProperties {

    private final long tickIntervalMs;
    private final int globalLimit;
    private final int defaultQueueLimit;
    private final int defaultTenantLimit;
    private final int workerThreads;
    private final int defaultMaxAttempts;
    private final long defaultBackoffBaseMs;
    private final double defaultBackoffMultiplier;

    public SchedulerProperties(
            @Value("${scheduler.tick-interval-ms:200}") long tickIntervalMs,
            @Value("${scheduler.quota.global:4}") int globalLimit,
            @Value("${scheduler.quota.default-queue:2}") int defaultQueueLimit,
            @Value("${scheduler.quota.default-tenant:3}") int defaultTenantLimit,
            @Value("${scheduler.worker-threads:8}") int workerThreads,
            @Value("${scheduler.retry.max-attempts:3}") int defaultMaxAttempts,
            @Value("${scheduler.retry.backoff-base-ms:500}") long defaultBackoffBaseMs,
            @Value("${scheduler.retry.backoff-multiplier:2.0}") double defaultBackoffMultiplier) {
        this.tickIntervalMs = tickIntervalMs;
        this.globalLimit = globalLimit;
        this.defaultQueueLimit = defaultQueueLimit;
        this.defaultTenantLimit = defaultTenantLimit;
        this.workerThreads = workerThreads;
        this.defaultMaxAttempts = defaultMaxAttempts;
        this.defaultBackoffBaseMs = defaultBackoffBaseMs;
        this.defaultBackoffMultiplier = defaultBackoffMultiplier;
    }

    public long getTickIntervalMs() { return tickIntervalMs; }
    public int getGlobalLimit() { return globalLimit; }
    public int getDefaultQueueLimit() { return defaultQueueLimit; }
    public int getDefaultTenantLimit() { return defaultTenantLimit; }
    public int getWorkerThreads() { return workerThreads; }
    public int getDefaultMaxAttempts() { return defaultMaxAttempts; }
    public long getDefaultBackoffBaseMs() { return defaultBackoffBaseMs; }
    public double getDefaultBackoffMultiplier() { return defaultBackoffMultiplier; }
}
