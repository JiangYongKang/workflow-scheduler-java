package com.github.highcumontoa.workflowschedulerjava.config;

import java.util.HashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 调度器配置项（前缀 {@code scheduler}）。
 *
 * <ul>
 *   <li>{@code scheduler.global-concurrency}：全局并发上限</li>
 *   <li>{@code scheduler.default-queue-concurrency}：未单独配置的队列默认上限</li>
 *   <li>{@code scheduler.default-tenant-concurrency}：未单独配置的租户默认上限</li>
 *   <li>{@code scheduler.queue-concurrency[q1]}：按队列覆盖上限</li>
 *   <li>{@code scheduler.tenant-concurrency[t1]}：按租户覆盖上限</li>
 *   <li>{@code scheduler.tick-millis}：调度循环周期</li>
 *   <li>{@code scheduler.store-dir}：JSON 持久化目录</li>
 *   <li>{@code scheduler.default-max-attempts} / {@code scheduler.default-backoff-millis}：默认重试策略</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "scheduler")
public class SchedulerProperties {

    private int globalConcurrency = 4;
    private int defaultQueueConcurrency = 2;
    private int defaultTenantConcurrency = 2;
    private Map<String, Integer> queueConcurrency = new HashMap<>();
    private Map<String, Integer> tenantConcurrency = new HashMap<>();
    private long tickMillis = 50L;
    private String storeDir = "data/scheduler";
    private int defaultMaxAttempts = 1;
    private long defaultBackoffMillis = 0L;

    public int getGlobalConcurrency() { return globalConcurrency; }
    public void setGlobalConcurrency(int globalConcurrency) { this.globalConcurrency = globalConcurrency; }

    public int getDefaultQueueConcurrency() { return defaultQueueConcurrency; }
    public void setDefaultQueueConcurrency(int v) { this.defaultQueueConcurrency = v; }

    public int getDefaultTenantConcurrency() { return defaultTenantConcurrency; }
    public void setDefaultTenantConcurrency(int v) { this.defaultTenantConcurrency = v; }

    public Map<String, Integer> getQueueConcurrency() { return queueConcurrency; }
    public void setQueueConcurrency(Map<String, Integer> queueConcurrency) { this.queueConcurrency = queueConcurrency; }

    public Map<String, Integer> getTenantConcurrency() { return tenantConcurrency; }
    public void setTenantConcurrency(Map<String, Integer> tenantConcurrency) { this.tenantConcurrency = tenantConcurrency; }

    public long getTickMillis() { return tickMillis; }
    public void setTickMillis(long tickMillis) { this.tickMillis = tickMillis; }

    public String getStoreDir() { return storeDir; }
    public void setStoreDir(String storeDir) { this.storeDir = storeDir; }

    public int getDefaultMaxAttempts() { return defaultMaxAttempts; }
    public void setDefaultMaxAttempts(int defaultMaxAttempts) { this.defaultMaxAttempts = defaultMaxAttempts; }

    public long getDefaultBackoffMillis() { return defaultBackoffMillis; }
    public void setDefaultBackoffMillis(long defaultBackoffMillis) { this.defaultBackoffMillis = defaultBackoffMillis; }
}
