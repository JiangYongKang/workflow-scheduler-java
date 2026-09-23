package com.github.highcumontoa.workflowschedulerjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * 按队列/租户名单独配置并发上限：
 * scheduler.quota.queue.<队列名>=N
 * scheduler.quota.tenant.<租户名>=N
 * 未列出的名称使用默认值。
 */
@ConfigurationProperties(prefix = "scheduler.quota")
public class QuotaLimitProperties {

    /** key: 队列名, value: 并发上限 */
    private Map<String, Integer> queue = new HashMap<>();

    /** key: 租户名, value: 并发上限 */
    private Map<String, Integer> tenant = new HashMap<>();

    public Map<String, Integer> getQueue() {
        return queue;
    }

    public void setQueue(Map<String, Integer> queue) {
        this.queue = queue;
    }

    public Map<String, Integer> getTenant() {
        return tenant;
    }

    public void setTenant(Map<String, Integer> tenant) {
        this.tenant = tenant;
    }
}
