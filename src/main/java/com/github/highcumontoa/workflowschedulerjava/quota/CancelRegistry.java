package com.github.highcumontoa.workflowschedulerjava.quota;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 进程内取消标记：作业在 RUNNING 期间的执行体可轮询该标记尽快中止。
 * 重启后遗留 RUNNING 作业会被重跑（at-least-once），不依赖此内存结构。
 */
@Component
public class CancelRegistry {

    private final ConcurrentHashMap<String, Boolean> cancelled = new ConcurrentHashMap<>();

    public void mark(String jobId) {
        cancelled.put(jobId, Boolean.TRUE);
    }

    public boolean isCancelled(String jobId) {
        return cancelled.getOrDefault(jobId, Boolean.FALSE);
    }

    public void clear(String jobId) {
        cancelled.remove(jobId);
    }
}
