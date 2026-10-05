package com.github.highcumontoa.workflowschedulerjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.workflowschedulerjava.config.SchedulerProperties;
import com.github.highcumontoa.workflowschedulerjava.dto.JobView;
import com.github.highcumontoa.workflowschedulerjava.engine.InProcessTaskHandler;
import com.github.highcumontoa.workflowschedulerjava.engine.SchedulerEngine;
import com.github.highcumontoa.workflowschedulerjava.model.JobState;
import com.github.highcumontoa.workflowschedulerjava.store.JsonFileJobStore;
import com.github.highcumontoa.workflowschedulerjava.store.JobStore;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 构造隔离的调度引擎实例（独立临时目录），并提供状态等待与快照打印。 */
final class EngineTestSupport {

    private static final Logger log = LoggerFactory.getLogger(EngineTestSupport.class);

    private EngineTestSupport() {
    }

    static SchedulerProperties props(Path dir) {
        SchedulerProperties p = new SchedulerProperties();
        p.setStoreDir(dir.toString());
        p.setTickMillis(10);
        p.setGlobalConcurrency(4);
        p.setDefaultQueueConcurrency(2);
        p.setDefaultTenantConcurrency(2);
        p.setDefaultMaxAttempts(1);
        return p;
    }

    static SchedulerEngine newEngine(Path dir, SchedulerProperties p) {
        JobStore store = new JsonFileJobStore(dir, new ObjectMapper());
        return new SchedulerEngine(store, new InProcessTaskHandler(), p);
    }

    /** 递归删除目录（存在才删），用于 Spring 测试在上下文启动前清理历史持久化数据。 */
    static void deleteRecursively(Path dir) {
        if (!java.nio.file.Files.exists(dir)) {
            return;
        }
        try (var walk = java.nio.file.Files.walk(dir)) {
            walk.sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            java.nio.file.Files.delete(p);
                        } catch (java.io.IOException e) {
                            throw new RuntimeException("清理测试目录失败: " + p, e);
                        }
                    });
        } catch (java.io.IOException e) {
            throw new RuntimeException("清理测试目录失败: " + dir, e);
        }
    }

    static JobView awaitState(SchedulerEngine engine, String id, JobState expected, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        JobView view = null;
        while (System.currentTimeMillis() < deadline) {
            view = engine.get(id);
            if (view.getState().equals(expected.name())) {
                return view;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        throw new AssertionError("job=" + id + " 未在 " + timeout + " 内到达 " + expected
                + "，实际=" + (view == null ? "null" : view.getState())
                + " detail=" + (view == null ? null : view.getDetail()));
    }

    static void printStates(String title, SchedulerEngine engine) {
        StringBuilder sb = new StringBuilder("\n-------- ").append(title).append(" --------");
        for (Map.Entry<String, JobView> e : engine.snapshot().entrySet()) {
            JobView v = e.getValue();
            sb.append("\njob=").append(v.getId())
                    .append(" state=").append(v.getState())
                    .append(" attempts=").append(v.getAttempts()).append('/').append(v.getMaxAttempts())
                    .append(" queue=").append(v.getQueue())
                    .append(" tenant=").append(v.getTenant())
                    .append(" blockedBy=").append(v.getBlockedBy())
                    .append(" failure=").append(v.getFailureReason())
                    .append("\n        判定依据: ").append(v.getDetail());
        }
        log.info(sb.toString());
    }
}
