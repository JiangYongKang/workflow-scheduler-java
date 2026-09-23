package com.github.highcumontoa.workflowschedulerjava.tests;

import com.github.highcumontoa.workflowschedulerjava.config.SchedulerProperties;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitBatchRequest;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobRequest;
import com.github.highcumontoa.workflowschedulerjava.engine.SchedulerEngine;
import com.github.highcumontoa.workflowschedulerjava.model.JobState;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.highcumontoa.workflowschedulerjava.tests.EngineTestSupport.awaitState;
import static com.github.highcumontoa.workflowschedulerjava.tests.EngineTestSupport.newEngine;
import static com.github.highcumontoa.workflowschedulerjava.tests.EngineTestSupport.printStates;
import static com.github.highcumontoa.workflowschedulerjava.tests.EngineTestSupport.props;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 并发配额：超限进入 QUEUED 排队，不失败不丢弃；槽位释放后按序推进完成。 */
class QuotaTest {

    private static final Logger log = LoggerFactory.getLogger(QuotaTest.class);

    private SubmitJobRequest sleep(String id, String queue, String tenant, long ms) {
        SubmitJobRequest r = new SubmitJobRequest();
        r.setId(id);
        r.setTaskType("sleep");
        r.setTaskParam(ms);
        r.setQueue(queue);
        r.setTenant(tenant);
        return r;
    }

    private long countState(SchedulerEngine engine, JobState state) {
        return engine.snapshot().values().stream().filter(v -> v.getState().equals(state.name())).count();
    }

    @Test
    void globalConcurrencyLimitQueuesExcessJobs(@TempDir Path dir) {
        SchedulerProperties p = props(dir);
        p.setGlobalConcurrency(2);
        p.setDefaultQueueConcurrency(10);
        p.setDefaultTenantConcurrency(10);
        SchedulerEngine engine = newEngine(dir, p);
        engine.start();
        try {
            SubmitBatchRequest batch = new SubmitBatchRequest();
            batch.setJobs(List.of(
                    sleep("g1", "default", "default", 500),
                    sleep("g2", "default", "default", 500),
                    sleep("g3", "default", "default", 500),
                    sleep("g4", "default", "default", 500)));
            engine.submit(batch);

            // 等待调度一拍后：恰好 2 个 RUNNING，其余 QUEUED
            awaitState(engine, "g2", JobState.RUNNING, Duration.ofSeconds(3));
            long running = countState(engine, JobState.RUNNING);
            long queued = countState(engine, JobState.QUEUED);
            log.info("[配额判定] 全局上限=2 实际RUNNING={} QUEUED={}", running, queued);
            printStates("全局配额：超额排队", engine);
            assertTrue(running <= 2, "RUNNING 不得超过全局上限");
            assertTrue(running >= 1);
            assertTrue(queued >= 2, "超额作业必须排队而非失败");

            for (String id : List.of("g1", "g2", "g3", "g4")) {
                awaitState(engine, id, JobState.SUCCESS, Duration.ofSeconds(10));
            }
            printStates("全部完成（槽位释放后继续推进）", engine);
        } finally {
            engine.stop();
        }
    }

    @Test
    void queueAndTenantLimitsAreEnforcedIndependently(@TempDir Path dir) {
        SchedulerProperties p = props(dir);
        p.setGlobalConcurrency(10);
        p.setDefaultQueueConcurrency(10);
        p.setDefaultTenantConcurrency(10);
        p.getQueueConcurrency().put("q1", 1);
        p.getTenantConcurrency().put("t1", 1);
        SchedulerEngine engine = newEngine(dir, p);
        engine.start();
        try {
            SubmitBatchRequest batch = new SubmitBatchRequest();
            batch.setJobs(List.of(
                    sleep("a", "q1", "tA", 500),
                    sleep("b", "q1", "tB", 500),   // 受队列 q1=1 限制
                    sleep("c", "q2", "t1", 500),
                    sleep("d", "q2", "t1", 500))); // 受租户 t1=1 限制
            engine.submit(batch);

            Thread.sleep(150);
            printStates("队列/租户配额判定", engine);
            long queued = countState(engine, JobState.QUEUED);
            long running = countState(engine, JobState.RUNNING);
            log.info("[配额判定] RUNNING={} QUEUED={}（q1=1, t1=1）", running, queued);
            assertTrue(running >= 2, "无冲突维度的作业应能并行启动");
            assertTrue(queued >= 2, "撞队列上限与租户上限的作业应排队");

            for (String id : List.of("a", "b", "c", "d")) {
                awaitState(engine, id, JobState.SUCCESS, Duration.ofSeconds(10));
            }
            printStates("队列/租户作业全部完成", engine);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            engine.stop();
        }
    }
}
