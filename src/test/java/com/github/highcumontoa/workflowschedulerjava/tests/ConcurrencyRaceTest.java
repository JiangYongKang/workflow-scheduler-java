package com.github.highcumontoa.workflowschedulerjava.tests;

import com.github.highcumontoa.workflowschedulerjava.dto.SubmitBatchRequest;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobRequest;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobResponse;
import com.github.highcumontoa.workflowschedulerjava.engine.SchedulerEngine;
import com.github.highcumontoa.workflowschedulerjava.model.JobState;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.highcumontoa.workflowschedulerjava.tests.EngineTestSupport.awaitState;
import static com.github.highcumontoa.workflowschedulerjava.tests.EngineTestSupport.newEngine;
import static com.github.highcumontoa.workflowschedulerjava.tests.EngineTestSupport.props;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 并发竞争：并发提交不同作业 + 并发取消，RUNNING 总数永不超配额，终态集合稳定可解释。 */
class ConcurrencyRaceTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyRaceTest.class);

    @Test
    void concurrentSubmitsNeverExceedGlobalConcurrency(@TempDir Path dir) throws Exception {
        var p = props(dir);
        p.setGlobalConcurrency(3);
        p.setDefaultQueueConcurrency(10);
        p.setDefaultTenantConcurrency(10);
        SchedulerEngine engine = newEngine(dir, p);
        engine.start();
        try {
            int n = 30;
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(10);
            AtomicInteger ok = new AtomicInteger();
            for (int i = 0; i < n; i++) {
                int idx = i;
                pool.submit(() -> {
                    try {
                        start.await();
                        SubmitJobRequest r = new SubmitJobRequest();
                        r.setId("race-" + idx);
                        r.setTaskType("sleep");
                        r.setTaskParam(60L);
                        SubmitBatchRequest req = new SubmitBatchRequest();
                        req.setJobs(List.of(r));
                        List<SubmitJobResponse> resp = engine.submit(req);
                        if (!resp.get(0).isDuplicate()) {
                            ok.incrementAndGet();
                        }
                    } catch (Exception e) {
                        log.warn("提交异常 idx={} : {}", idx, e.toString());
                    }
                });
            }
            start.countDown();
            // 提交过程中轮询：RUNNING 不得超过全局上限
            long deadline = System.currentTimeMillis() + 3000;
            int maxObserved = 0;
            while (System.currentTimeMillis() < deadline) {
                long running = engine.snapshot().values().stream()
                        .filter(v -> v.getState().equals(JobState.RUNNING.name())).count();
                maxObserved = (int) Math.max(maxObserved, running);
                assertTrue(running <= 3, "竞争期间 RUNNING=" + running + " 超过全局配额 3");
                Thread.sleep(5);
            }
            pool.shutdown();
            pool.awaitTermination(5, TimeUnit.SECONDS);
            log.info("[并发判定] 提交 {} 个作业，观察到最大 RUNNING={}（上限3），接受={}", n, maxObserved, ok.get());
            assertEquals(n, ok.get());

            for (int i = 0; i < n; i++) {
                awaitState(engine, "race-" + i, JobState.SUCCESS, Duration.ofSeconds(20));
            }
            long success = engine.snapshot().values().stream()
                    .filter(v -> v.getState().equals(JobState.SUCCESS.name())).count();
            log.info("[并发判定] 最终 SUCCESS 数量={}（应等于提交数 {}，无丢失无重复）", success, n);
            assertEquals(n, success);
        } finally {
            engine.stop();
        }
    }

    @Test
    void concurrentCancelsConvergeToSingleTerminalState(@TempDir Path dir) throws Exception {
        SchedulerEngine engine = newEngine(dir, props(dir));
        engine.start();
        try {
            SubmitJobRequest r = new SubmitJobRequest();
            r.setId("cancel-race");
            r.setTaskType("sleep");
            r.setTaskParam(1500L);
            SubmitBatchRequest req = new SubmitBatchRequest();
            req.setJobs(List.of(r));
            engine.submit(req);
            awaitState(engine, "cancel-race", JobState.RUNNING, Duration.ofSeconds(5));

            int threads = 8;
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            Set<String> results = ConcurrentHashMap.newKeySet();
            AtomicInteger errors = new AtomicInteger();
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        results.add(engine.cancel("cancel-race").getState());
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            pool.awaitTermination(5, TimeUnit.SECONDS);

            var terminal = awaitState(engine, "cancel-race", JobState.CANCELLED, Duration.ofSeconds(5));
            log.info("[取消竞争] 并发取消响应状态集合={} 异常数={} 最终状态={}",
                    results, errors.get(), terminal.getState());
            assertEquals(JobState.CANCELLED.name(), terminal.getState());
            // 无论竞争如何，最终只有一个确定终态
            assertTrue(Set.of("RUNNING", "CANCELLED").containsAll(results));
        } finally {
            engine.stop();
        }
    }
}
