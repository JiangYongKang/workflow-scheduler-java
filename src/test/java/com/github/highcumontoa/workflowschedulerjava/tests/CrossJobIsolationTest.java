package com.github.highcumontoa.workflowschedulerjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.workflowschedulerjava.config.SchedulerProperties;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitBatchRequest;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobRequest;
import com.github.highcumontoa.workflowschedulerjava.engine.SchedulerEngine;
import com.github.highcumontoa.workflowschedulerjava.engine.TaskHandler;
import com.github.highcumontoa.workflowschedulerjava.model.Job;
import com.github.highcumontoa.workflowschedulerjava.model.JobState;
import com.github.highcumontoa.workflowschedulerjava.model.RetryPolicy;
import com.github.highcumontoa.workflowschedulerjava.store.JsonFileJobStore;
import com.github.highcumontoa.workflowschedulerjava.store.JobStore;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.highcumontoa.workflowschedulerjava.tests.EngineTestSupport.awaitState;
import static com.github.highcumontoa.workflowschedulerjava.tests.EngineTestSupport.printStates;
import static com.github.highcumontoa.workflowschedulerjava.tests.EngineTestSupport.props;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 作业间隔离：一个作业超时尝试的迟到结果既不能污染它自己的后续尝试，也绝不能牵连
 * 同时在跑/排队的“别的作业”——不能摘掉别人的 Future、不能重复释放名额让旁观者越额启动。
 */
class CrossJobIsolationTest {

    private static final Logger log = LoggerFactory.getLogger(CrossJobIsolationTest.class);

    /** 按 jobId+attempt 注册独立闩的执行器：忽略中断，精确模拟“对取消不敏感”的僵尸执行。 */
    static final class PerAttemptHandler implements TaskHandler {
        private final ConcurrentMap<String, CountDownLatch> entered = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, CountDownLatch> release = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicInteger> execCount = new ConcurrentHashMap<>();

        private static String key(String id, int attempt) {
            return id + "#" + attempt;
        }

        CountDownLatch register(String id, int attempt) {
            entered.put(key(id, attempt), new CountDownLatch(1));
            release.put(key(id, attempt), new CountDownLatch(1));
            return entered.get(key(id, attempt));
        }

        void release(String id, int attempt) {
            CountDownLatch l = release.get(key(id, attempt));
            if (l != null) {
                l.countDown();
            }
        }

        void releaseAll() {
            release.values().forEach(CountDownLatch::countDown);
        }

        int executions(String id) {
            return execCount.getOrDefault(id, new AtomicInteger()).get();
        }

        CountDownLatch enteredLatch(String id, int attempt) {
            return entered.get(key(id, attempt));
        }

        @Override
        public void execute(Job job) throws Exception {
            int attempt = job.getAttempts();
            execCount.computeIfAbsent(job.getId(), k -> new AtomicInteger()).incrementAndGet();
            log.info("[脚本任务] job={} attempt={} 进入执行", job.getId(), attempt);
            CountDownLatch ent = entered.get(key(job.getId(), attempt));
            if (ent == null) {
                return; // 未注册脚本的尝试：立即成功
            }
            ent.countDown();
            boolean interrupted = false;
            while (true) {
                try {
                    release.get(key(job.getId(), attempt)).await();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true; // 忽略中断，模拟僵尸
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static SchedulerEngine newEngine(Path dir, SchedulerProperties p, TaskHandler h) {
        JobStore store = new JsonFileJobStore(dir, new ObjectMapper());
        return new SchedulerEngine(store, h, p);
    }

    private static SubmitJobRequest job(String id, String queue, Long timeout, int maxAttempts) {
        SubmitJobRequest r = new SubmitJobRequest();
        r.setId(id);
        r.setTaskType("scripted");
        r.setTaskParam(0L);
        r.setQueue(queue);
        if (timeout != null) {
            r.setTimeoutMillis(timeout);
        }
        r.setRetryPolicy(new RetryPolicy(maxAttempts, 20));
        return r;
    }

    /**
     * 全局上限 2：作业 A 超时后第 1 次尝试成为僵尸（占 1 个物理线程），第 2 次尝试补位
     * （占第 2 个名额），旁观者 C 只能排队。A 的第 1 次尝试迟到结束时：
     * 逻辑名额不得被重复归还（C 仍排队），A 的第 2 次尝试不受影响；只有 A 真正结束后 C 才动。
     */
    @Test
    void lateResultOfTimedOutAttemptDoesNotPromoteUnrelatedQueuedJob(@TempDir Path dir) throws Exception {
        SchedulerProperties p = props(dir);
        p.setGlobalConcurrency(3);
        p.setDefaultQueueConcurrency(1); // 同队列上限 1：A 与 C 争一个名额
        p.setDefaultTenantConcurrency(3);
        PerAttemptHandler handler = new PerAttemptHandler();
        CountDownLatch a1 = handler.register("a", 1);
        CountDownLatch a2 = handler.register("a", 2);
        handler.register("c", 1);
        SchedulerEngine engine = newEngine(dir, p, handler);
        engine.start();
        try {
            // A、C 一起提交（A 的僵尸尝试立即占 1 个名额，C 只能排队；A 超时后第 2 次尝试
            // 补走释放出来的逻辑名额，旁观者 C 因 PENDING 判定在 A 之后而继续排队）
            SubmitBatchRequest batch = new SubmitBatchRequest();
            // 同队列、队列上限 1：A 与 C 竞争同一个队列名额；物理全局给到 3 排除线程数干扰
            batch.setJobs(List.of(job("a", "q", 100L, 2), job("c", "q", null, 1)));
            engine.submit(batch);
            engine.setAttemptTimeoutOverride("a", 2, 30_000L);

            assertTrue(a1.await(5, TimeUnit.SECONDS), "A 第 1 次尝试进入执行");
            // C 先被判定为 QUEUED（队列名额被 A 占着）
            long queuedDeadline = System.currentTimeMillis() + 3000;
            while (System.currentTimeMillis() < queuedDeadline
                    && !JobState.QUEUED.name().equals(engine.get("c").getState())) {
                Thread.sleep(2);
            }
            assertEquals(JobState.QUEUED.name(), engine.get("c").getState(), "C 必须先排队");
            // A 超时：队列名额立即还给“A 的第 2 次尝试”（退避仅 20ms），C 仍应排队
            assertTrue(a2.await(5, TimeUnit.SECONDS), "超时后 A 第 2 次尝试补位执行");
            Thread.sleep(200); // 留出若干 tick，确认 C 没有被迟到结果/唤醒错误地放进 RUNNING
            assertEquals(JobState.QUEUED.name(), engine.get("c").getState(), "C 必须继续排队等待名额");

            // A 的第 1 次尝试迟到结束
            handler.release("a", 1);
            Thread.sleep(300);
            log.info("[隔离判定] A 迟到结果后: a={} c={} A执行次数={} C执行次数={}",
                    engine.get("a").getState(), engine.get("c").getState(),
                    handler.executions("a"), handler.executions("c"));
            assertEquals(JobState.RUNNING.name(), engine.get("a").getState(),
                    "A 的当前（第 2 次）尝试不受迟到结果影响");
            assertEquals(JobState.QUEUED.name(), engine.get("c").getState(),
                    "迟到结果不得把名额重复归还给旁观者 C");
            assertEquals(2, handler.executions("a"));
            assertEquals(0, handler.executions("c"), "C 一次都不该被启动");

            // 第 2 次尝试结束后 C 才获得名额；放行 C 后它成功
            handler.release("a", 2);
            awaitState(engine, "a", JobState.SUCCESS, Duration.ofSeconds(5));
            CountDownLatch cEntered = handler.enteredLatch("c", 1);
            assertTrue(cEntered != null && cEntered.await(5, TimeUnit.SECONDS),
                    "A 结束后 C 才启动");
            assertEquals(1, handler.executions("c"), "C 最终恰好执行一次");
            handler.release("c", 1);
            awaitState(engine, "c", JobState.SUCCESS, Duration.ofSeconds(5));
            printStates("跨作业隔离", engine);
        } finally {
            handler.releaseAll();
            engine.stop();
        }
    }
}
