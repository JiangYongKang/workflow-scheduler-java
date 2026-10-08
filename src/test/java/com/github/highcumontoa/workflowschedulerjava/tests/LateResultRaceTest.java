package com.github.highcumontoa.workflowschedulerjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.workflowschedulerjava.config.SchedulerProperties;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitBatchRequest;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobRequest;
import com.github.highcumontoa.workflowschedulerjava.engine.SchedulerEngine;
import com.github.highcumontoa.workflowschedulerjava.engine.TaskHandler;
import com.github.highcumontoa.workflowschedulerjava.model.AttemptRecord;
import com.github.highcumontoa.workflowschedulerjava.model.AttemptResult;
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
 * 迟到结果跨尝试的竞争修复（本类是该缺陷的稳定复现用例）。
 *
 * <p>场景：作业同时配置最长执行时间与重试。第 1 次尝试的任务对中断不敏感（吞掉中断信号、
 * 继续跑完），调度器在超时点按 TIMEOUT 判这一次失败并立即安排重试；第 1 次尝试之后才
 * “自己跑完”把结果回送。修复前该迟到结果会被当成第 2 次尝试的结果，导致作业提前
 * 成功/失败、第 2 次尝试的 Future 被误摘、名额被重复归还；修复后迟到结果只对它自己那一次
 * 尝试生效：被丢弃且不触碰任何账本，也不牵连正在跑的尝试。</p>
 *
 * <p>为保证用例在同一台机器上连跑两遍结果一致，任务全部用闩控制时序，而不是睡眠赌时序。</p>
 */
class LateResultRaceTest {

    private static final Logger log = LoggerFactory.getLogger(LateResultRaceTest.class);

    /** 可编排的执行器：按“作业 id + 尝试序号”注册脚本；进入执行先通知 ENTERED，再等 RELEASE。 */
    static final class ScriptedHandler implements TaskHandler {
        record Script(CountDownLatch entered, CountDownLatch release, boolean fail, boolean swallowInterrupt) {
        }

        private final ConcurrentMap<String, Script> scripts = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicInteger> execCount = new ConcurrentHashMap<>();

        /** key = jobId + "#" + attempt（attempt 从 1 开始）。 */
        Script register(String jobId, int attempt, boolean fail, boolean swallowInterrupt) {
            Script s = new Script(new CountDownLatch(1), new CountDownLatch(1), fail, swallowInterrupt);
            scripts.put(jobId + "#" + attempt, s);
            return s;
        }

        int executions(String jobId) {
            return execCount.getOrDefault(jobId, new AtomicInteger()).get();
        }

        @Override
        public void execute(Job job) throws Exception {
            int attempt = job.getAttempts();
            int n = execCount.computeIfAbsent(job.getId(), k -> new AtomicInteger()).incrementAndGet();
            Script s = scripts.get(job.getId() + "#" + attempt);
            log.info("[脚本任务] job={} attempt={} 第 {} 次进入执行（fail={}, 忽略中断={}）",
                    job.getId(), attempt, n, s != null && s.fail(), s != null && s.swallowInterrupt());
            if (s == null) {
                return; // 未注册脚本的尝试：立即成功
            }
            s.entered().countDown();
            if (s.swallowInterrupt()) {
                // 模拟“任务本身对取消不敏感”：interrupt 不抛出、不结束，直到测试放行才真正跑完
                awaitUninterruptibly(s.release());
                Thread.interrupted();
            } else {
                // 正常任务：等待期间响应中断
                s.release().await();
            }
            if (s.fail()) {
                throw new IllegalStateException("脚本任务迟到失败: 第 " + attempt + " 次尝试");
            }
        }
    }

    /** 不可中断地等到闩归零：即使收到 interrupt 也继续等，用于模拟“对取消不敏感”的任务。 */
    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    latch.await();
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static SchedulerEngine newEngine(Path dir, SchedulerProperties p, TaskHandler handler) {
        JobStore store = new JsonFileJobStore(dir, new ObjectMapper());
        return new SchedulerEngine(store, handler, p);
    }

    private static SubmitJobRequest job(String id, Long timeout, int maxAttempts, long backoff) {
        SubmitJobRequest r = new SubmitJobRequest();
        r.setId(id);
        r.setTaskType("scripted");
        r.setTaskParam(0L);
        if (timeout != null) {
            r.setTimeoutMillis(timeout);
        }
        r.setRetryPolicy(new RetryPolicy(maxAttempts, backoff));
        return r;
    }

    private static SubmitJobRequest job(String id, String queue, Long timeout, int maxAttempts, long backoff) {
        SubmitJobRequest r = job(id, timeout, maxAttempts, backoff);
        r.setQueue(queue);
        return r;
    }

    /**
     * 测试编排辅助：提交后直接给持久化实体写“按尝试序号的超时覆盖”，让第 2 次尝试不被
     * 100ms 的超时预算误伤，时序完全由闩控制。覆盖不持久化、生产路径恒为空。
     */
    private static void overrideAttemptTimeouts(SchedulerEngine e, String id,
                                                java.util.Map<Integer, Long> overrides) {
        overrides.forEach((attempt, millis) -> e.setAttemptTimeoutOverride(id, attempt, millis));
    }

    private static void submit(SchedulerEngine e, SubmitJobRequest... jobs) {
        SubmitBatchRequest batch = new SubmitBatchRequest();
        batch.setJobs(List.of(jobs));
        e.submit(batch);
    }

    private static void waitExecutions(ScriptedHandler handler, String id, int n) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (handler.executions(id) < n && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(n, handler.executions(id), "期望 " + id + " 已进入第 " + n + " 次执行");
    }

    private static void assertTrueForAWhile(String msg, java.util.function.BooleanSupplier cond, long holdMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + holdMs;
        while (System.currentTimeMillis() < deadline) {
            assertTrue(cond.getAsBoolean(), msg);
            Thread.sleep(10);
        }
        assertTrue(cond.getAsBoolean(), msg);
    }

    /**
     * 核心复现：第 1 次尝试超时后迟到“成功”，第 2 次尝试正在跑——迟到成功绝不能让作业提前
     * SUCCESS；第 2 次尝试自己结束后才按其自身结果定终态。
     */
    @Test
    void lateSuccessOfTimedOutAttemptIsNotAdoptedByNextAttempt(@TempDir Path dir) throws Exception {
        SchedulerProperties p = props(dir);
        // 物理线程池大小 = 全局配额：僵尸线程与第 2 次尝试需要同时存在，故至少 2
        p.setGlobalConcurrency(2);
        ScriptedHandler handler = new ScriptedHandler();
        ScriptedHandler.Script first = handler.register("z", 1, false, true);  // 僵尸：忽略中断，最终成功
        ScriptedHandler.Script second = handler.register("z", 2, false, false); // 第 2 次：正常受控
        SchedulerEngine e = newEngine(dir, p, handler);
        e.start();
        try {
            submit(e, job("z", 100L, 2, 20));
            overrideAttemptTimeouts(e, "z", java.util.Map.of(2, 30_000L));
            assertTrue(first.entered().await(5, TimeUnit.SECONDS), "第 1 次尝试应已进入执行");
            // 超时判 TIMEOUT -> 退避 -> 第 2 次尝试进入 RUNNING 并开始执行
            assertTrue(second.entered().await(5, TimeUnit.SECONDS), "第 2 次尝试应已启动");
            waitExecutions(handler, "z", 2);

            // 第 1 次尝试此刻才“自己跑完”，回送迟到成功
            first.release().countDown();
            Thread.sleep(200);
            var view = e.get("z");
            log.info("[迟到结果判定] 迟到成功回送后 state={} attempts={}/2", view.getState(), view.getAttempts());
            assertEquals(JobState.RUNNING.name(), view.getState(),
                    "上一次尝试的迟到成功不得算到正在跑的第 2 次尝试头上");
            assertEquals(2, view.getAttempts());

            // 第 2 次尝试自己成功，作业才 SUCCESS
            second.release().countDown();
            var done = awaitState(e, "z", JobState.SUCCESS, Duration.ofSeconds(5));
            printStates("迟到成功不冒名", e);
            log.info("[迟到结果判定] 第 2 次尝试自己成功后作业才 SUCCESS attempts={}", done.getAttempts());
            assertEquals(2, done.getAttempts());
            List<AttemptRecord> records = e.listAttempts("z");
            assertEquals(2, records.size(), "真实只发生过 2 次尝试，记录不多不少");
            assertEquals(AttemptResult.TIMEOUT, records.get(0).getResult(), "第 1 次记录必须是 TIMEOUT");
            assertEquals(AttemptResult.SUCCESS, records.get(1).getResult(), "第 2 次记录才是 SUCCESS");
        } finally {
            first.release().countDown();
            second.release().countDown();
            e.stop();
        }
    }

    /**
     * 迟到失败同样不能冒名：第 1 次超时后迟到“失败”，不能让正在跑的第 2 次尝试提前 FAILED；
     * 第 2 次尝试自己失败耗尽重试后，作业才 FAILED。
     */
    @Test
    void lateFailureOfTimedOutAttemptDoesNotFailRunningAttempt(@TempDir Path dir) throws Exception {
        SchedulerProperties p = props(dir);
        p.setGlobalConcurrency(2);
        ScriptedHandler handler = new ScriptedHandler();
        ScriptedHandler.Script first = handler.register("zf", 1, true, true);
        ScriptedHandler.Script second = handler.register("zf", 2, true, false);
        SchedulerEngine e = newEngine(dir, p, handler);
        e.start();
        try {
            submit(e, job("zf", 100L, 2, 20));
            overrideAttemptTimeouts(e, "zf", java.util.Map.of(2, 30_000L));
            assertTrue(first.entered().await(5, TimeUnit.SECONDS));
            assertTrue(second.entered().await(5, TimeUnit.SECONDS), "第 2 次尝试应已启动");
            waitExecutions(handler, "zf", 2);

            first.release().countDown(); // 第 1 次尝试迟到失败回送
            Thread.sleep(200);
            var view = e.get("zf");
            log.info("[迟到结果判定] 迟到失败回送后 state={} attempts={}/2", view.getState(), view.getAttempts());
            assertEquals(JobState.RUNNING.name(), view.getState(),
                    "上一次尝试的迟到失败不得提前结束正在跑的第 2 次尝试");
            assertEquals(2, view.getAttempts());

            second.release().countDown(); // 第 2 次尝试自己失败 -> 重试耗尽
            var done = awaitState(e, "zf", JobState.FAILED, Duration.ofSeconds(5));
            printStates("迟到失败不冒名", e);
            log.info("[迟到结果判定] 第 2 次尝试自己失败耗尽重试后才 FAILED attempts={}", done.getAttempts());
            assertEquals(2, done.getAttempts());
            List<AttemptRecord> records = e.listAttempts("zf");
            assertEquals(2, records.size());
            assertEquals(AttemptResult.TIMEOUT, records.get(0).getResult());
            assertEquals(AttemptResult.FAILED, records.get(1).getResult());
        } finally {
            first.release().countDown();
            second.release().countDown();
            e.stop();
        }
    }

    /**
     * 名额只在超时点归还一次：迟到结果不能重复释放名额把排队作业放进来，也不能让正在跑的
     * 尝试丢掉名额。用队列配额 1 卡住旁观者（物理池给到 3，排除“线程不够”这种无关因素）。
     */
    @Test
    void slotReleasedExactlyOnceAndLateResultCannotPromoteQueuedJob(@TempDir Path dir) throws Exception {
        SchedulerProperties p = props(dir);
        p.setGlobalConcurrency(3);
        p.setDefaultQueueConcurrency(1); // zs 与 observer 同队列，队列上限 1
        p.setDefaultTenantConcurrency(3);
        ScriptedHandler handler = new ScriptedHandler();
        ScriptedHandler.Script first = handler.register("zs", 1, false, true);
        ScriptedHandler.Script second = handler.register("zs", 2, false, false);
        ScriptedHandler.Script observer = handler.register("observer", 1, false, false);
        SchedulerEngine e = newEngine(dir, p, handler);
        e.start();
        try {
            // 先只提交 zs 并等它进入 RUNNING，避免作业表迭代顺序让旁观者先占名额
            submit(e, job("zs", "q", 100L, 2, 20));
            overrideAttemptTimeouts(e, "zs", java.util.Map.of(2, 30_000L));
            assertTrue(first.entered().await(5, TimeUnit.SECONDS));
            // 队列 q 名额此刻被 zs 占着，observer 提交后只能排队
            submit(e, job("observer", "q", (Long) null, 1, 0));
            assertTrue(second.entered().await(5, TimeUnit.SECONDS), "超时释放名额后第 2 次尝试补位");
            waitExecutions(handler, "zs", 2);
            // 旁观者必须仍在排队（队列上限 1，名额属于第 2 次尝试）
            assertEquals(JobState.QUEUED.name(), e.get("observer").getState());

            // 第 1 次尝试迟到成功：修复前会把第 2 次尝试的名额“再还一次”，旁观者将被放进 RUNNING
            first.release().countDown();
            assertTrueForAWhile("迟到结果重复归还名额会让旁观者越过队列配额",
                    () -> !JobState.RUNNING.name().equals(e.get("observer").getState()), 400);
            assertTrueForAWhile("迟到结果后正在跑的第 2 次尝试必须保持 RUNNING",
                    () -> JobState.RUNNING.name().equals(e.get("zs").getState()), 400);
            log.info("[名额判定] 迟到结果回送后 observer={}、zs={}（名额未被重复归还）",
                    e.get("observer").getState(), e.get("zs").getState());

            second.release().countDown();
            var done = awaitState(e, "zs", JobState.SUCCESS, Duration.ofSeconds(5));
            assertEquals(2, done.getAttempts());
            assertTrue(observer.entered().await(5, TimeUnit.SECONDS), "第 2 次尝试结束后旁观者才获得名额");
            observer.release().countDown();
            awaitState(e, "observer", JobState.SUCCESS, Duration.ofSeconds(5));
            printStates("名额不重复归还", e);
        } finally {
            first.release().countDown();
            second.release().countDown();
            observer.release().countDown();
            e.stop();
        }
    }

    /** 连续多次超时直到重试耗尽：FAILED、下游按原规则 UNREACHABLE、记录条数等于真实尝试次数。 */
    @Test
    void repeatedTimeoutsExhaustRetryAndPropagateUnreachable(@TempDir Path dir) {
        SchedulerProperties p = props(dir);
        ScriptedHandler handler = new ScriptedHandler();
        handler.register("zt", 1, false, true);
        handler.register("zt", 2, false, true);
        handler.register("zt", 3, false, true);
        SchedulerEngine e = newEngine(dir, p, handler);
        e.start();
        try {
            SubmitJobRequest z = job("zt", 80L, 3, 10);
            SubmitJobRequest down = new SubmitJobRequest();
            down.setId("zt-down");
            down.setTaskType("scripted");
            down.setDependencies(List.of("zt"));
            submit(e, z, down);

            var failed = awaitState(e, "zt", JobState.FAILED, Duration.ofSeconds(10));
            var unreachable = awaitState(e, "zt-down", JobState.UNREACHABLE, Duration.ofSeconds(10));
            printStates("连续超时耗尽", e);
            log.info("[连续超时判定] zt state={} attempts={}/3 failure={} downstream blockedBy={}",
                    failed.getState(), failed.getAttempts(), failed.getFailureReason(), unreachable.getBlockedBy());
            assertEquals(3, failed.getAttempts(), "三次超时各消耗一次尝试");
            assertTrue(failed.getFailureReason().contains("TimeoutException"));
            assertEquals("zt", unreachable.getBlockedBy());
            assertEquals(0, unreachable.getAttempts(), "下游不可达，从未执行");
            List<AttemptRecord> records = e.listAttempts("zt");
            assertEquals(3, records.size(), "记录条数等于真实尝试次数");
            assertTrue(records.stream().allMatch(r -> r.getResult() == AttemptResult.TIMEOUT));
        } finally {
            // 三次僵尸尝试均已被超时收尾，无需再放行；随引擎停止即可
            e.stop();
        }
    }

    /**
     * 超时与“自己跑完”撞车，结果先到：结果把本次尝试收尾为 SUCCESS 后，挂着的超时回调必须
     * 作废，作业稳定 SUCCESS，不翻成 FAILED/重试。
     */
    @Test
    void completionWinsOverTimeoutAndTimeoutBecomesNoOp(@TempDir Path dir) throws Exception {
        SchedulerProperties p = props(dir);
        ScriptedHandler handler = new ScriptedHandler();
        ScriptedHandler.Script only = handler.register("tc", 1, false, false);
        SchedulerEngine e = newEngine(dir, p, handler);
        e.start();
        try {
            submit(e, job("tc", 300L, 1, 0));
            assertTrue(only.entered().await(5, TimeUnit.SECONDS));
            // 在 300ms 超时点之前放行，让任务自己成功；结果先回送并被采纳
            only.release().countDown();
            var done = awaitState(e, "tc", JobState.SUCCESS, Duration.ofSeconds(5));
            // 越过超时点后终态必须保持
            Thread.sleep(500);
            var after = e.get("tc");
            log.info("[撞车判定:完成先到] 超时点过后 state={} attempts={}", after.getState(), after.getAttempts());
            assertEquals(JobState.SUCCESS.name(), after.getState());
            assertEquals(1, done.getAttempts());
            List<AttemptRecord> records = e.listAttempts("tc");
            assertEquals(1, records.size());
            assertEquals(AttemptResult.SUCCESS, records.get(0).getResult());
        } finally {
            only.release().countDown();
            e.stop();
        }
    }

    /**
     * 超时与“自己跑完”撞车，超时先到：超时判本次失败并重试；第 1 次尝试随后才完成（迟到成功），
     * 绝不能翻掉已定的结果。与 {@link #lateSuccessOfTimedOutAttemptIsNotAdoptedByNextAttempt}
     * 的区别：这里第 1 次是正常响应中断的任务，只是碰巧在超时点附近才跑完。
     */
    @Test
    void timeoutWinsOverCompletionAndLateCompletionIsDropped(@TempDir Path dir) throws Exception {
        SchedulerProperties p = props(dir);
        p.setGlobalConcurrency(2);
        ScriptedHandler handler = new ScriptedHandler();
        ScriptedHandler.Script first = handler.register("tt", 1, false, false); // 响应中断，但测试延迟放行模拟“刚好超时”
        ScriptedHandler.Script second = handler.register("tt", 2, false, false);
        SchedulerEngine e = newEngine(dir, p, handler);
        e.start();
        try {
            submit(e, job("tt", 80L, 2, 20));
            overrideAttemptTimeouts(e, "tt", java.util.Map.of(2, 30_000L));
            assertTrue(first.entered().await(5, TimeUnit.SECONDS));
            // 超时先生效：第 1 次尝试判 TIMEOUT 并安排重试
            assertTrue(second.entered().await(5, TimeUnit.SECONDS), "超时后第 2 次尝试已启动");
            waitExecutions(handler, "tt", 2);
            // 放行第 1 次尝试：无论它此前是否已被 cancel 打断，它的回送都属于第 1 次，
            // 此时到达只能作为“超时后的迟到完成”被丢弃
            first.release().countDown();
            Thread.sleep(200);
            assertEquals(JobState.RUNNING.name(), e.get("tt").getState(),
                    "超时已定的结果不能被随后的迟到完成翻掉");
            second.release().countDown();
            var done = awaitState(e, "tt", JobState.SUCCESS, Duration.ofSeconds(5));
            printStates("超时先到完成迟到", e);
            log.info("[撞车判定:超时先到] 迟到完成被丢弃，第 2 次成功 attempts={}", done.getAttempts());
            assertEquals(2, done.getAttempts());
            List<AttemptRecord> records = e.listAttempts("tt");
            assertEquals(AttemptResult.TIMEOUT, records.get(0).getResult());
            assertEquals(AttemptResult.SUCCESS, records.get(1).getResult());
        } finally {
            first.release().countDown();
            second.release().countDown();
            e.stop();
        }
    }

    /**
     * 超时与取消撞车，取消先到（在超时点前取消）：取消优先，结果唯一为 CANCELLED；之后超时
     * 点过去不得翻成 TIMEOUT/FAILED。
     */
    @Test
    void cancelBeforeTimeoutWinsDeterministically(@TempDir Path dir) throws Exception {
        SchedulerProperties p = props(dir);
        ScriptedHandler handler = new ScriptedHandler();
        ScriptedHandler.Script only = handler.register("cx", 1, false, false); // 响应中断
        SchedulerEngine e = newEngine(dir, p, handler);
        e.start();
        try {
            submit(e, job("cx", 500L, 1, 0));
            assertTrue(only.entered().await(5, TimeUnit.SECONDS));
            e.cancel("cx"); // 100ms 内取消，早于 500ms 超时点
            var view = awaitState(e, "cx", JobState.CANCELLED, Duration.ofSeconds(5));
            Thread.sleep(600); // 越过超时点
            var after = e.get("cx");
            log.info("[撞车判定:取消先到] 超时点过后 state={}（取消优先，结果唯一）", after.getState());
            assertEquals(JobState.CANCELLED.name(), after.getState());
            assertEquals(JobState.CANCELLED.name(), view.getState());
            List<AttemptRecord> records = e.listAttempts("cx");
            assertEquals(1, records.size());
            assertEquals(AttemptResult.CANCELLED, records.get(0).getResult());
        } finally {
            only.release().countDown();
            e.stop();
        }
    }

    /**
     * 超时与取消撞车，取消请求在超时判定同一时刻已存在：即使超时先触发判定，取消请求优先，
     * 终态为 CANCELLED 而不是 TIMEOUT/FAILED。
     */
    @Test
    void cancelRequestWinsWhenTimeoutFiresFirst(@TempDir Path dir) throws Exception {
        SchedulerProperties p = props(dir);
        ScriptedHandler handler = new ScriptedHandler();
        // 僵尸任务：忽略中断，因此 cancel 不会让执行体结束，只能等超时判定来收尾
        ScriptedHandler.Script only = handler.register("cz", 1, false, true);
        SchedulerEngine e = newEngine(dir, p, handler);
        e.start();
        try {
            submit(e, job("cz", 120L, 1, 0));
            assertTrue(only.entered().await(5, TimeUnit.SECONDS));
            // 先送取消请求（cancelRequested 置位），再等超时点触发 onTimeout 收尾
            e.cancel("cz");
            var view = awaitState(e, "cz", JobState.CANCELLED, Duration.ofSeconds(5));
            Thread.sleep(300);
            var after = e.get("cz");
            printStates("超时触发但取消优先", e);
            log.info("[撞车判定:超时触发取消优先] state={} failure={}", after.getState(), after.getFailureReason());
            assertEquals(JobState.CANCELLED.name(), after.getState());
            assertEquals(JobState.CANCELLED.name(), view.getState());
            List<AttemptRecord> records = e.listAttempts("cz");
            assertEquals(1, records.size());
            assertEquals(AttemptResult.CANCELLED, records.get(0).getResult());
        } finally {
            only.release().countDown();
            e.stop();
        }
    }

    /**
     * 调度器停止后不得再有后台活动推进状态：stop 后再放行卡住的任务让其迟到完成，作业状态
     * 不得被任何残留回调/结果处理改变；后台线程全部终止。
     */
    @Test
    void noBackgroundActivityAfterStop(@TempDir Path dir) throws Exception {
        SchedulerProperties p = props(dir);
        p.setGlobalConcurrency(2);
        ScriptedHandler handler = new ScriptedHandler();
        ScriptedHandler.Script first = handler.register("sd", 1, false, true);
        ScriptedHandler.Script second = handler.register("sd", 2, false, false);
        SchedulerEngine e = newEngine(dir, p, handler);
        e.start();
        try {
            submit(e, job("sd", 80L, 2, 20));
            overrideAttemptTimeouts(e, "sd", java.util.Map.of(2, 30_000L));
            assertTrue(first.entered().await(5, TimeUnit.SECONDS));
            assertTrue(second.entered().await(5, TimeUnit.SECONDS), "第 2 次尝试已在跑");
            waitExecutions(handler, "sd", 2);
        } finally {
            e.stop();
        }
        assertTrue(e.isBackgroundStopped(), "stop 后调度循环、工作池、超时计时器都必须终止");
        String stateFrozen = e.get("sd").getState();
        int attemptsFrozen = e.get("sd").getAttempts();
        // 停止后两个卡住的任务才迟到结束：任何后台处理都不得再改状态
        first.release().countDown();
        second.release().countDown();
        Thread.sleep(400);
        var after = e.get("sd");
        log.info("[停止判定] stop 后迟到结果到达，state 冻结前={} 后={} attempts={}/{}",
                stateFrozen, after.getState(), attemptsFrozen, after.getAttempts());
        assertEquals(stateFrozen, after.getState(), "停止后状态不得被迟到结果改变");
        assertEquals(attemptsFrozen, after.getAttempts());
        assertTrue(e.isBackgroundStopped());
    }

    /**
     * 兼容性：任务自己抛异常（非超时、非中断）仍按既有 FAILED -> 重试逻辑走，重试成功后
     * 记录依次为 FAILED、SUCCESS；本次改动的尝试序号守卫不得影响正常结果的归属。
     */
    @Test
    void taskExceptionStillRetriesAndSucceeds(@TempDir Path dir) throws Exception {
        SchedulerProperties p = props(dir);
        ScriptedHandler handler = new ScriptedHandler();
        ScriptedHandler.Script first = handler.register("ex", 1, true, false); // 第 1 次放行后抛异常
        // 第 2 次尝试不注册脚本：按执行器约定立即成功
        SchedulerEngine e = newEngine(dir, p, handler);
        e.start();
        try {
            submit(e, job("ex", null, 2, 10)); // 无超时
            assertTrue(first.entered().await(5, TimeUnit.SECONDS));
            first.release().countDown();
            var done = awaitState(e, "ex", JobState.SUCCESS, Duration.ofSeconds(5));
            log.info("[兼容判定:异常重试] state={} attempts={}", done.getState(), done.getAttempts());
            assertEquals(2, done.getAttempts());
            List<AttemptRecord> records = e.listAttempts("ex");
            assertEquals(2, records.size());
            assertEquals(AttemptResult.FAILED, records.get(0).getResult());
            assertTrue(records.get(0).getError().contains("IllegalStateException"));
            assertEquals(AttemptResult.SUCCESS, records.get(1).getResult());
        } finally {
            first.release().countDown();
            e.stop();
        }
    }

    /**
     * 兼容性：未配置最长执行时间的作业，任务跑得久也不会被判超时；迟到完成正常算成功，
     * 行为与旧版一致（不安排任何超时计时任务）。
     */
    @Test
    void jobWithoutTimeoutNeverTimesOut(@TempDir Path dir) throws Exception {
        SchedulerProperties p = props(dir);
        ScriptedHandler handler = new ScriptedHandler();
        ScriptedHandler.Script only = handler.register("nt", 1, false, false);
        SchedulerEngine e = newEngine(dir, p, handler);
        e.start();
        try {
            submit(e, job("nt", null, 1, 0)); // 无超时
            assertTrue(only.entered().await(5, TimeUnit.SECONDS));
            // 远超任何可能的超时点后才放行
            Thread.sleep(400);
            only.release().countDown();
            var done = awaitState(e, "nt", JobState.SUCCESS, Duration.ofSeconds(5));
            log.info("[兼容判定:无超时] state={} attempts={} timeoutMillis={}",
                    done.getState(), done.getAttempts(), done.getTimeoutMillis());
            assertEquals(1, done.getAttempts());
            assertEquals(null, done.getTimeoutMillis());
            List<AttemptRecord> records = e.listAttempts("nt");
            assertEquals(1, records.size());
            assertEquals(AttemptResult.SUCCESS, records.get(0).getResult());
        } finally {
            only.release().countDown();
            e.stop();
        }
    }
}
