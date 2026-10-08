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
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
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
 * 超时 × 重试 × 取消 的竞争确定性测试。
 *
 * <p>任务体由 {@link ControllableTaskHandler} 控制，<b>故意不响应中断</b>，
 * 因此可以稳定制造“作业已被判超时、旧尝试其实还在跑、稍后迟到结果”的时序，
 * 验证一次执行的结果只对它自己那一次生效。</p>
 */
class TimeoutRetryRaceTest {

    private static final Logger log = LoggerFactory.getLogger(TimeoutRetryRaceTest.class);

    private static SchedulerEngine engine(Path dir, SchedulerProperties p, ControllableTaskHandler h) {
        return new SchedulerEngine(new JsonFileJobStore(dir, new ObjectMapper()), h, p);
    }

    /**
     * 构造一个可按「作业 id + 尝试序号」覆盖超时预算的引擎：
     * 默认每次尝试用作业配置值，override 命中时用覆盖值。用于让“只有第 1 次短超时”
     * 的场景可稳定复现，而后续尝试在观察窗口内不会自己超时。
     */
    private static SchedulerEngine engineWithTimeoutOverride(
            Path dir, SchedulerProperties p, TaskHandler h,
            java.util.Map<java.util.Map.Entry<String, Integer>, Long> override) {
        return new SchedulerEngine(new JsonFileJobStore(dir, new ObjectMapper()), h, p) {
            @Override
            protected Long timeoutForAttempt(Job job, int attempt) {
                Long v = override.get(java.util.Map.entry(job.getId(), attempt));
                return v != null ? v : super.timeoutForAttempt(job, attempt);
            }
        };
    }

    private static SubmitJobRequest job(String id, Long timeout, int maxAttempts, long backoff) {
        SubmitJobRequest r = new SubmitJobRequest();
        r.setId(id);
        r.setTaskType("controlled");
        r.setRetryPolicy(new RetryPolicy(maxAttempts, backoff));
        if (timeout != null) {
            r.setTimeoutMillis(timeout);
        }
        return r;
    }

    private static SubmitJobRequest noop(String id) {
        SubmitJobRequest r = new SubmitJobRequest();
        r.setId(id);
        r.setTaskType("noop");
        return r;
    }

    private static SubmitJobRequest downstream(String id, String upstream) {
        SubmitJobRequest r = noop(id);
        r.setDependencies(List.of(upstream));
        return r;
    }

    private static void submit(SchedulerEngine e, SubmitJobRequest... jobs) {
        SubmitBatchRequest batch = new SubmitBatchRequest();
        batch.setJobs(List.of(jobs));
        e.submit(batch);
    }

    /**
     * 核心场景：第 1 次执行被判超时进入重试，但任务没真停下来；它迟到的“成功”
     * 绝不能被算到正在运行的第 2 次头上（旧实现会直接把作业置成 SUCCESS）。
     */
    @Test
    void lateSuccessOfTimedOutAttemptIsNotAttributedToNextAttempt(@TempDir Path dir) throws Exception {
        ControllableTaskHandler tasks = new ControllableTaskHandler();
        // 第 1 次短超时（150ms），第 2 次给 30s 窗口：保证观察迟到结果期间它不会自己超时
        SchedulerEngine e = engineWithTimeoutOverride(dir, props(dir), tasks, java.util.Map.of(
                java.util.Map.entry("race", 1), 150L,
                java.util.Map.entry("race", 2), 30_000L));
        e.start();
        try {
            // 仅第 1 次需要快速超时（由引擎 override 控制）；作业默认给 30s 窗口
            submit(e, job("race", 30_000L, 2, 50));
            tasks.awaitStarted("race", 1);
            // 150ms 后超时：第 1 次按 TIMEOUT 失败，退避后启动第 2 次
            tasks.awaitStarted("race", 2);
            assertEquals(2, e.get("race").getAttempts(), "第 2 次尝试应已启动");
            assertEquals(JobState.RUNNING.name(), e.get("race").getState());

            // 旧尝试此刻才“自己跑完”并回送成功 —— 必须被丢弃，不能打乱第 2 次
            tasks.release("race", 1);
            Thread.sleep(150);
            log.info("[迟到结果-成功] 旧尝试迟到成功后：state={} attempts={}",
                    e.get("race").getState(), e.get("race").getAttempts());
            assertEquals(JobState.RUNNING.name(), e.get("race").getState(),
                    "迟到成功不得把正在跑的第 2 次顶替成 SUCCESS");
            assertEquals(2, e.get("race").getAttempts());

            tasks.release("race", 2);
            var view = awaitState(e, "race", JobState.SUCCESS, Duration.ofSeconds(5));
            printStates("迟到成功不跨尝试", e);
            assertEquals(2, view.getAttempts(), "成功必须属于真正跑完的第 2 次尝试");
            List<AttemptRecord> records = e.listAttempts("race");
            assertEquals(2, records.size(), "记录条数必须等于真实尝试次数");
            assertEquals(AttemptResult.TIMEOUT, records.get(0).getResult());
            assertEquals(AttemptResult.SUCCESS, records.get(1).getResult(),
                    "SUCCESS 只能由第 2 次尝试自己的结果写入");
        } finally {
            e.stop();
        }
    }

    /**
     * 反向迟到：旧尝试迟到的是“失败”，也不能让正在跑的新尝试提前失败/重试。
     */
    @Test
    void lateFailureOfTimedOutAttemptDoesNotFailNextAttempt(@TempDir Path dir) throws Exception {
        ControllableTaskHandler tasks = new ControllableTaskHandler();
        tasks.failOn("race", 1, "旧尝试迟到的失败");
        SchedulerEngine e = engineWithTimeoutOverride(dir, props(dir), tasks, java.util.Map.of(
                java.util.Map.entry("race", 1), 150L,
                java.util.Map.entry("race", 2), 30_000L,
                java.util.Map.entry("race", 3), 30_000L));
        e.start();
        try {
            submit(e, job("race", 30_000L, 3, 50));
            tasks.awaitStarted("race", 1);
            tasks.awaitStarted("race", 2);

            tasks.release("race", 1); // 第 1 次迟到失败
            Thread.sleep(150);
            log.info("[迟到结果-失败] 旧尝试迟到失败后：state={} attempts={}",
                    e.get("race").getState(), e.get("race").getAttempts());
            assertEquals(JobState.RUNNING.name(), e.get("race").getState(),
                    "迟到失败不得拖垮正在跑的第 2 次尝试");
            assertEquals(2, e.get("race").getAttempts());

            tasks.release("race", 2); // 第 2 次真正成功
            var view = awaitState(e, "race", JobState.SUCCESS, Duration.ofSeconds(5));
            assertEquals(2, view.getAttempts());
            List<AttemptRecord> records = e.listAttempts("race");
            assertEquals(List.of(AttemptResult.TIMEOUT, AttemptResult.SUCCESS),
                    records.stream().map(AttemptRecord::getResult).toList());
        } finally {
            e.stop();
        }
    }

    /**
     * 同一次尝试内“超时”和“自己跑完成功”撞车，完成先到：超时判定随后必须是空操作，
     * 作业稳定 SUCCESS，不能在超时点被翻成 FAILED。
     */
    @Test
    void completionWinsOverTimeoutOnSameAttemptAndStaysStable(@TempDir Path dir) throws Exception {
        ControllableTaskHandler tasks = new ControllableTaskHandler();
        SchedulerEngine e = engine(dir, props(dir), tasks);
        e.start();
        try {
            submit(e, job("near", 200L, 1, 0));
            tasks.awaitStarted("near", 1);
            // 在超时点之前放行：任务先自然成功
            Thread.sleep(80);
            tasks.release("near", 1);
            var view = awaitState(e, "near", JobState.SUCCESS, Duration.ofSeconds(5));
            // 越过超时点再观察，状态不得翻转
            Thread.sleep(300);
            var after = e.get("near");
            log.info("[超时vs完成-完成先到] 超时点过后 state={} attempts={}", after.getState(), after.getAttempts());
            assertEquals(JobState.SUCCESS.name(), after.getState());
            assertEquals(1, view.getAttempts());
            List<AttemptRecord> records = e.listAttempts("near");
            assertEquals(1, records.size());
            assertEquals(AttemptResult.SUCCESS, records.get(0).getResult());
        } finally {
            e.stop();
        }
    }

    /**
     * “超时”和“取消”几乎同时发生：两个线程分别触发、多轮重复，
     * 无论锁内谁先谁后，终态都必须唯一收敛为 CANCELLED，且只产生 1 条执行记录。
     */
    @Test
    void timeoutAndCancelRacingAlwaysConvergeToCancelled(@TempDir Path dir) throws Exception {
        for (int round = 0; round < 20; round++) {
            ControllableTaskHandler tasks = new ControllableTaskHandler();
            SchedulerProperties p = props(dir.resolve("round-" + round));
            SchedulerEngine e = engine(dir.resolve("round-" + round), p, tasks);
            e.start();
            try {
                submit(e, job("j", 100L, 3, 50));
                tasks.awaitStarted("j", 1);
                // 两个动作在 ~100ms 超时点附近同时发出
                Thread t1 = new Thread(() -> e.cancel("j"), "cancel-" + round);
                t1.start();
                Thread.sleep(90 + (round % 3) * 15L); // 90/105/120ms，制造两种先后顺序
                tasks.release("j", 1); // 任务无视取消继续跑完
                t1.join(2000);
                var view = awaitState(e, "j", JobState.CANCELLED, Duration.ofSeconds(5));
                List<AttemptRecord> records = e.listAttempts("j");
                log.info("[超时vs取消 round={}] state={} attempts={} records={}",
                        round, view.getState(), view.getAttempts(),
                        records.stream().map(r -> r.getResult().name()).toList());
                assertEquals(JobState.CANCELLED.name(), view.getState(), "第 " + round + " 轮终态必须唯一");
                assertEquals(1, records.size(), "只真正开始过 1 次执行，记录只能有 1 条");
                assertTrue(records.get(0).getResult() == AttemptResult.CANCELLED
                                || records.get(0).getResult() == AttemptResult.TIMEOUT,
                        "记录结果须可解释（取消先生效=CANCELLED，超时先生效=TIMEOUT 随后整体取消）");
            } finally {
                e.stop();
            }
        }
    }

    /**
     * 连续超时直到重试耗尽：FAILED，下游 UNREACHABLE，执行记录条数等于真实尝试次数。
     */
    @Test
    void allAttemptsTimeoutThenFailedAndDownstreamUnreachable(@TempDir Path dir) throws Exception {
        ControllableTaskHandler tasks = new ControllableTaskHandler();
        // 每次尝试都只给 120ms：连续三次超时（任务始终不放行），直到重试耗尽
        SchedulerEngine e = engineWithTimeoutOverride(dir, props(dir), tasks, java.util.Map.of(
                java.util.Map.entry("stuck", 1), 120L,
                java.util.Map.entry("stuck", 2), 120L,
                java.util.Map.entry("stuck", 3), 120L));
        e.start();
        try {
            submit(e, job("stuck", 30_000L, 3, 30), downstream("child", "stuck"));
            tasks.awaitStarted("stuck", 1);
            tasks.awaitStarted("stuck", 2);
            tasks.awaitStarted("stuck", 3);
            var stuck = awaitState(e, "stuck", JobState.FAILED, Duration.ofSeconds(10));
            var child = awaitState(e, "child", JobState.UNREACHABLE, Duration.ofSeconds(5));
            // 迟到结果（三次都放行）不得改变终态
            tasks.release("stuck", 1);
            tasks.release("stuck", 2);
            tasks.release("stuck", 3);
            Thread.sleep(200);
            log.info("[连续超时] stuck={}/{} child={}/blockedBy={}",
                    e.get("stuck").getState(), e.get("stuck").getAttempts(),
                    e.get("child").getState(), e.get("child").getBlockedBy());
            assertEquals(JobState.FAILED.name(), e.get("stuck").getState());
            assertEquals(JobState.UNREACHABLE.name(), e.get("child").getState());
            assertEquals("stuck", child.getBlockedBy());
            assertEquals(3, stuck.getAttempts());
            assertTrue(stuck.getFailureReason().contains("TimeoutException"));
            List<AttemptRecord> records = e.listAttempts("stuck");
            assertEquals(3, records.size(), "三次真实尝试三条记录，迟到结果不增不减");
            assertTrue(records.stream().allMatch(r -> r.getResult() == AttemptResult.TIMEOUT));
        } finally {
            e.stop();
        }
    }

    /**
     * 名额只在超时判定时归还一次：迟到结果不能重复归还名额，也不能把正在跑的重试
     * 从运行账本里摘掉（否则并发 1 也会超额启动）。
     */
    @Test
    void quotaSlotReleasedExactlyOnceAcrossLateResult(@TempDir Path dir) throws Exception {
        SchedulerProperties p = props(dir);
        p.setGlobalConcurrency(1);
        p.setDefaultQueueConcurrency(1);
        p.setDefaultTenantConcurrency(1);
        ControllableTaskHandler tasks = new ControllableTaskHandler();
        SchedulerEngine e = engineWithTimeoutOverride(dir, p, tasks, java.util.Map.of(
                java.util.Map.entry("a", 1), 120L,
                java.util.Map.entry("a", 2), 30_000L,
                java.util.Map.entry("a", 3), 30_000L));
        e.start();
        try {
            submit(e, job("a", 30_000L, 3, 1000), noop("b"));
            tasks.awaitStarted("a", 1);
            // a 超时立即释放唯一名额，排队的 b 补位成功
            awaitState(e, "b", JobState.SUCCESS, Duration.ofSeconds(5));
            // b 释放后 a 的第 2 次重试拿到名额
            tasks.awaitStarted("a", 2);
            // 此时再提交 c：a 的第 2 次仍占着唯一名额，c 必须排队
            submit(e, noop("c"));
            Thread.sleep(100);
            assertEquals(JobState.QUEUED.name(), e.get("c").getState(),
                    "迟到结果不得提前释放名额，c 不能与 a 的第 2 次并发");

            // a 的第 1 次此刻才迟到成功：既不能摘掉第 2 次的运行账本，也不能让 c 提前启动
            tasks.release("a", 1);
            Thread.sleep(150);
            log.info("[名额唯一] 迟到结果后 a={}/{} c={}",
                    e.get("a").getState(), e.get("a").getAttempts(), e.get("c").getState());
            assertEquals(JobState.RUNNING.name(), e.get("a").getState());
            assertEquals(2, e.get("a").getAttempts());
            assertEquals(JobState.QUEUED.name(), e.get("c").getState());

            tasks.release("a", 2);
            awaitState(e, "a", JobState.SUCCESS, Duration.ofSeconds(5));
            awaitState(e, "c", JobState.SUCCESS, Duration.ofSeconds(5));
            printStates("名额不重复归还", e);
        } finally {
            e.stop();
        }
    }

    /**
     * 调度器停掉后不得再有后台活动推进状态：即便卡住的任务之后才结束，
     * 作业状态与执行记录都必须保持停机瞬间的样子。
     */
    @Test
    void stoppedEngineDoesNotProcessLateOutcome(@TempDir Path dir) throws Exception {
        ControllableTaskHandler tasks = new ControllableTaskHandler();
        SchedulerEngine e = engine(dir, props(dir), tasks);
        e.start();
        try {
            submit(e, job("z", 5000L, 2, 0));
            tasks.awaitStarted("z", 1);
            awaitState(e, "z", JobState.RUNNING, Duration.ofSeconds(5));
        } finally {
            e.stop();
        }
        // 停机后旧任务才“跑完”，结果无人采纳：状态停留 RUNNING，记录停留在 STARTED
        tasks.release("z", 1);
        Thread.sleep(300);
        log.info("[停机静默] 迟到结果后 state={} records={}",
                e.get("z").getState(),
                e.listAttempts("z").stream().map(r -> r.getResult().name()).toList());
        assertEquals(JobState.RUNNING.name(), e.get("z").getState(), "停机后不得再迁移状态");
        assertEquals(List.of(AttemptResult.STARTED),
                e.listAttempts("z").stream().map(AttemptRecord::getResult).toList(),
                "停机后不得再回填执行记录");
    }
}
