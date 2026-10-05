package com.github.highcumontoa.workflowschedulerjava.tests;

import com.github.highcumontoa.workflowschedulerjava.config.SchedulerProperties;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitBatchRequest;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobRequest;
import com.github.highcumontoa.workflowschedulerjava.engine.SchedulerEngine;
import com.github.highcumontoa.workflowschedulerjava.model.AttemptRecord;
import com.github.highcumontoa.workflowschedulerjava.model.AttemptResult;
import com.github.highcumontoa.workflowschedulerjava.model.JobState;
import com.github.highcumontoa.workflowschedulerjava.model.RetryPolicy;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 单作业执行超时：超时按本次失败处理、立即释放并发名额、走既有重试与下游传播。 */
class TimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(TimeoutTest.class);

    private static SubmitJobRequest job(String id, String type, long param) {
        SubmitJobRequest r = new SubmitJobRequest();
        r.setId(id);
        r.setTaskType(type);
        r.setTaskParam(param);
        return r;
    }

    private static void submit(SchedulerEngine e, SubmitJobRequest... jobs) {
        SubmitBatchRequest batch = new SubmitBatchRequest();
        batch.setJobs(List.of(jobs));
        e.submit(batch);
    }

    @Test
    void timeoutCountsAsFailureAndExhaustsRetry(@TempDir Path dir) {
        SchedulerEngine e = newEngine(dir, props(dir));
        e.start();
        try {
            SubmitJobRequest j = job("t1", "sleep", 3000);
            j.setTimeoutMillis(200L);
            j.setRetryPolicy(new RetryPolicy(2, 50));
            submit(e, j);
            var view = awaitState(e, "t1", JobState.FAILED, Duration.ofSeconds(10));
            printStates("超时耗尽重试", e);
            log.info("[超时判定] attempts={}/2 failure={}", view.getAttempts(), view.getFailureReason());
            assertEquals(2, view.getAttempts(), "每次超时都计一次尝试");
            assertTrue(view.getFailureReason().contains("TimeoutException"), "失败原因应标明超时");
            List<AttemptRecord> records = e.listAttempts("t1");
            assertEquals(2, records.size(), "两次超时各留一条记录");
            assertTrue(records.stream().allMatch(r -> r.getResult() == AttemptResult.TIMEOUT));
            assertTrue(records.stream().allMatch(r -> r.getFinishedAt() >= r.getStartedAt()));
        } finally {
            e.stop();
        }
    }

    @Test
    void timeoutReleasesSlotForQueuedJobImmediately(@TempDir Path dir) {
        SchedulerProperties p = props(dir);
        p.setGlobalConcurrency(1);
        p.setDefaultQueueConcurrency(1);
        p.setDefaultTenantConcurrency(1);
        SchedulerEngine e = newEngine(dir, p);
        e.start();
        try {
            SubmitJobRequest a = job("slow", "sleep", 5000);
            a.setTimeoutMillis(200L);
            SubmitJobRequest b = job("queued", "noop", 0);
            submit(e, a, b);
            long start = System.currentTimeMillis();
            awaitState(e, "queued", JobState.SUCCESS, Duration.ofSeconds(4));
            long elapsed = System.currentTimeMillis() - start;
            printStates("超时立即释放名额", e);
            log.info("[超时判定] 排队作业在 {}ms 内完成（远小于 slow 的 5000ms 睡眠）", elapsed);
            assertTrue(elapsed < 4000, "超时后名额应立即还给排队作业，而不是等任务自己跑完");
            awaitState(e, "slow", JobState.FAILED, Duration.ofSeconds(5));
        } finally {
            e.stop();
        }
    }

    @Test
    void timeoutRetryThenSuccessAndDownstreamRuns(@TempDir Path dir) {
        SchedulerEngine e = newEngine(dir, props(dir));
        e.start();
        try {
            SubmitJobRequest a = job("flaky-slow", "slowonce", 2000);
            a.setTimeoutMillis(200L);
            a.setRetryPolicy(new RetryPolicy(2, 50));
            SubmitJobRequest b = job("downstream", "noop", 0);
            b.setDependencies(List.of("flaky-slow"));
            submit(e, a, b);
            var va = awaitState(e, "flaky-slow", JobState.SUCCESS, Duration.ofSeconds(10));
            awaitState(e, "downstream", JobState.SUCCESS, Duration.ofSeconds(10));
            printStates("超时重试后成功", e);
            log.info("[超时判定] 第 1 次超时、第 2 次成功，下游照常执行 attempts={}", va.getAttempts());
            assertEquals(2, va.getAttempts());
            List<AttemptRecord> records = e.listAttempts("flaky-slow");
            assertEquals(AttemptResult.TIMEOUT, records.get(0).getResult());
            assertEquals(AttemptResult.SUCCESS, records.get(1).getResult());
        } finally {
            e.stop();
        }
    }

    @Test
    void queueWaitDoesNotConsumeTimeoutBudget(@TempDir Path dir) {
        SchedulerProperties p = props(dir);
        p.setGlobalConcurrency(1);
        p.setDefaultQueueConcurrency(1);
        p.setDefaultTenantConcurrency(1);
        SchedulerEngine e = newEngine(dir, p);
        e.start();
        try {
            // holder 占住唯一名额约 400ms；waiter 的超时 600ms 只从真正 RUNNING 起算
            SubmitJobRequest holder = job("holder", "sleep", 400);
            SubmitJobRequest waiter = job("waiter", "sleep", 200);
            waiter.setTimeoutMillis(600L);
            submit(e, holder, waiter);
            awaitState(e, "holder", JobState.SUCCESS, Duration.ofSeconds(5));
            var view = awaitState(e, "waiter", JobState.SUCCESS, Duration.ofSeconds(5));
            printStates("排队不消耗超时预算", e);
            log.info("[超时判定] waiter 排队约 400ms 后执行 200ms，总耗时超 600ms 但未超时");
            assertEquals(1, view.getAttempts());
        } finally {
            e.stop();
        }
    }

    @Test
    void cancelWinsOverTimeoutDeterministically(@TempDir Path dir) {
        SchedulerEngine e = newEngine(dir, props(dir));
        e.start();
        try {
            SubmitJobRequest j = job("cancel-me", "sleep", 3000);
            j.setTimeoutMillis(500L);
            submit(e, j);
            awaitState(e, "cancel-me", JobState.RUNNING, Duration.ofSeconds(5));
            e.cancel("cancel-me");
            var view = awaitState(e, "cancel-me", JobState.CANCELLED, Duration.ofSeconds(5));
            // 等超时点（500ms）过去之后，状态仍必须是 CANCELLED，不得被超时翻成 FAILED
            Thread.sleep(600);
            var after = e.get("cancel-me");
            printStates("取消优先于超时", e);
            log.info("[超时判定] 超时点过后 state={}（取消请求优先，结果确定）", after.getState());
            assertEquals(JobState.CANCELLED.name(), after.getState());
            assertEquals(JobState.CANCELLED.name(), view.getState());
            List<AttemptRecord> records = e.listAttempts("cancel-me");
            assertEquals(1, records.size());
            assertEquals(AttemptResult.CANCELLED, records.get(0).getResult());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            e.stop();
        }
    }

    @Test
    void jobWithoutTimeoutKeepsLegacyBehavior(@TempDir Path dir) {
        SchedulerEngine e = newEngine(dir, props(dir));
        e.start();
        try {
            submit(e, job("legacy", "sleep", 300));
            var view = awaitState(e, "legacy", JobState.SUCCESS, Duration.ofSeconds(5));
            log.info("[超时判定] 未配置超时的作业行为不变 state={} timeoutMillis={}",
                    view.getState(), view.getTimeoutMillis());
            assertEquals(JobState.SUCCESS.name(), view.getState());
            assertEquals(null, view.getTimeoutMillis());
        } finally {
            e.stop();
        }
    }
}
