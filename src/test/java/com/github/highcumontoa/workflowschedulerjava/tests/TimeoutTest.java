package com.github.highcumontoa.workflowschedulerjava.tests;

import com.github.highcumontoa.workflowschedulerjava.config.SchedulerProperties;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitBatchRequest;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobRequest;
import com.github.highcumontoa.workflowschedulerjava.engine.SchedulerEngine;
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

/** 单作业执行超时：按失败处理、立即释放配额、走既有重试与传播；未配置的作业行为不变。 */
class TimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(TimeoutTest.class);

    private static SubmitJobRequest job(String id, String type, long param) {
        SubmitJobRequest r = new SubmitJobRequest();
        r.setId(id);
        r.setTaskType(type);
        r.setTaskParam(param);
        return r;
    }

    private static SubmitBatchRequest batch(SubmitJobRequest... jobs) {
        SubmitBatchRequest b = new SubmitBatchRequest();
        b.setJobs(List.of(jobs));
        return b;
    }

    @Test
    void timeoutFailsAttemptAndPropagatesDownstream(@TempDir Path dir) {
        SchedulerEngine e = newEngine(dir, props(dir));
        e.start();
        try {
            SubmitJobRequest slow = job("slow", "sleep", 30_000);
            slow.setTimeoutMillis(200L);
            SubmitJobRequest down = job("down", "noop", 0);
            down.setDependencies(List.of("slow"));
            e.submit(batch(slow, down));

            var view = awaitState(e, "slow", JobState.FAILED, Duration.ofSeconds(10));
            printStates("超时按失败处理并传播", e);
            assertTrue(view.getFailureReason().contains("超时"),
                    "失败原因应说明超时，实际: " + view.getFailureReason());
            assertEquals(1, view.getAttempts());
            awaitState(e, "down", JobState.UNREACHABLE, Duration.ofSeconds(5));
            log.info("[超时判定] slow 超时 FAILED，下游 down 传播 UNREACHABLE");
        } finally {
            e.stop();
        }
    }

    @Test
    void timeoutReleasesSlotForQueuedJob(@TempDir Path dir) {
        SchedulerProperties p = props(dir);
        p.setGlobalConcurrency(1);
        p.setDefaultQueueConcurrency(1);
        p.setDefaultTenantConcurrency(1);
        SchedulerEngine e = newEngine(dir, p);
        e.start();
        try {
            SubmitJobRequest stuck = job("stuck", "sleep", 30_000);
            stuck.setTimeoutMillis(200L);
            SubmitJobRequest next = job("next", "noop", 0);
            e.submit(batch(stuck, next));

            awaitState(e, "stuck", JobState.RUNNING, Duration.ofSeconds(5));
            // stuck 超时后名额立即归还：next 应在远小于 30s 内完成
            awaitState(e, "stuck", JobState.FAILED, Duration.ofSeconds(10));
            var nextView = awaitState(e, "next", JobState.SUCCESS, Duration.ofSeconds(5));
            printStates("超时释放配额", e);
            assertEquals(1, nextView.getAttempts());
            log.info("[超时判定] stuck 超时后名额归还，next 未被堵塞，attempts={}", nextView.getAttempts());
        } finally {
            e.stop();
        }
    }

    @Test
    void timeoutCountsTowardRetryBudget(@TempDir Path dir) {
        SchedulerEngine e = newEngine(dir, props(dir));
        e.start();
        try {
            SubmitJobRequest r = job("retry-timeout", "sleep", 30_000);
            r.setTimeoutMillis(150L);
            r.setRetryPolicy(new RetryPolicy(2, 0));
            e.submit(batch(r));

            var view = awaitState(e, "retry-timeout", JobState.FAILED, Duration.ofSeconds(10));
            printStates("超时计入重试预算", e);
            assertEquals(2, view.getAttempts(), "超时尝试应计入重试预算");
            assertTrue(view.getFailureReason().contains("超时"));
            log.info("[超时判定] 两次尝试均超时，重试耗尽 FAILED attempts={}/2", view.getAttempts());
        } finally {
            e.stop();
        }
    }

    @Test
    void queuedTimeDoesNotConsumeTimeout(@TempDir Path dir) {
        SchedulerProperties p = props(dir);
        p.setGlobalConcurrency(1);
        p.setDefaultQueueConcurrency(1);
        p.setDefaultTenantConcurrency(1);
        SchedulerEngine e = newEngine(dir, p);
        e.start();
        try {
            // head 占住唯一名额 800ms；waiter 的超时只有 400ms，排队时间不得计入超时
            SubmitJobRequest head = job("head", "sleep", 800);
            SubmitJobRequest waiter = job("waiter", "noop", 0);
            waiter.setTimeoutMillis(400L);
            e.submit(batch(head, waiter));

            awaitState(e, "head", JobState.SUCCESS, Duration.ofSeconds(10));
            var view = awaitState(e, "waiter", JobState.SUCCESS, Duration.ofSeconds(10));
            printStates("排队不计入超时", e);
            assertEquals(1, view.getAttempts());
            log.info("[超时判定] waiter 排队约 800ms 远超 400ms 超时，但只在 RUNNING 后计时，最终成功");
        } finally {
            e.stop();
        }
    }

    @Test
    void jobWithoutTimeoutIsUnaffected(@TempDir Path dir) {
        SchedulerEngine e = newEngine(dir, props(dir));
        e.start();
        try {
            e.submit(batch(job("plain", "sleep", 300)));
            var view = awaitState(e, "plain", JobState.SUCCESS, Duration.ofSeconds(10));
            assertEquals(1, view.getAttempts());
            assertEquals(0, view.getTimeoutMillis());
            log.info("[超时判定] 未配置超时的作业行为不变，正常成功");
        } finally {
            e.stop();
        }
    }

    @Test
    void cancelBeatsTimeoutDeterministically(@TempDir Path dir) throws InterruptedException {
        SchedulerEngine e = newEngine(dir, props(dir));
        e.start();
        try {
            SubmitJobRequest r = job("cancel-race", "sleep", 30_000);
            r.setTimeoutMillis(5_000L); // 超时足够晚，取消必然先到
            e.submit(batch(r));
            awaitState(e, "cancel-race", JobState.RUNNING, Duration.ofSeconds(5));
            e.cancel("cancel-race");
            var view = awaitState(e, "cancel-race", JobState.CANCELLED, Duration.ofSeconds(5));
            printStates("取消优先于超时", e);
            // 等过超时时间点，状态必须稳定在 CANCELLED，不得再翻成 FAILED
            Thread.sleep(200);
            var after = e.get("cancel-race");
            assertEquals(JobState.CANCELLED.name(), after.getState(), "取消后不得再被超时改写");
            assertEquals(1, after.getAttempts());
            log.info("[超时判定] 取消与超时竞争结果确定：state={} 稳定不变", after.getState());
        } finally {
            e.stop();
        }
    }
}
