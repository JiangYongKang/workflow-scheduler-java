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

/** 进程重启恢复：依据持久化状态继续推进；SUCCESS 不重复执行，可恢复作业不被跳过。 */
class RecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(RecoveryTest.class);

    @Test
    void completedJobsSurviveRestartAndNeverReRun(@TempDir Path dir) {
        SchedulerEngine e1 = newEngine(dir, props(dir));
        e1.start();
        try {
            SubmitBatchRequest batch = new SubmitBatchRequest();
            SubmitJobRequest a = new SubmitJobRequest();
            a.setId("done");
            a.setTaskType("noop");
            batch.setJobs(List.of(a));
            e1.submit(batch);
            awaitState(e1, "done", JobState.SUCCESS, Duration.ofSeconds(5));
            log.info("[恢复准备] job=done 已 SUCCESS，停止引擎模拟进程退出");
        } finally {
            e1.stop();
        }

        // 同目录重建引擎 = 进程重启
        SchedulerEngine e2 = newEngine(dir, props(dir));
        e2.start();
        try {
            Thread.sleep(200);
            var view = e2.get("done");
            log.info("[恢复判定] job=done 重启后 state={} attempts={}（必须保持 SUCCESS 且不增加尝试次数）",
                    view.getState(), view.getAttempts());
            assertEquals(JobState.SUCCESS.name(), view.getState());
            assertEquals(1, view.getAttempts(), "已成功作业重启后不得重复执行");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            e2.stop();
        }
    }

    @Test
    void queuedAndDependentJobsContinueAfterRestart(@TempDir Path dir) {
        SchedulerProperties p = props(dir);
        p.setGlobalConcurrency(1);
        p.setDefaultQueueConcurrency(1);
        p.setDefaultTenantConcurrency(1);

        SchedulerEngine e1 = newEngine(dir, p);
        e1.start();
        try {
            SubmitBatchRequest batch = new SubmitBatchRequest();
            SubmitJobRequest head = new SubmitJobRequest();
            head.setId("head");
            head.setTaskType("sleep");
            head.setTaskParam(400L);
            SubmitJobRequest tail = new SubmitJobRequest();
            tail.setId("tail");
            tail.setTaskType("noop");
            tail.setDependencies(List.of("head"));
            batch.setJobs(List.of(head, tail));
            e1.submit(batch);
            awaitState(e1, "head", JobState.RUNNING, Duration.ofSeconds(5));
            log.info("[恢复准备] head RUNNING、tail 等待，停止引擎模拟崩溃");
        } finally {
            e1.stop();
        }

        SchedulerEngine e2 = newEngine(dir, p);
        e2.start();
        try {
            // head 崩溃时 RUNNING -> 回置 PENDING 重新执行（at-least-once）；tail 必须最终被执行而非跳过
            var head = awaitState(e2, "head", JobState.SUCCESS, Duration.ofSeconds(10));
            var tail = awaitState(e2, "tail", JobState.SUCCESS, Duration.ofSeconds(10));
            printStates("重启恢复后继续推进", e2);
            log.info("[恢复判定] head attempts={}（崩溃中断后重新执行） tail attempts={}（未被跳过）",
                    head.getAttempts(), tail.getAttempts());
            assertEquals(1, tail.getAttempts(), "可恢复作业不得被跳过");
            assertTrue(head.getAttempts() >= 1);
        } finally {
            e2.stop();
        }
    }

    @Test
    void failedAttemptsAreCountedAcrossRestart(@TempDir Path dir) {
        // flaky 前 2 次失败、第 3 次成功；第 1 次尝试在“崩溃”中完成，恢复后只能再试 maxAttempts-1 次
        SchedulerProperties p = props(dir);
        p.setDefaultMaxAttempts(3);
        SchedulerEngine e1 = newEngine(dir, p);
        e1.start();
        try {
            SubmitBatchRequest batch = new SubmitBatchRequest();
            SubmitJobRequest f = new SubmitJobRequest();
            f.setId("flaky1");
            f.setTaskType("flaky");
            f.setTaskParam(5L); // 总是失败，直到预算耗尽
            f.setRetryPolicy(new RetryPolicy(2, 0));
            batch.setJobs(List.of(f));
            e1.submit(batch);
            awaitState(e1, "flaky1", JobState.FAILED, Duration.ofSeconds(10));
        } finally {
            e1.stop();
        }
        // 此例验证 FAILED 终态在重启后保持稳定、不被重新调度
        SchedulerEngine e2 = newEngine(dir, p);
        e2.start();
        try {
            Thread.sleep(150);
            var view = e2.get("flaky1");
            log.info("[恢复判定] job=flaky1 重启后 state={} attempts={}/2（FAILED 终态稳定）",
                    view.getState(), view.getAttempts());
            assertEquals(JobState.FAILED.name(), view.getState());
            assertEquals(2, view.getAttempts());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            e2.stop();
        }
    }
}
