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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 暂停/恢复：暂停期间不启动新执行、运行中作业自然完成、排队作业保留；恢复后继续推进。 */
class PauseResumeTest {

    private static final Logger log = LoggerFactory.getLogger(PauseResumeTest.class);

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
    void pausedSchedulerDoesNotStartNewExecutions(@TempDir Path dir) throws InterruptedException {
        SchedulerEngine e = newEngine(dir, props(dir));
        e.start();
        try {
            e.pause();
            assertTrue(e.isPaused());
            e.submit(batch(job("held", "noop", 0)));
            // 暂停期间作业不得进入 RUNNING，也不得失败/丢弃
            Thread.sleep(300);
            var view = e.get("held");
            assertEquals(JobState.PENDING.name(), view.getState(), "暂停期间不得启动新执行");
            assertEquals(0, view.getAttempts(), "暂停期间不得消耗尝试次数");
            printStates("暂停期间作业保持等待", e);

            e.resume();
            assertFalse(e.isPaused());
            var done = awaitState(e, "held", JobState.SUCCESS, Duration.ofSeconds(5));
            assertEquals(1, done.getAttempts());
            log.info("[暂停判定] 暂停期间未执行，恢复后正常完成 attempts={}", done.getAttempts());
        } finally {
            e.stop();
        }
    }

    @Test
    void runningJobFinishesNaturallyWhileQueuedJobWaitsForResume(@TempDir Path dir) throws InterruptedException {
        SchedulerProperties p = props(dir);
        p.setGlobalConcurrency(1);
        p.setDefaultQueueConcurrency(1);
        p.setDefaultTenantConcurrency(1);
        SchedulerEngine e = newEngine(dir, p);
        e.start();
        try {
            e.submit(batch(job("running", "sleep", 400), job("queued", "noop", 0)));
            awaitState(e, "running", JobState.RUNNING, Duration.ofSeconds(5));
            e.pause();

            // 运行中的作业自然跑完；排队的作业在暂停期间不得启动、不得失败
            awaitState(e, "running", JobState.SUCCESS, Duration.ofSeconds(5));
            Thread.sleep(200);
            var queued = e.get("queued");
            assertTrue(queued.getState().equals(JobState.PENDING.name())
                            || queued.getState().equals(JobState.QUEUED.name()),
                    "暂停期间排队作业应保持等待，实际: " + queued.getState());
            assertEquals(0, queued.getAttempts(), "暂停期间排队作业不得被执行");
            printStates("暂停中：运行作业完成，排队作业等待", e);

            e.resume();
            var done = awaitState(e, "queued", JobState.SUCCESS, Duration.ofSeconds(5));
            assertEquals(1, done.getAttempts());
            log.info("[暂停判定] running 自然完成，queued 暂停期间未动，恢复后执行成功");
        } finally {
            e.stop();
        }
    }

    @Test
    void pauseAndResumeAreIdempotent(@TempDir Path dir) {
        SchedulerEngine e = newEngine(dir, props(dir));
        e.start();
        try {
            e.resume(); // 未暂停时恢复是空操作
            assertFalse(e.isPaused());
            e.pause();
            e.pause(); // 重复暂停是空操作
            assertTrue(e.isPaused());
            e.resume();
            e.resume();
            assertFalse(e.isPaused());
            e.submit(batch(job("after", "noop", 0)));
            awaitState(e, "after", JobState.SUCCESS, Duration.ofSeconds(5));
            log.info("[暂停判定] 暂停/恢复幂等，恢复后调度正常");
        } finally {
            e.stop();
        }
    }

    @Test
    void pausedStateSurvivesEngineRestart(@TempDir Path dir) throws InterruptedException {
        SchedulerProperties p = props(dir);
        SchedulerEngine e1 = newEngine(dir, p);
        e1.start();
        try {
            e1.pause();
            e1.submit(batch(job("held-restart", "noop", 0)));
            Thread.sleep(200);
            assertEquals(0, e1.get("held-restart").getAttempts());
        } finally {
            e1.stop();
        }

        // 同目录重建引擎 = 进程重启：暂停状态必须保持，作业不得被偷跑
        SchedulerEngine e2 = newEngine(dir, p);
        e2.start();
        try {
            assertTrue(e2.isPaused(), "重启后暂停状态必须保持");
            Thread.sleep(300);
            var held = e2.get("held-restart");
            assertEquals(0, held.getAttempts(), "暂停期间（含重启后）不得启动新执行");
            printStates("重启后暂停保持", e2);

            e2.resume();
            var done = awaitState(e2, "held-restart", JobState.SUCCESS, Duration.ofSeconds(5));
            assertEquals(1, done.getAttempts(), "恢复后作业应恰好执行一次");
            log.info("[暂停判定] 重启后暂停保持，恢复后作业恰好执行一次");
        } finally {
            e2.stop();
        }
    }
}
