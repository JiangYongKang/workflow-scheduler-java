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

/** 暂停 / 恢复：暂停不启新执行、运行中自然跑完、排队不丢；恢复后按依赖与配额继续。 */
class PauseResumeTest {

    private static final Logger log = LoggerFactory.getLogger(PauseResumeTest.class);

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
    void pauseBlocksNewLaunchesWhileRunningFinishesNaturally(@TempDir Path dir) throws Exception {
        SchedulerProperties p = props(dir);
        p.setGlobalConcurrency(1);
        p.setDefaultQueueConcurrency(1);
        p.setDefaultTenantConcurrency(1);
        SchedulerEngine e = newEngine(dir, p);
        e.start();
        try {
            submit(e, job("running", "sleep", 400), job("waiting", "noop", 0));
            awaitState(e, "running", JobState.RUNNING, Duration.ofSeconds(5));
            e.pause();
            assertTrue(e.isPaused());
            // 运行中的作业自然跑完
            awaitState(e, "running", JobState.SUCCESS, Duration.ofSeconds(5));
            // 暂停期间名额已空，但 waiting 不得被启动
            Thread.sleep(300);
            var waiting = e.get("waiting");
            printStates("暂停中：运行完成、排队保留", e);
            log.info("[暂停判定] waiting state={} attempts={}（暂停期间不得启动新执行）",
                    waiting.getState(), waiting.getAttempts());
            assertEquals(0, waiting.getAttempts());
            assertTrue(waiting.getState().equals(JobState.PENDING.name())
                    || waiting.getState().equals(JobState.QUEUED.name()));
            // 恢复后继续推进
            e.resume();
            assertFalse(e.isPaused());
            awaitState(e, "waiting", JobState.SUCCESS, Duration.ofSeconds(5));
        } finally {
            e.stop();
        }
    }

    @Test
    void queuedJobsAreNeitherDroppedNorFailedDuringPause(@TempDir Path dir) throws Exception {
        SchedulerEngine e = newEngine(dir, props(dir));
        e.start();
        try {
            e.pause();
            submit(e, job("p1", "noop", 0), job("p2", "noop", 0));
            Thread.sleep(300);
            var v1 = e.get("p1");
            var v2 = e.get("p2");
            log.info("[暂停判定] 暂停中提交 p1={} p2={}（不得执行、不得失败）", v1.getState(), v2.getState());
            assertEquals(0, v1.getAttempts());
            assertEquals(0, v2.getAttempts());
            e.resume();
            awaitState(e, "p1", JobState.SUCCESS, Duration.ofSeconds(5));
            awaitState(e, "p2", JobState.SUCCESS, Duration.ofSeconds(5));
            printStates("恢复后继续推进", e);
        } finally {
            e.stop();
        }
    }

    @Test
    void pauseStateSurvivesRestart(@TempDir Path dir) throws Exception {
        SchedulerProperties p = props(dir);
        SchedulerEngine e1 = newEngine(dir, p);
        e1.start();
        try {
            e1.pause();
            submit(e1, job("held", "noop", 0));
            Thread.sleep(200);
            assertEquals(0, e1.get("held").getAttempts());
            log.info("[暂停判定] 暂停中停止引擎，模拟进程退出");
        } finally {
            e1.stop();
        }

        SchedulerEngine e2 = newEngine(dir, p);
        e2.start();
        try {
            // 重启后仍是暂停：作业不得被启动，也不得卡死（恢复后可继续）
            assertTrue(e2.isPaused(), "暂停开关必须随持久化恢复");
            Thread.sleep(300);
            var held = e2.get("held");
            log.info("[暂停判定] 重启后 held state={} attempts={}（保持暂停不执行）",
                    held.getState(), held.getAttempts());
            assertEquals(0, held.getAttempts());
            e2.resume();
            awaitState(e2, "held", JobState.SUCCESS, Duration.ofSeconds(5));
            printStates("重启后恢复继续", e2);
        } finally {
            e2.stop();
        }
    }

    @Test
    void dependenciesStillHonoredAfterResume(@TempDir Path dir) {
        SchedulerEngine e = newEngine(dir, props(dir));
        e.start();
        try {
            e.pause();
            SubmitJobRequest a = job("up", "sleep", 200);
            SubmitJobRequest b = job("down", "noop", 0);
            b.setDependencies(List.of("up"));
            submit(e, a, b);
            e.resume();
            awaitState(e, "up", JobState.SUCCESS, Duration.ofSeconds(5));
            var vb = awaitState(e, "down", JobState.SUCCESS, Duration.ofSeconds(5));
            printStates("恢复后依赖顺序不变", e);
            log.info("[暂停判定] down 在 up 成功后执行 attempts={}", vb.getAttempts());
            assertEquals(1, vb.getAttempts());
        } finally {
            e.stop();
        }
    }
}
