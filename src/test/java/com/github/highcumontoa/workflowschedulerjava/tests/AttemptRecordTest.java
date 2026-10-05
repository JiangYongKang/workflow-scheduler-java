package com.github.highcumontoa.workflowschedulerjava.tests;

import com.github.highcumontoa.workflowschedulerjava.dto.SubmitBatchRequest;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobRequest;
import com.github.highcumontoa.workflowschedulerjava.engine.SchedulerEngine;
import com.github.highcumontoa.workflowschedulerjava.exception.SchedulerException;
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
import static com.github.highcumontoa.workflowschedulerjava.tests.EngineTestSupport.props;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 执行记录：每次尝试留一条（开始/结束/结果/失败原因），按序可查，重启不丢不重。 */
class AttemptRecordTest {

    private static final Logger log = LoggerFactory.getLogger(AttemptRecordTest.class);

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
    void successfulJobLeavesOneSuccessRecord(@TempDir Path dir) {
        SchedulerEngine e = newEngine(dir, props(dir));
        e.start();
        try {
            e.submit(batch(job("ok", "sleep", 50)));
            awaitState(e, "ok", JobState.SUCCESS, Duration.ofSeconds(5));
            List<AttemptRecord> records = e.attempts("ok");
            assertEquals(1, records.size(), "成功一次应只有一条记录");
            AttemptRecord r = records.get(0);
            assertEquals(1, r.getAttempt());
            assertEquals(AttemptResult.SUCCESS, r.getResult());
            assertTrue(r.getStartedAt() > 0 && r.getFinishedAt() >= r.getStartedAt(),
                    "记录应含有效的开始/结束时间");
            assertNull(r.getError());
            log.info("[记录判定] 成功记录: attempt={} startedAt={} finishedAt={}",
                    r.getAttempt(), r.getStartedAt(), r.getFinishedAt());
        } finally {
            e.stop();
        }
    }

    @Test
    void retriesLeaveOrderedRecordsWithFailureReasons(@TempDir Path dir) {
        SchedulerEngine e = newEngine(dir, props(dir));
        e.start();
        try {
            SubmitJobRequest f = job("flaky-rec", "flaky", 2); // 前 2 次失败，第 3 次成功
            f.setRetryPolicy(new RetryPolicy(3, 0));
            e.submit(batch(f));
            awaitState(e, "flaky-rec", JobState.SUCCESS, Duration.ofSeconds(10));

            List<AttemptRecord> records = e.attempts("flaky-rec");
            assertEquals(3, records.size(), "每次尝试一条记录");
            for (int i = 0; i < 3; i++) {
                assertEquals(i + 1, records.get(i).getAttempt(), "记录应按尝试序号升序");
            }
            assertEquals(AttemptResult.FAILED, records.get(0).getResult());
            assertEquals(AttemptResult.FAILED, records.get(1).getResult());
            assertEquals(AttemptResult.SUCCESS, records.get(2).getResult());
            assertNotNull(records.get(0).getError(), "失败记录应含失败原因");
            assertTrue(records.get(0).getError().contains("模拟失败"));
            assertNull(records.get(2).getError());
            log.info("[记录判定] 重试留痕: {} 条，前两次失败原因={}", records.size(), records.get(0).getError());
        } finally {
            e.stop();
        }
    }

    @Test
    void timeoutAndCancelAreRecorded(@TempDir Path dir) {
        SchedulerEngine e = newEngine(dir, props(dir));
        e.start();
        try {
            SubmitJobRequest t = job("t-out", "sleep", 30_000);
            t.setTimeoutMillis(150L);
            e.submit(batch(t));
            awaitState(e, "t-out", JobState.FAILED, Duration.ofSeconds(10));
            List<AttemptRecord> timeoutRecords = e.attempts("t-out");
            assertEquals(1, timeoutRecords.size());
            assertEquals(AttemptResult.FAILED, timeoutRecords.get(0).getResult());
            assertTrue(timeoutRecords.get(0).getError().contains("超时"),
                    "超时记录应含超时原因，实际: " + timeoutRecords.get(0).getError());

            e.submit(batch(job("to-cancel", "sleep", 30_000)));
            awaitState(e, "to-cancel", JobState.RUNNING, Duration.ofSeconds(5));
            e.cancel("to-cancel");
            awaitState(e, "to-cancel", JobState.CANCELLED, Duration.ofSeconds(5));
            List<AttemptRecord> cancelRecords = e.attempts("to-cancel");
            assertEquals(1, cancelRecords.size());
            assertEquals(AttemptResult.CANCELLED, cancelRecords.get(0).getResult());
            log.info("[记录判定] 超时记录 error={}；取消记录 result={}",
                    timeoutRecords.get(0).getError(), cancelRecords.get(0).getResult());
        } finally {
            e.stop();
        }
    }

    @Test
    void recordsSurviveRestartWithoutDuplication(@TempDir Path dir) throws InterruptedException {
        SchedulerEngine e1 = newEngine(dir, props(dir));
        e1.start();
        try {
            SubmitJobRequest f = job("restart-rec", "flaky", 1); // 第 1 次失败，第 2 次成功
            f.setRetryPolicy(new RetryPolicy(2, 10_000)); // 长退避：让第 2 次尝试发生在“重启”之后
            e1.submit(batch(f));
            // 等第 1 次尝试失败落账（attempts=1 且回到 PENDING 等退避）
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline) {
                var v = e1.get("restart-rec");
                if (v.getAttempts() == 1 && v.getState().equals(JobState.PENDING.name())) {
                    break;
                }
                Thread.sleep(10);
            }
            assertEquals(1, e1.attempts("restart-rec").size());
        } finally {
            e1.stop();
        }

        SchedulerEngine e2 = newEngine(dir, props(dir));
        e2.start();
        try {
            awaitState(e2, "restart-rec", JobState.SUCCESS, Duration.ofSeconds(20));
            List<AttemptRecord> records = e2.attempts("restart-rec");
            assertEquals(2, records.size(), "重启后记录不丢不重：失败 1 条 + 成功 1 条");
            assertEquals(AttemptResult.FAILED, records.get(0).getResult());
            assertEquals(AttemptResult.SUCCESS, records.get(1).getResult());
            assertEquals(1, records.get(0).getAttempt());
            assertEquals(2, records.get(1).getAttempt());
            log.info("[记录判定] 重启后记录 {} 条，有序且真实", records.size());
        } finally {
            e2.stop();
        }
    }

    @Test
    void interruptedAttemptIsRecordedOnRecovery(@TempDir Path dir) {
        SchedulerEngine e1 = newEngine(dir, props(dir));
        e1.start();
        try {
            e1.submit(batch(job("crash-run", "sleep", 2_000)));
            awaitState(e1, "crash-run", JobState.RUNNING, Duration.ofSeconds(5));
            // 不等待执行结束直接停引擎，模拟进程退出时 RUNNING 被中断
        } finally {
            e1.stop();
        }

        SchedulerEngine e2 = newEngine(dir, props(dir));
        e2.start();
        try {
            awaitState(e2, "crash-run", JobState.SUCCESS, Duration.ofSeconds(10));
            List<AttemptRecord> records = e2.attempts("crash-run");
            assertEquals(2, records.size(), "被中断的尝试补一条记录 + 重跑一条记录");
            assertEquals(AttemptResult.CANCELLED, records.get(0).getResult());
            assertTrue(records.get(0).getError().contains("重启"));
            assertEquals(AttemptResult.SUCCESS, records.get(1).getResult());
            log.info("[记录判定] 中断尝试已补记: {} -> {}", records.get(0).getResult(), records.get(1).getResult());
        } finally {
            e2.stop();
        }
    }

    @Test
    void attemptsOfUnknownJobRejected(@TempDir Path dir) {
        SchedulerEngine e = newEngine(dir, props(dir));
        e.start();
        try {
            SchedulerException ex = assertThrows(SchedulerException.class, () -> e.attempts("nope"));
            assertEquals("JOB_NOT_FOUND", ex.getErrorCode().code());
        } finally {
            e.stop();
        }
    }
}
