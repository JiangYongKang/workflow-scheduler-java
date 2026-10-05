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
import static com.github.highcumontoa.workflowschedulerjava.tests.EngineTestSupport.props;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 执行记录：每次执行一条、按序可查、重启不丢、不多不少。 */
class AttemptRecordTest {

    private static final Logger log = LoggerFactory.getLogger(AttemptRecordTest.class);

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
    void eachAttemptLeavesOneOrderedRecord(@TempDir Path dir) {
        SchedulerProperties p = props(dir);
        SchedulerEngine e = newEngine(dir, p);
        e.start();
        try {
            SubmitJobRequest j = job("flaky", "flaky", 2);
            j.setRetryPolicy(new RetryPolicy(3, 30));
            submit(e, j);
            awaitState(e, "flaky", JobState.SUCCESS, Duration.ofSeconds(10));
            List<AttemptRecord> records = e.listAttempts("flaky");
            log.info("[记录判定] 3 次尝试 {} 条记录，结果依次为 {}",
                    records.size(), records.stream().map(r -> r.getResult().name()).toList());
            assertEquals(3, records.size(), "每次执行恰好一条记录");
            for (int i = 0; i < 3; i++) {
                assertEquals(i + 1, records.get(i).getAttempt(), "记录按尝试序号升序");
                assertTrue(records.get(i).getStartedAt() > 0);
                assertTrue(records.get(i).getFinishedAt() >= records.get(i).getStartedAt());
            }
            assertEquals(AttemptResult.FAILED, records.get(0).getResult());
            assertEquals(AttemptResult.FAILED, records.get(1).getResult());
            assertEquals(AttemptResult.SUCCESS, records.get(2).getResult());
            assertNotNull(records.get(0).getError(), "失败记录必须带失败原因");
            assertTrue(records.get(0).getError().contains("模拟失败"));
        } finally {
            e.stop();
        }
    }

    @Test
    void cancelledExecutionLeavesCancelledRecord(@TempDir Path dir) {
        SchedulerEngine e = newEngine(dir, props(dir));
        e.start();
        try {
            submit(e, job("to-cancel", "sleep", 3000));
            awaitState(e, "to-cancel", JobState.RUNNING, Duration.ofSeconds(5));
            e.cancel("to-cancel");
            awaitState(e, "to-cancel", JobState.CANCELLED, Duration.ofSeconds(5));
            List<AttemptRecord> records = e.listAttempts("to-cancel");
            log.info("[记录判定] 取消的执行留 {} 条记录 result={}", records.size(),
                    records.isEmpty() ? "无" : records.get(0).getResult());
            assertEquals(1, records.size());
            assertEquals(AttemptResult.CANCELLED, records.get(0).getResult());
        } finally {
            e.stop();
        }
    }

    @Test
    void neverExecutedJobHasNoRecords(@TempDir Path dir) {
        SchedulerEngine e = newEngine(dir, props(dir));
        e.start();
        try {
            // 先暂停，保证 never-run 在取消前绝无机会进入 RUNNING
            e.pause();
            SubmitJobRequest bad = job("bad", "flaky", 9);
            SubmitJobRequest blocked = job("blocked", "noop", 0);
            blocked.setDependencies(List.of("bad"));
            SubmitJobRequest cancelledBeforeRun = job("never-run", "sleep", 5000);
            submit(e, bad, blocked, cancelledBeforeRun);
            e.cancel("never-run");
            awaitState(e, "never-run", JobState.CANCELLED, Duration.ofSeconds(5));
            e.resume();
            awaitState(e, "bad", JobState.FAILED, Duration.ofSeconds(10));
            awaitState(e, "blocked", JobState.UNREACHABLE, Duration.ofSeconds(10));
            log.info("[记录判定] 未执行的作业 blocked={} 条、never-run={} 条记录",
                    e.listAttempts("blocked").size(), e.listAttempts("never-run").size());
            assertEquals(0, e.listAttempts("blocked").size(), "UNREACHABLE 作业未执行，不应有记录");
            assertEquals(0, e.listAttempts("never-run").size(), "执行前被取消，不应有记录");
        } finally {
            e.stop();
        }
    }

    @Test
    void recordsSurviveRestartAndInterruptedAttemptIsClosed(@TempDir Path dir) {
        SchedulerProperties p = props(dir);
        SchedulerEngine e1 = newEngine(dir, p);
        e1.start();
        try {
            SubmitJobRequest done = job("done", "noop", 0);
            done.setIdempotencyKey("k-done");
            SubmitJobRequest running = job("was-running", "sleep", 3000);
            submit(e1, done, running);
            awaitState(e1, "done", JobState.SUCCESS, Duration.ofSeconds(5));
            awaitState(e1, "was-running", JobState.RUNNING, Duration.ofSeconds(5));
            log.info("[记录判定] done 成功、was-running 执行中，停止引擎模拟进程退出");
        } finally {
            e1.stop();
        }

        SchedulerEngine e2 = newEngine(dir, p);
        e2.start();
        try {
            // 重启不丢：done 的记录仍是 1 条；幂等重复提交不多出记录
            SubmitJobRequest dup = job("done", "noop", 0);
            dup.setIdempotencyKey("k-done");
            submit(e2, dup);
            Thread.sleep(200);
            List<AttemptRecord> doneRecords = e2.listAttempts("done");
            log.info("[记录判定] 重启+重复提交后 done 记录数={}（必须仍为 1）", doneRecords.size());
            assertEquals(1, doneRecords.size());
            assertEquals(AttemptResult.SUCCESS, doneRecords.get(0).getResult());

            // 崩溃中断的那次执行被收尾为 INTERRUPTED，重新执行追加新记录
            var view = awaitState(e2, "was-running", JobState.RUNNING, Duration.ofSeconds(5));
            List<AttemptRecord> running = e2.listAttempts("was-running");
            log.info("[记录判定] was-running 记录数={} 首条结果={}（中断收尾）", running.size(), running.get(0).getResult());
            assertEquals(2, running.size(), "中断 1 条 + 重新执行 1 条");
            assertEquals(AttemptResult.INTERRUPTED, running.get(0).getResult());
            assertEquals(AttemptResult.STARTED, running.get(1).getResult());
            e2.cancel("was-running");
            awaitState(e2, "was-running", JobState.CANCELLED, Duration.ofSeconds(5));
            List<AttemptRecord> finished = e2.listAttempts("was-running");
            assertEquals(2, finished.size(), "取消不多不少仍是 2 条");
            assertEquals(AttemptResult.CANCELLED, finished.get(1).getResult());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            e2.stop();
        }
    }
}
