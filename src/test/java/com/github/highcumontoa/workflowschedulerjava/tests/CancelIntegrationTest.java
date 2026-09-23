package com.github.highcumontoa.workflowschedulerjava.tests;

import com.github.highcumontoa.workflowschedulerjava.dto.JobResponse;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobRequest;
import com.github.highcumontoa.workflowschedulerjava.error.ErrorCode;
import com.github.highcumontoa.workflowschedulerjava.error.SchedulerException;
import com.github.highcumontoa.workflowschedulerjava.service.JobService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** 取消语义与级联不可达。 */
@SpringBootTest
class CancelIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(CancelIntegrationTest.class);

    @Autowired
    private JobService jobService;

    private SubmitJobRequest req(String id, String type, String payload, List<String> deps) {
        return new SubmitJobRequest(id, null, type, payload, deps,
                null, null, null, null, null);
    }

    @Test
    @DisplayName("取消排队中的作业：立即 CANCELLED，下游级联 UNREACHABLE，自身 attempts=0")
    void cancelQueuedCascades() throws Exception {
        // hold 队列上限=1：一个长 SLEEP 占满该队列，同队列的 victim 必然保持 QUEUED
        jobService.submit(new SubmitJobRequest("hold-c-0", null, "SLEEP", "6000",
                null, "hold", "hold-t", null, null, null));
        jobService.submit(new SubmitJobRequest("victim", null, "SLEEP", "6000",
                null, "hold", "hold-t", null, null, null));
        jobService.submit(req("victim-down", "NOOP", null, List.of("victim")));

        // 等待 victim 确定处于排队（配额满）后取消
        TestAwait.until(() -> "QUEUED".equals(jobService.get("victim").status()),
                3000, "victim 进入 QUEUED");
        JobResponse cancelled = jobService.cancel("victim");
        assertEquals("CANCELLED", cancelled.status());
        assertTrue(cancelled.failureReason().contains("用户取消"), cancelled.failureReason());
        assertEquals(0, cancelled.attempts(), "排队作业未执行过");

        JobResponse down = TestAwait.awaitStatus(jobService, "victim-down", "UNREACHABLE", 3000);
        assertEquals(0, down.attempts());
        assertTrue(down.failureReason().contains("victim"), down.failureReason());
        log.info("排队取消判定依据: victim={} attempts={}，victim-down={} 原因='{}'",
                cancelled.status(), cancelled.attempts(), down.status(), down.failureReason());
    }

    @Test
    @DisplayName("取消 PENDING 作业后，其下游沿链 UNREACHABLE")
    void cancelPendingCascades() throws Exception {
        jobService.submit(req("cp-up", "SLEEP", "2000", null));
        jobService.submit(req("cp-mid", "NOOP", null, List.of("cp-up")));
        jobService.submit(req("cp-leaf", "NOOP", null, List.of("cp-mid")));
        // cp-mid 此刻应仍是 PENDING
        TestAwait.until(() -> "PENDING".equals(jobService.get("cp-mid").status()),
                2000, "cp-mid 保持 PENDING");
        JobResponse c = jobService.cancel("cp-mid");
        assertEquals("CANCELLED", c.status());
        JobResponse leaf = TestAwait.awaitStatus(jobService, "cp-leaf", "UNREACHABLE", 3000);
        assertTrue(leaf.failureReason().contains("cp-mid"), leaf.failureReason());
        log.info("PENDING 取消判定依据: cp-leaf='{}'", leaf.failureReason());
    }

    @Test
    @DisplayName("对终态作业重复取消 -> 409 ALREADY_TERMINAL，原因可区分")
    void cancelTerminalRejected() throws Exception {
        jobService.submit(req("done-job", "NOOP", null, null));
        TestAwait.awaitStatus(jobService, "done-job", "SUCCESS", 5000);
        SchedulerException ex = assertThrows(SchedulerException.class,
                () -> jobService.cancel("done-job"));
        assertEquals(ErrorCode.ALREADY_TERMINAL, ex.getCode());
        assertTrue(ex.getMessage().contains("SUCCESS"));
        log.info("重复取消判定依据: code={} message='{}'", ex.getCode(), ex.getMessage());
    }

    @Test
    @DisplayName("取消不存在的作业 -> 404 JOB_NOT_FOUND")
    void cancelMissing() {
        SchedulerException ex = assertThrows(SchedulerException.class,
                () -> jobService.cancel("missing-job-xyz"));
        assertEquals(ErrorCode.JOB_NOT_FOUND, ex.getCode());
        log.info("取消不存在作业判定依据: code={}", ex.getCode());
    }

    @Test
    @DisplayName("执行中作业收到取消：执行体轮询标记后以 CANCELLED 收口（best-effort）")
    void cancelRunningIsBestEffort() throws Exception {
        // 独立队列/租户 + 更长等待，避免与其他用例残留的长 SLEEP 争抢全局配额
        SubmitJobRequest longJob = new SubmitJobRequest("long-run", null, "SLEEP", "5000",
                null, "q-cancel-running", "t-cancel-running", null, null, null);
        jobService.submit(longJob);
        TestAwait.until(() -> "RUNNING".equals(jobService.get("long-run").status()),
                8000, "long-run 进入 RUNNING");
        JobResponse ack = jobService.cancel("long-run");
        // RUNNING 期间取消返回体仍可能是 RUNNING，但终态必须是 CANCELLED
        assertNotNull(ack.status());
        JobResponse terminal = TestAwait.awaitStatus(jobService, "long-run", "CANCELLED", 5000);
        assertTrue(terminal.failureReason().contains("取消"), terminal.failureReason());
        log.info("执行中取消判定依据: 终态={} 原因='{}'", terminal.status(), terminal.failureReason());
    }
}
