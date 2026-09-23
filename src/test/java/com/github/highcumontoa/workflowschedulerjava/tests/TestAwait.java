package com.github.highcumontoa.workflowschedulerjava.tests;

import com.github.highcumontoa.workflowschedulerjava.dto.JobResponse;
import com.github.highcumontoa.workflowschedulerjava.service.JobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Supplier;

/** 测试轮询工具：反复查询直到满足断言或超时。 */
final class TestAwait {

    private static final Logger log = LoggerFactory.getLogger(TestAwait.class);

    private TestAwait() {
    }

    static JobResponse awaitStatus(JobService service, String jobId, String expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        JobResponse last = null;
        while (System.currentTimeMillis() < deadline) {
            last = service.get(jobId);
            log.info("await job={} 期望状态={} 当前状态={} attempts={}/{}",
                    jobId, expected, last.status(), last.attempts(), last.maxAttempts());
            if (Objects.equals(last.status(), expected)) {
                return last;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("作业 " + jobId + " 在 " + timeoutMs + "ms 内未到达 "
                + expected + "，最终状态=" + (last == null ? "null" : last.status())
                + "，原因=" + (last == null ? null : last.failureReason()));
    }

    static void until(Supplier<Boolean> condition, long timeoutMs, String desc)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(condition.get())) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("等待超时: " + desc);
    }
}
