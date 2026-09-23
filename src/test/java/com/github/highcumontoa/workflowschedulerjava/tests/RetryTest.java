package com.github.highcumontoa.workflowschedulerjava.tests;

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

/** 失败重试：可配置尝试次数与退避；成功即止，耗尽则 FAILED 且下游 UNREACHABLE。 */
class RetryTest {

    private static final Logger log = LoggerFactory.getLogger(RetryTest.class);

    private SubmitJobRequest flaky(String id, long failTimes, int maxAttempts, long backoff, String... deps) {
        SubmitJobRequest r = new SubmitJobRequest();
        r.setId(id);
        r.setTaskType("flaky");
        r.setTaskParam(failTimes); // 前 failTimes 次尝试失败
        r.setRetryPolicy(new RetryPolicy(maxAttempts, backoff));
        r.setDependencies(List.of(deps));
        return r;
    }

    @Test
    void succeedsAfterConfiguredRetries(@TempDir Path dir) {
        SchedulerEngine engine = newEngine(dir, props(dir));
        engine.start();
        try {
            SubmitBatchRequest batch = new SubmitBatchRequest();
            // 前 2 次失败，第 3 次成功；上限 3，退避 30ms
            batch.setJobs(List.of(flaky("r", 2, 3, 30)));
            engine.submit(batch);

            var view = awaitState(engine, "r", JobState.SUCCESS, Duration.ofSeconds(10));
            log.info("[重试判定] job=r 最终 state={} attempts={}/3", view.getState(), view.getAttempts());
            printStates("重试后成功", engine);
            assertEquals(3, view.getAttempts(), "应恰好尝试 3 次后成功");
        } finally {
            engine.stop();
        }
    }

    @Test
    void failsAfterExhaustingRetriesAndPropagates(@TempDir Path dir) {
        SchedulerEngine engine = newEngine(dir, props(dir));
        engine.start();
        try {
            SubmitBatchRequest batch = new SubmitBatchRequest();
            // 前 5 次都失败，但上限只有 2 => 耗尽后 FAILED
            batch.setJobs(List.of(flaky("f", 5, 2, 10), flaky("child", 0, 1, 0, "f")));
            engine.submit(batch);

            var failed = awaitState(engine, "f", JobState.FAILED, Duration.ofSeconds(10));
            var child = awaitState(engine, "child", JobState.UNREACHABLE, Duration.ofSeconds(10));
            log.info("[重试判定] job=f 最终 state={} attempts={}/2 failure={}",
                    failed.getState(), failed.getAttempts(), failed.getFailureReason());
            printStates("重试耗尽失败并传播下游", engine);
            assertEquals(2, failed.getAttempts(), "不得超过配置的最大尝试次数");
            assertEquals("f", child.getBlockedBy());
            assertEquals(0, child.getAttempts(), "下游在重试耗尽后仍不得执行");
        } finally {
            engine.stop();
        }
    }
}
