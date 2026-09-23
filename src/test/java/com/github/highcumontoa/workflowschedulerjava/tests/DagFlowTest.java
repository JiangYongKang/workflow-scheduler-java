package com.github.highcumontoa.workflowschedulerjava.tests;

import com.github.highcumontoa.workflowschedulerjava.dto.SubmitBatchRequest;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobRequest;
import com.github.highcumontoa.workflowschedulerjava.engine.SchedulerEngine;
import com.github.highcumontoa.workflowschedulerjava.model.JobState;
import java.nio.file.Files;
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

/** 依赖编排：上游全部成功才执行；上游失败/取消，下游标记 UNREACHABLE 并沿链传播。 */
class DagFlowTest {

    private static final Logger log = LoggerFactory.getLogger(DagFlowTest.class);

    private SubmitJobRequest req(String id, String type, long param, int maxAttempts, String... deps) {
        SubmitJobRequest r = new SubmitJobRequest();
        r.setId(id);
        r.setTaskType(type);
        r.setTaskParam(param);
        r.setDependencies(List.of(deps));
        var rp = new com.github.highcumontoa.workflowschedulerjava.model.RetryPolicy(maxAttempts, 0);
        r.setRetryPolicy(rp);
        return r;
    }

    @Test
    void downstreamRunsOnlyAfterAllUpstreamsSuccess(@TempDir java.nio.file.Path dir) {
        SchedulerEngine engine = newEngine(dir, props(dir));
        engine.start();
        try {
            SubmitBatchRequest batch = new SubmitBatchRequest();
            batch.setJobs(List.of(
                    req("a", "noop", 0, 1),
                    req("b", "noop", 0, 1),
                    req("c", "noop", 0, 1, "a", "b")));
            engine.submit(batch);
            log.info("提交 DAG: a,b 无上游；c 依赖 [a,b]");

            awaitState(engine, "a", JobState.SUCCESS, Duration.ofSeconds(5));
            awaitState(engine, "b", JobState.SUCCESS, Duration.ofSeconds(5));
            awaitState(engine, "c", JobState.SUCCESS, Duration.ofSeconds(5));
            printStates("上游全部成功后下游执行", engine);
        } finally {
            engine.stop();
        }
    }

    @Test
    void failedUpstreamMarksEntireChainUnreachable(@TempDir java.nio.file.Path dir) {
        SchedulerEngine engine = newEngine(dir, props(dir));
        engine.start();
        try {
            SubmitBatchRequest batch = new SubmitBatchRequest();
            batch.setJobs(List.of(
                    // fail flaky：param=1 表示首次失败，maxAttempts=1 => 重试耗尽 FAILED
                    req("bad", "flaky", 10, 1),
                    req("mid", "noop", 0, 1, "bad"),
                    req("leaf", "noop", 0, 1, "mid")));
            engine.submit(batch);
            log.info("提交链: bad(必失败) -> mid -> leaf");

            awaitState(engine, "bad", JobState.FAILED, Duration.ofSeconds(5));
            var mid = awaitState(engine, "mid", JobState.UNREACHABLE, Duration.ofSeconds(5));
            var leaf = awaitState(engine, "leaf", JobState.UNREACHABLE, Duration.ofSeconds(5));
            printStates("失败沿依赖链传播", engine);
            assertEquals("bad", mid.getBlockedBy());
            assertEquals("mid", leaf.getBlockedBy());
            assertEquals(0, mid.getAttempts(), "UNREACHABLE 作业不得被执行");
            assertEquals(0, leaf.getAttempts());
        } finally {
            engine.stop();
        }
    }

    @Test
    void cancelledUpstreamAlsoMarksDownstreamUnreachable(@TempDir java.nio.file.Path dir) {
        SchedulerEngine engine = newEngine(dir, props(dir));
        engine.start();
        try {
            SubmitBatchRequest batch = new SubmitBatchRequest();
            batch.setJobs(List.of(
                    req("x", "sleep", 2000, 1),
                    req("y", "noop", 0, 1, "x")));
            engine.submit(batch);
            awaitState(engine, "x", JobState.RUNNING, Duration.ofSeconds(5));
            engine.cancel("x");
            log.info("取消 RUNNING 作业 x，期望下游 y 不可达");

            awaitState(engine, "x", JobState.CANCELLED, Duration.ofSeconds(5));
            awaitState(engine, "y", JobState.UNREACHABLE, Duration.ofSeconds(5));
            printStates("取消上游后下游不可达", engine);
        } finally {
            engine.stop();
        }
    }
}
