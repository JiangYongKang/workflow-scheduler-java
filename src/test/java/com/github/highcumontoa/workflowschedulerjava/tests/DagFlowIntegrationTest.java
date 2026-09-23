package com.github.highcumontoa.workflowschedulerjava.tests;

import com.github.highcumontoa.workflowschedulerjava.dto.BatchSubmitResponse;
import com.github.highcumontoa.workflowschedulerjava.dto.JobResponse;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitBatchRequest;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobRequest;
import com.github.highcumontoa.workflowschedulerjava.service.JobService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** 依赖编排与状态流转：上游全部成功才执行；上游失败/取消时下游不可达。 */
@SpringBootTest
class DagFlowIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(DagFlowIntegrationTest.class);

    @Autowired
    private JobService jobService;

    private SubmitJobRequest job(String id, String type, String payload, List<String> deps) {
        return new SubmitJobRequest(id, null, type, payload, deps,
                null, null, null, null, null);
    }

    @Test
    @DisplayName("线性 DAG：上游全部成功后下游依次执行，各执行一次")
    void linearDagRunsInOrderOnce() throws Exception {
        BatchSubmitResponse resp = jobService.submitBatch(new SubmitBatchRequest(List.of(
                job("d-a", "NOOP", null, null),
                job("d-b", "SLEEP", "150", List.of("d-a")),
                job("d-c", "NOOP", null, List.of("d-b"))
        )));
        assertEquals(3, resp.jobs().size());

        JobResponse a = TestAwait.awaitStatus(jobService, "d-a", "SUCCESS", 5000);
        assertEquals(1, a.attempts());
        JobResponse b = TestAwait.awaitStatus(jobService, "d-b", "SUCCESS", 5000);
        assertEquals(1, b.attempts());
        assertTrue(b.updatedAt() >= a.updatedAt(), "下游 b 必须在上游 a 成功之后完成");
        JobResponse c = TestAwait.awaitStatus(jobService, "d-c", "SUCCESS", 5000);
        assertEquals(1, c.attempts());
        log.info("线性 DAG 判定依据: a/b/c 均 SUCCESS 且各 1 次，updatedAt 单调");
    }

    @Test
    @DisplayName("菱形 DAG：两个上游都成功，汇聚节点恰好执行一次")
    void diamondDag() throws Exception {
        jobService.submitBatch(new SubmitBatchRequest(List.of(
                job("d-top", "NOOP", null, null),
                job("d-left", "NOOP", null, List.of("d-top")),
                job("d-right", "NOOP", null, List.of("d-top")),
                job("d-join", "NOOP", null, List.of("d-left", "d-right"))
        )));
        JobResponse join = TestAwait.awaitStatus(jobService, "d-join", "SUCCESS", 5000);
        assertEquals(1, join.attempts(), "汇聚节点被两个上游各解锁一次也只能执行一次");
        log.info("菱形 DAG 判定依据: d-join SUCCESS 且 attempts=1");
    }

    @Test
    @DisplayName("上游重试耗尽 FAILED：直接与间接下游全部 UNREACHABLE，且 attempts=0 从未执行")
    void failurePropagatesDownChain() throws Exception {
        jobService.submitBatch(new SubmitBatchRequest(List.of(
                job("f-root", "FAIL", "根失败", null),
                job("f-mid", "NOOP", null, List.of("f-root")),
                job("f-leaf", "NOOP", null, List.of("f-mid"))
        )));
        JobResponse root = TestAwait.awaitStatus(jobService, "f-root", "FAILED", 8000);
        assertEquals(3, root.attempts(), "默认重试 3 次");
        assertTrue(root.failureReason().contains("重试耗尽"), "失败原因可解释: " + root.failureReason());

        JobResponse mid = TestAwait.awaitStatus(jobService, "f-mid", "UNREACHABLE", 5000);
        JobResponse leaf = TestAwait.awaitStatus(jobService, "f-leaf", "UNREACHABLE", 5000);
        assertEquals(0, mid.attempts(), "下游从未被执行");
        assertEquals(0, leaf.attempts(), "间接下游从未被执行");
        assertTrue(mid.failureReason().contains("f-root"), "原因须指明阻断上游: " + mid.failureReason());
        assertTrue(leaf.failureReason().contains("f-mid"), "间接下游原因沿链携带: " + leaf.failureReason());
        log.info("失败传播判定依据: root={} mid='{}' leaf='{}'",
                root.status(), mid.failureReason(), leaf.failureReason());
    }

    @Test
    @DisplayName("FLAKY 作业在重试预算内最终成功，下游正常执行")
    void retryThenSuccess() throws Exception {
        jobService.submitBatch(new SubmitBatchRequest(List.of(
                job("flaky-1", "FLAKY", "failFirst=2", null),
                job("flaky-down", "NOOP", null, List.of("flaky-1"))
        )));
        JobResponse flaky = TestAwait.awaitStatus(jobService, "flaky-1", "SUCCESS", 10000);
        assertEquals(3, flaky.attempts(), "前两次失败、第三次成功");
        JobResponse down = TestAwait.awaitStatus(jobService, "flaky-down", "SUCCESS", 5000);
        assertEquals(1, down.attempts());
        log.info("重试用例判定依据: flaky-1 尝试 {} 次后 SUCCESS，下游执行一次", flaky.attempts());
    }
}
