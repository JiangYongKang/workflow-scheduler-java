package com.github.highcumontoa.workflowschedulerjava.tests;

import com.github.highcumontoa.workflowschedulerjava.dag.DagValidator;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobRequest;
import com.github.highcumontoa.workflowschedulerjava.error.ErrorCode;
import com.github.highcumontoa.workflowschedulerjava.error.SchedulerException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/** DAG 接受前校验：未知上游 / 环 / 批内重复必须以可区分原因被拒绝。 */
class DagValidatorTest {

    private static final Logger log = LoggerFactory.getLogger(DagValidatorTest.class);

    private SubmitJobRequest req(String id, List<String> deps) {
        return new SubmitJobRequest(id, null, "NOOP", null, deps,
                null, null, null, null, null);
    }

    @Test
    @DisplayName("单作业引用不存在上游 -> UNKNOWN_DEPENDENCY")
    void singleUnknownDependency() {
        DagValidator validator = new DagValidator(new FakeJobRepository());
        SchedulerException ex = assertThrows(SchedulerException.class,
                () -> validator.validateSingle(req("j1", List.of("ghost"))));
        assertEquals(ErrorCode.UNKNOWN_DEPENDENCY, ex.getCode());
        log.info("用例判定依据: 错误码={} 信息='{}'", ex.getCode(), ex.getMessage());
    }

    @Test
    @DisplayName("单作业引用已存在上游 -> 通过")
    void singleExistingDependency() {
        DagValidator validator = new DagValidator(new FakeJobRepository("up"));
        assertDoesNotThrow(() -> validator.validateSingle(req("j1", List.of("up"))));
        log.info("用例通过: 已存在上游不拒绝");
    }

    @Test
    @DisplayName("自环（单作业依赖自身）-> CYCLE_DETECTED")
    void selfLoopSingle() {
        DagValidator validator = new DagValidator(new FakeJobRepository());
        SchedulerException ex = assertThrows(SchedulerException.class,
                () -> validator.validateSingle(req("j1", List.of("j1"))));
        assertEquals(ErrorCode.CYCLE_DETECTED, ex.getCode());
        log.info("用例判定依据: 错误码={} 信息='{}'", ex.getCode(), ex.getMessage());
    }

    @Test
    @DisplayName("批量 A->B->A 环 -> CYCLE_DETECTED，且与未知上游原因可区分")
    void batchCycle() {
        DagValidator validator = new DagValidator(new FakeJobRepository());
        SchedulerException ex = assertThrows(SchedulerException.class,
                () -> validator.validateBatch(
                        List.of(req("A", List.of("B")), req("B", List.of("A"))),
                        Set.of("A", "B")));
        assertEquals(ErrorCode.CYCLE_DETECTED, ex.getCode());
        assertNotEquals(ErrorCode.UNKNOWN_DEPENDENCY, ex.getCode());
        log.info("用例判定依据: 错误码={} 信息='{}'", ex.getCode(), ex.getMessage());
    }

    @Test
    @DisplayName("批量引用不存在上游 -> UNKNOWN_DEPENDENCY")
    void batchUnknownDependency() {
        DagValidator validator = new DagValidator(new FakeJobRepository());
        SchedulerException ex = assertThrows(SchedulerException.class,
                () -> validator.validateBatch(
                        List.of(req("A", List.of("ghost"))),
                        Set.of("A")));
        assertEquals(ErrorCode.UNKNOWN_DEPENDENCY, ex.getCode());
        log.info("用例判定依据: 错误码={} 信息='{}'", ex.getCode(), ex.getMessage());
    }

    @Test
    @DisplayName("合法 DAG（含对已存在作业的依赖）-> 通过")
    void validBatch() {
        DagValidator validator = new DagValidator(new FakeJobRepository("old"));
        assertDoesNotThrow(() -> validator.validateBatch(
                List.of(req("A", List.of("old")), req("B", List.of("A")),
                        req("C", List.of("A", "B"))),
                Set.of("A", "B", "C")));
        log.info("用例通过: A,B 无环且 old 已存在");
    }

    @Test
    @DisplayName("批内 id 重复 -> INVALID_REQUEST")
    void batchDuplicateId() {
        DagValidator validator = new DagValidator(new FakeJobRepository());
        SchedulerException ex = assertThrows(SchedulerException.class,
                () -> validator.validateBatch(
                        List.of(req("A", null), req("A", null)),
                        Set.of("A")));
        assertEquals(ErrorCode.INVALID_REQUEST, ex.getCode());
        log.info("用例判定依据: 错误码={} 信息='{}'", ex.getCode(), ex.getMessage());
    }
}
