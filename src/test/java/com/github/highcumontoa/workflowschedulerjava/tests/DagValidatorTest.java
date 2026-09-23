package com.github.highcumontoa.workflowschedulerjava.tests;

import com.github.highcumontoa.workflowschedulerjava.engine.DagValidator;
import com.github.highcumontoa.workflowschedulerjava.exception.ErrorCode;
import com.github.highcumontoa.workflowschedulerjava.exception.SchedulerException;
import com.github.highcumontoa.workflowschedulerjava.model.Job;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** DAG 准入校验：拒绝原因必须可区分（UPSTREAM_NOT_FOUND / CYCLIC_DEPENDENCY / DUPLICATE_JOB_ID）。 */
class DagValidatorTest {

    private static final Logger log = LoggerFactory.getLogger(DagValidatorTest.class);

    private final DagValidator validator = new DagValidator();

    private Function<String, List<String>> resolverOf(List<Job> jobs, java.util.Set<String> known) {
        java.util.Map<String, List<String>> map = new java.util.HashMap<>();
        for (Job j : jobs) {
            // 重复 id 的场景先保留任意一条，重复检测由校验器自身完成
            map.putIfAbsent(j.getId(), j.getDependencies());
        }
        return map::get;
    }

    private Job job(String id, String... deps) {
        Job j = new Job();
        j.setId(id);
        j.setDependencies(List.of(deps));
        return j;
    }

    @Test
    void rejectsMissingUpstreamWithDistinctCode() {
        List<Job> batch = List.of(job("a", "ghost"));
        SchedulerException ex = assertThrows(SchedulerException.class,
                () -> validator.validate(batch, Set.of(), resolverOf(batch, Set.of())));
        log.info("[判定依据] code={} message={}", ex.getErrorCode().code(), ex.getMessage());
        assertEquals(ErrorCode.UPSTREAM_NOT_FOUND, ex.getErrorCode());
        assertTrue(ex.getMessage().contains("ghost"));
    }

    @Test
    void rejectsCycleWithDistinctCodeAndPath() {
        List<Job> batch = List.of(job("a", "c"), job("b", "a"), job("c", "b"));
        SchedulerException ex = assertThrows(SchedulerException.class,
                () -> validator.validate(batch, Set.of(), resolverOf(batch, Set.of())));
        log.info("[判定依据] code={} message={}", ex.getErrorCode().code(), ex.getMessage());
        assertEquals(ErrorCode.CYCLIC_DEPENDENCY, ex.getErrorCode());
        assertTrue(ex.getMessage().contains("环依赖"));
    }

    @Test
    void rejectsDuplicateIdWithinBatch() {
        List<Job> batch = List.of(job("a"), job("a"));
        SchedulerException ex = assertThrows(SchedulerException.class,
                () -> validator.validate(batch, Set.of(), resolverOf(batch, Set.of())));
        log.info("[判定依据] code={} message={}", ex.getErrorCode().code(), ex.getMessage());
        assertEquals(ErrorCode.DUPLICATE_JOB_ID, ex.getErrorCode());
    }

    @Test
    void acceptsValidDagAndDependenciesOnExistingJobs() {
        List<Job> batch = List.of(job("x", "root"), job("y", "x"));
        validator.validate(batch, Set.of("root"), resolverOf(batch, Set.of("root")));
        log.info("[判定依据] 合法 DAG 被接受: x->root, y->x");
    }
}
