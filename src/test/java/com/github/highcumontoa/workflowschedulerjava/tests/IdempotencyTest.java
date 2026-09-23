package com.github.highcumontoa.workflowschedulerjava.tests;

import com.github.highcumontoa.workflowschedulerjava.dto.SubmitBatchRequest;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobRequest;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobResponse;
import com.github.highcumontoa.workflowschedulerjava.engine.SchedulerEngine;
import com.github.highcumontoa.workflowschedulerjava.exception.ErrorCode;
import com.github.highcumontoa.workflowschedulerjava.exception.SchedulerException;
import com.github.highcumontoa.workflowschedulerjava.model.JobState;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.highcumontoa.workflowschedulerjava.tests.EngineTestSupport.awaitState;
import static com.github.highcumontoa.workflowschedulerjava.tests.EngineTestSupport.newEngine;
import static com.github.highcumontoa.workflowschedulerjava.tests.EngineTestSupport.props;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 幂等：相同键并发重复提交只接受一次，只执行一次；同键不同内容返回可区分冲突。 */
class IdempotencyTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyTest.class);

    private SubmitJobRequest noopWithKey(String id, String key) {
        SubmitJobRequest r = new SubmitJobRequest();
        r.setId(id);
        r.setIdempotencyKey(key);
        r.setTaskType("sleep");
        r.setTaskParam(200L);
        return r;
    }

    @Test
    void concurrentDuplicateSubmissionsExecuteExactlyOnce(@TempDir Path dir) throws Exception {
        SchedulerEngine engine = newEngine(dir, props(dir));
        engine.start();
        try {
            int threads = 12;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            Set<String> acceptedIds = ConcurrentHashMap.newKeySet();
            int[] duplicates = {0};
            int[] errors = {0};
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        SubmitBatchRequest req = new SubmitBatchRequest();
                        req.setJobs(List.of(noopWithKey("dedup-job", "key-001")));
                        List<SubmitJobResponse> resp = engine.submit(req);
                        SubmitJobResponse only = resp.get(0);
                        acceptedIds.add(only.getJobId());
                        synchronized (duplicates) {
                            if (only.isDuplicate()) {
                                duplicates[0]++;
                            }
                        }
                    } catch (Exception e) {
                        synchronized (errors) {
                            errors[0]++;
                        }
                        log.warn("并发提交异常: {}", e.toString());
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS));
            pool.shutdown();

            log.info("[幂等判定] 并发={} 接受作业id集合={} duplicate响应数={} 异常数={}",
                    threads, acceptedIds, duplicates[0], errors[0]);
            assertEquals(1, acceptedIds.size(), "相同幂等键必须解析到同一个作业 id");
            // 明确断言：只有 1 个非 duplicate 响应，其余全部 duplicate
            assertEquals(threads - 1, duplicates[0], "除首次外都应标记 duplicate");
            assertEquals(0, errors[0]);

            var view = awaitState(engine, "dedup-job", JobState.SUCCESS, Duration.ofSeconds(10));
            log.info("[幂等判定] 作业最终 state={} attempts={}（必须为 1，未重复执行）",
                    view.getState(), view.getAttempts());
            assertEquals(1, view.getAttempts(), "重复提交不得导致重复执行");
        } finally {
            engine.stop();
        }
    }

    @Test
    void sameKeyWithDifferentPayloadIsRejectedWithConflictCode(@TempDir Path dir) {
        SchedulerEngine engine = newEngine(dir, props(dir));
        engine.start();
        try {
            SubmitBatchRequest first = new SubmitBatchRequest();
            first.setJobs(List.of(noopWithKey("job-x", "key-9")));
            engine.submit(first);

            SubmitJobRequest different = new SubmitJobRequest();
            different.setId("job-x");
            different.setIdempotencyKey("key-9");
            different.setTaskType("flaky"); // 与首次 noop/sleep 语义不同
            different.setTaskParam(3L);
            SubmitBatchRequest second = new SubmitBatchRequest();
            second.setJobs(List.of(different));

            SchedulerException ex = null;
            try {
                engine.submit(second);
            } catch (SchedulerException e) {
                ex = e;
            }
            log.info("[幂等判定] 同键不同内容 code={} message={}",
                    ex == null ? null : ex.getErrorCode().code(), ex == null ? null : ex.getMessage());
            assertTrue(ex != null && ex.getErrorCode() == ErrorCode.IDEMPOTENCY_KEY_CONFLICT);
        } finally {
            engine.stop();
        }
    }
}
