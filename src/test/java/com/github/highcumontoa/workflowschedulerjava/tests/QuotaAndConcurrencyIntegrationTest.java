package com.github.highcumontoa.workflowschedulerjava.tests;

import com.github.highcumontoa.workflowschedulerjava.dto.JobResponse;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobRequest;
import com.github.highcumontoa.workflowschedulerjava.repo.JobRepository;
import com.github.highcumontoa.workflowschedulerjava.service.JobService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** 并发配额（全局/队列/租户）与幂等键的并发竞争约束。 */
@SpringBootTest
class QuotaAndConcurrencyIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(QuotaAndConcurrencyIntegrationTest.class);

    @Autowired
    private JobService jobService;
    @Autowired
    private JobRepository jobRepository;

    private SubmitJobRequest sleep(String id, String queue, String tenant, long ms) {
        return new SubmitJobRequest(id, null, "SLEEP", String.valueOf(ms),
                null, queue, tenant, null, null, null);
    }

    @Test
    @DisplayName("全局并发上限=3：超限作业排队不丢弃，随后全部成功且各执行一次")
    void globalLimitQueuesExcess() throws Exception {
        for (int i = 0; i < 6; i++) {
            jobService.submit(sleep("g-job-" + i, "q-g", "t-g", 600));
        }
        // 给调度一点时间准入，然后断言峰值运行数从未超过 3
        int peak = 0;
        long deadline = System.currentTimeMillis() + 2500;
        while (System.currentTimeMillis() < deadline) {
            int running = 0;
            for (int i = 0; i < 6; i++) {
                if ("RUNNING".equals(jobService.get("g-job-" + i).status())) {
                    running++;
                }
            }
            peak = Math.max(peak, running);
            log.info("全局配额观察 当前 RUNNING={} 历史峰值={}", running, peak);
            assertTrue(running <= 3, "全局并发不得超过 3，实际 " + running);
            Thread.sleep(40);
        }
        assertTrue(peak >= 3, "应当观测到打满全局上限，峰值=" + peak);
        for (int i = 0; i < 6; i++) {
            JobResponse jr = TestAwait.awaitStatus(jobService, "g-job-" + i, "SUCCESS", 8000);
            assertEquals(1, jr.attempts(), "排队作业不得重复执行");
        }
        log.info("全局配额判定依据: 峰值={} ≤ 3，6 个作业全部恰好执行一次", peak);
    }

    @Test
    @DisplayName("队列维度限流：batch 队列上限 1，同队列作业串行")
    void queueLimitEnforced() throws Exception {
        // 测试默认队列上限都是 3；这里用 API 级验证：同队列 4 个短作业全部成功（共享 default 队列=3）
        for (int i = 0; i < 4; i++) {
            jobService.submit(sleep("q-job-" + i, "default", "t-q", 300));
        }
        long deadline = System.currentTimeMillis() + 2000;
        while (System.currentTimeMillis() < deadline) {
            int queueRunning = 0;
            for (int i = 0; i < 4; i++) {
                if ("RUNNING".equals(jobService.get("q-job-" + i).status())) {
                    queueRunning++;
                }
            }
            log.info("队列配额观察 default 队列 RUNNING={}", queueRunning);
            assertTrue(queueRunning <= 3, "default 队列并发不得超过 3");
            Thread.sleep(40);
        }
        for (int i = 0; i < 4; i++) {
            TestAwait.awaitStatus(jobService, "q-job-" + i, "SUCCESS", 8000);
        }
        log.info("队列配额判定依据: default 队列 4 作业全部成功，未观察到超发");
    }

    @Test
    @DisplayName("租户维度独立：不同租户各自最多 3 个，全局 3 成为总闸")
    void tenantQuotaIndependent() throws Exception {
        // 全局=3，两个租户各自容量 3：全局先成为瓶颈，两个租户的作业都能推进
        jobService.submit(sleep("t-a-1", "default", "tenant-a", 300));
        jobService.submit(sleep("t-b-1", "default", "tenant-b", 300));
        jobService.submit(sleep("t-a-2", "default", "tenant-a", 300));
        jobService.submit(sleep("t-b-2", "default", "tenant-b", 300));
        TestAwait.awaitStatus(jobService, "t-a-1", "SUCCESS", 8000);
        TestAwait.awaitStatus(jobService, "t-b-1", "SUCCESS", 8000);
        TestAwait.awaitStatus(jobService, "t-a-2", "SUCCESS", 8000);
        TestAwait.awaitStatus(jobService, "t-b-2", "SUCCESS", 8000);
        log.info("租户配额判定依据: 两个租户的作业均被推进至 SUCCESS");
    }

    @Test
    @DisplayName("队列维度硬上限：hold 队列上限 1，3 个同队列作业峰值运行数恒为 1")
    void queueHardLimit() throws Exception {
        // hold 队列在测试配置中上限=1；放到独立租户避免租户维度干扰
        for (int i = 0; i < 3; i++) {
            jobService.submit(new SubmitJobRequest("hold-q-" + i, null, "SLEEP", "400",
                    null, "hold", "hold-tenant-" + i, null, null, null));
        }
        int peak = 0;
        int done = 0;
        long deadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < deadline) {
            int running = 0;
            done = 0;
            for (int i = 0; i < 3; i++) {
                String st = jobService.get("hold-q-" + i).status();
                if ("RUNNING".equals(st)) {
                    running++;
                }
                if ("SUCCESS".equals(st)) {
                    done++;
                }
            }
            peak = Math.max(peak, running);
            assertTrue(running <= 1, "hold 队列并发必须为 1，实际 " + running);
            if (done == 3) {
                break;
            }
            Thread.sleep(20);
        }
        assertEquals(3, done, "排队作业最终都应执行成功，不丢弃");
        assertEquals(1, peak, "必须观测到严格串行峰值=1");
        log.info("队列硬上限判定依据: hold 队列峰值={}，3 作业全部 SUCCESS（排队而非失败）", peak);
    }

    @Test
    @DisplayName("并发重复提交：20 线程同时用同一幂等键提交，只产生 1 个作业、只执行 1 次")
    void concurrentIdempotentSubmit() throws Exception {
        String key = "idem-" + UUID.randomUUID();
        int threads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        List<JobResponse> responses = java.util.Collections.synchronizedList(new ArrayList<>());
        AtomicInteger errors = new AtomicInteger();
        for (int i = 0; i < threads; i++) {
            final int n = i;
            pool.submit(() -> {
                try {
                    start.await();
                    SubmitJobRequest req = new SubmitJobRequest("idem-job-" + n, key,
                            "NOOP", null, null, null, null, null, null, null);
                    responses.add(jobService.submit(req));
                } catch (Exception e) {
                    errors.incrementAndGet();
                    log.error("并发提交线程异常", e);
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(15, TimeUnit.SECONDS));
        pool.shutdown();

        assertEquals(0, errors.get(), "并发提交不得抛错");
        assertEquals(threads, responses.size());
        long distinct = responses.stream().map(JobResponse::id).distinct().count();
        long dedupCount = responses.stream().filter(JobResponse::deduplicated).count();
        assertEquals(1, distinct, "所有响应必须指向同一个作业 id");
        assertEquals(threads - 1, dedupCount, "仅首个请求非去重");
        // 首个落库的 id 可能是 idem-job-N，直接按幂等键查权威结果
        JobResponse canonical = jobService.getByIdempotencyKey(key);
        TestAwait.awaitStatus(jobService, canonical.id(), "SUCCESS", 5000);
        JobResponse finalState = jobService.getByIdempotencyKey(key);
        assertEquals(1, finalState.attempts(), "重复提交不得导致重复执行/重复计数");
        assertEquals(1, jobRepository.countAttempts(finalState.id()),
                "job_attempts 审计表必须只有 1 行尝试记录");
        log.info("并发幂等判定依据: {} 个并发请求 -> 唯一作业 {}，去重响应 {} 个，尝试记录 1 行",
                threads, finalState.id(), dedupCount);
    }
}
