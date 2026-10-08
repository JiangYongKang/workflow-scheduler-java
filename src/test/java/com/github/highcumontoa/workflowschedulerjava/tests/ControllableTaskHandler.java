package com.github.highcumontoa.workflowschedulerjava.tests;

import com.github.highcumontoa.workflowschedulerjava.engine.InProcessTaskHandler;
import com.github.highcumontoa.workflowschedulerjava.engine.TaskHandler;
import com.github.highcumontoa.workflowschedulerjava.model.Job;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 测试专用任务处理器：按「作业 id + 尝试序号」分别控制每一次执行的开始与结束，
 * 且执行体<b>故意不响应中断</b>（吞掉 InterruptedException 继续跑），用于稳定复现
 * “被判超时/取消后任务没有真的停下来、迟到结果跨尝试回送”的竞争。
 *
 * <p>每个 (jobId, attempt) 有两个门闩：started（任务体已开始）与 release（允许任务体结束）。
 * 结束方式可配置：默认成功返回；{@link #failOn(String, int, String)} 配置的尝试改为抛异常。</p>
 */
final class ControllableTaskHandler implements TaskHandler {

    private static final Logger log = LoggerFactory.getLogger(ControllableTaskHandler.class);

    private record Key(String jobId, int attempt) {
    }

    private final Map<Key, CountDownLatch> started = new ConcurrentHashMap<>();
    private final Map<Key, CountDownLatch> released = new ConcurrentHashMap<>();
    /** 配置某作业某尝试结束时抛出的失败信息（不配置则成功返回）。 */
    private final Map<Key, String> failures = new ConcurrentHashMap<>();
    /** 非 controlled 类型（如 noop）直接委托内置处理器，便于在同一引擎里安排“补位作业”。 */
    private final InProcessTaskHandler delegate = new InProcessTaskHandler();

    private CountDownLatch startedLatch(String jobId, int attempt) {
        return started.computeIfAbsent(new Key(jobId, attempt), k -> new CountDownLatch(1));
    }

    private CountDownLatch releaseLatch(String jobId, int attempt) {
        return released.computeIfAbsent(new Key(jobId, attempt), k -> new CountDownLatch(1));
    }

    /** 等待某作业第 attempt 次执行真正进入任务体。 */
    void awaitStarted(String jobId, int attempt) throws InterruptedException {
        if (!startedLatch(jobId, attempt).await(10, java.util.concurrent.TimeUnit.SECONDS)) {
            throw new AssertionError("job=" + jobId + " 第 " + attempt + " 次执行未在 10s 内开始");
        }
    }

    /** 放行某作业第 attempt 次执行，使其结束（成功或按配置抛错）。 */
    void release(String jobId, int attempt) {
        log.info("[可控任务] 放行 job={} attempt={}", jobId, attempt);
        releaseLatch(jobId, attempt).countDown();
    }

    /** 配置某作业某尝试在被放行后以失败（抛异常）结束。 */
    void failOn(String jobId, int attempt, String message) {
        failures.put(new Key(jobId, attempt), message);
    }

    @Override
    public void execute(Job job) throws Exception {
        if (!"controlled".equalsIgnoreCase(job.getTaskType())) {
            delegate.execute(job);
            return;
        }
        int attempt = job.getAttempts();
        String jobId = job.getId();
        startedLatch(jobId, attempt).countDown();
        log.info("[可控任务] job={} attempt={} 开始（不响应中断，等待放行）", jobId, attempt);
        // 故意忽略中断：模拟“任务本身对取消不敏感、自己继续跑完”
        boolean interruptedWhileWaiting = false;
        while (!releaseLatch(jobId, attempt).await(20, java.util.concurrent.TimeUnit.MILLISECONDS)) {
            if (Thread.currentThread().isInterrupted()) {
                interruptedWhileWaiting = true;
            }
        }
        if (interruptedWhileWaiting) {
            log.warn("[可控任务] job={} attempt={} 等待期间收到过中断信号，但任务忽略它并继续跑完",
                    jobId, attempt);
        }
        String failure = failures.get(new Key(jobId, attempt));
        if (failure != null) {
            log.warn("[可控任务] job={} attempt={} 迟到地以失败结束: {}", jobId, attempt, failure);
            throw new IllegalStateException(failure);
        }
        log.info("[可控任务] job={} attempt={} 迟到地成功结束", jobId, attempt);
    }
}
