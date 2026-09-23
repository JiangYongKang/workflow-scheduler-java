package com.github.highcumontoa.workflowschedulerjava.scheduler;

import com.github.highcumontoa.workflowschedulerjava.handler.HandlerResult;
import com.github.highcumontoa.workflowschedulerjava.handler.HandlerRegistry;
import com.github.highcumontoa.workflowschedulerjava.handler.TaskContext;
import com.github.highcumontoa.workflowschedulerjava.model.Job;
import com.github.highcumontoa.workflowschedulerjava.model.JobStatus;
import com.github.highcumontoa.workflowschedulerjava.quota.CancelRegistry;
import com.github.highcumontoa.workflowschedulerjava.repo.JobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Set;

/** 在工作线程中执行单个作业：调用处理器、落尝试记录、按重试策略迁移状态、级联传播。 */
@Component
public class JobRunner {

    private static final Logger log = LoggerFactory.getLogger(JobRunner.class);

    private static final Set<JobStatus> RUNNING_ONLY = Set.of(JobStatus.RUNNING);

    private final JobRepository jobRepository;
    private final HandlerRegistry handlerRegistry;
    private final CancelRegistry cancelRegistry;
    private final DependencyPropagator propagator;
    private final SchedulerSignals signals;

    public JobRunner(JobRepository jobRepository, HandlerRegistry handlerRegistry,
                     CancelRegistry cancelRegistry, DependencyPropagator propagator,
                     SchedulerSignals signals) {
        this.jobRepository = jobRepository;
        this.handlerRegistry = handlerRegistry;
        this.cancelRegistry = cancelRegistry;
        this.propagator = propagator;
        this.signals = signals;
    }

    /** 阻塞执行作业（由 Scheduler 的工作线程池调用）。 */
    public void run(String jobId) {
        Job job = jobRepository.findById(jobId);
        if (job == null || job.getStatus() != JobStatus.RUNNING) {
            log.info("job={} 工作线程跳过执行，判定依据: 当前状态非 RUNNING（可能已被并发取消）", jobId);
            return;
        }
        int attemptNo = job.getAttempts();
        long attemptId = jobRepository.recordAttemptStart(jobId, attemptNo);
        log.info("job={} 状态迁移 RUNNING 中 开始第 {}/{} 次尝试",
                jobId, attemptNo, job.getMaxAttempts());

        HandlerResult result;
        Throwable thrown = null;
        try {
            TaskContext context = new TaskContext(
                    jobId, attemptNo, job.getPayload(),
                    () -> cancelRegistry.isCancelled(jobId));
            result = handlerRegistry.require(job.getJobType()).handle(context);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            thrown = ie;
            result = HandlerResult.fail("执行被中断: " + ie.getClass().getSimpleName());
        } catch (Throwable t) {
            thrown = t;
            result = HandlerResult.fail("执行抛出异常: " + t.getClass().getSimpleName()
                    + ": " + t.getMessage());
        }

        // 取消优先于成功/失败：执行期间收到取消请求则一律以 CANCELLED 收口
        if (cancelRegistry.isCancelled(jobId)) {
            jobRepository.recordAttemptFinish(attemptId, "CANCELLED", "执行期间被取消");
            boolean moved = jobRepository.casStatus(jobId, RUNNING_ONLY, JobStatus.CANCELLED,
                    null, "CANCELLED: 执行期间被取消");
            log.info("job={} 第{}次尝试结束 -> CANCELLED，判定依据: 取消标记存在 (迁移成功={})",
                    jobId, attemptNo, moved);
            if (moved) {
                propagator.propagateUnreachable(jobId, "作业被取消");
            }
            cancelRegistry.clear(jobId);
            signals.wakeup();
            return;
        }

        if (result.success()) {
            jobRepository.recordAttemptFinish(attemptId, "SUCCESS", null);
            boolean moved = jobRepository.casStatus(jobId, RUNNING_ONLY, JobStatus.SUCCESS,
                    null, null);
            log.info("job={} 第{}次尝试结束 -> SUCCESS (迁移成功={})", jobId, attemptNo, moved);
            signals.wakeup(); // 成功可能解锁下游
            return;
        }

        // 失败路径
        String failureMsg = result.message();
        jobRepository.recordAttemptFinish(attemptId, "FAILED", failureMsg);
        if (attemptNo >= job.getMaxAttempts()) {
            String reason = "FAILED: 重试耗尽（共 " + attemptNo + " 次），末次原因: " + failureMsg;
            boolean moved = jobRepository.casStatus(jobId, RUNNING_ONLY, JobStatus.FAILED,
                    null, reason);
            log.info("job={} 第{}次尝试失败且重试耗尽 -> FAILED (迁移成功={}) reason={}",
                    jobId, attemptNo, moved, reason);
            if (moved) {
                propagator.propagateUnreachable(jobId, reason);
            }
            signals.wakeup();
        } else {
            long backoff = Math.round(job.getBackoffBaseMs()
                    * Math.pow(job.getBackoffMultiplier(), attemptNo - 1));
            long runAfter = System.currentTimeMillis() + backoff;
            boolean moved = jobRepository.casStatus(jobId, RUNNING_ONLY, JobStatus.QUEUED,
                    runAfter, "第 " + attemptNo + " 次尝试失败: " + failureMsg
                            + "；退避 " + backoff + "ms 后重试");
            log.info("job={} 第{}/{}次尝试失败 -> QUEUED（退避 {}ms，runAfter={}）(迁移成功={})",
                    jobId, attemptNo, job.getMaxAttempts(), backoff, runAfter, moved);
            // 退避结束后唤醒调度；即便唤醒丢失，下一次 tick 也会自然兜底
            Thread.ofVirtual().start(() -> {
                try {
                    Thread.sleep(backoff + 10L);
                    signals.wakeup();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        if (thrown != null) {
            log.debug("job={} 执行异常详情", jobId, thrown);
        }
    }
}
