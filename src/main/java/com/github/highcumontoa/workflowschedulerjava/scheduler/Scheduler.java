package com.github.highcumontoa.workflowschedulerjava.scheduler;

import com.github.highcumontoa.workflowschedulerjava.config.SchedulerProperties;
import com.github.highcumontoa.workflowschedulerjava.model.Job;
import com.github.highcumontoa.workflowschedulerjava.model.JobStatus;
import com.github.highcumontoa.workflowschedulerjava.quota.CancelRegistry;
import com.github.highcumontoa.workflowschedulerjava.quota.QuotaManager;
import com.github.highcumontoa.workflowschedulerjava.repo.JobRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 调度核心：单线程周期 tick 负责准入（依赖判定 + 配额 + CAS），固定大小工作线程池负责执行。
 * 准入与状态迁移全部以数据库条件更新为准，保证并发竞争下结果确定：
 * <ul>
 *   <li>准入单线程 ⇒ 配额观察与 CAS 之间不会有第二个准入者；</li>
 *   <li>CAS 要求旧状态 ∈ {PENDING, QUEUED} ⇒ 即便多 tick 重叠也不可能重复派发；</li>
 *   <li>CAS 成功时 attempts+1 ⇒ “实际执行次数”由数据库权威计数。</li>
 * </ul>
 */
@Component
public class Scheduler {

    private static final Logger log = LoggerFactory.getLogger(Scheduler.class);

    /** 可被准入的非终态状态。 */
    static final Set<JobStatus> ADMITTABLE = Set.of(JobStatus.PENDING, JobStatus.QUEUED);

    /** 判定依赖时阻断下游的上游状态。 */
    private static final Set<JobStatus> BLOCKING_UPSTREAM =
            Set.of(JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.UNREACHABLE);

    private final JobRepository jobRepository;
    private final QuotaManager quotaManager;
    private final CancelRegistry cancelRegistry;
    private final SchedulerProperties properties;
    private final JobRunner jobRunner;
    private final SchedulerSignals signals;
    private final DependencyPropagator propagator;

    private volatile boolean running = false;
    private Thread tickThread;
    private ExecutorService workerPool;

    public Scheduler(JobRepository jobRepository, QuotaManager quotaManager,
                     CancelRegistry cancelRegistry, SchedulerProperties properties,
                     JobRunner jobRunner, SchedulerSignals signals,
                     DependencyPropagator propagator) {
        this.jobRepository = jobRepository;
        this.quotaManager = quotaManager;
        this.cancelRegistry = cancelRegistry;
        this.properties = properties;
        this.jobRunner = jobRunner;
        this.signals = signals;
        this.propagator = propagator;
    }

    @PostConstruct
    public void start() {
        // 1) 崩溃恢复：遗留 RUNNING 回 QUEUED 重跑；SUCCESS/FAILED 等终态原样保留
        int recovered = jobRepository.recoverRunningToQueued();
        if (recovered > 0) {
            log.info("重启恢复完成：{} 个崩溃前在执行的作业回到 QUEUED，将按 at-least-once 重新执行", recovered);
        }

        workerPool = Executors.newFixedThreadPool(properties.getWorkerThreads(), r -> {
            Thread t = new Thread(r, "scheduler-worker");
            t.setDaemon(true);
            return t;
        });
        running = true;
        tickThread = Thread.ofPlatform().name("scheduler-tick").daemon(true).start(this::tickLoop);
        log.info("调度器已启动 tickIntervalMs={} workerThreads={} globalLimit={}",
                properties.getTickIntervalMs(), properties.getWorkerThreads(),
                properties.getGlobalLimit());
    }

    private void tickLoop() {
        while (running) {
            try {
                tick();
                signals.await(properties.getTickIntervalMs());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Throwable t) {
                log.error("调度 tick 发生异常，下一轮继续", t);
            }
        }
    }

    @PreDestroy
    public void stop() {
        log.info("调度器正在停止……");
        running = false;
        signals.wakeup();
        if (tickThread != null) {
            tickThread.interrupt();
        }
        if (workerPool != null) {
            workerPool.shutdown();
            try {
                workerPool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** 单次调度循环（包私有，测试可直接驱动）。 */
    void tick() {
        long now = System.currentTimeMillis();
        List<Job> candidates = jobRepository.findByStatuses(
                JobStatus.PENDING, JobStatus.QUEUED);
        // 先进先出：按创建时间排序，同批次按 id 稳定排序
        candidates.sort(Comparator.comparingLong(Job::getCreatedAt).thenComparing(Job::getId));
        for (Job job : candidates) {
            if (job.getRunAfter() > now) {
                continue; // 退避窗口未结束
            }
            DependencyVerdict verdict = evaluateDependencies(job);
            if (verdict == DependencyVerdict.BLOCKED) {
                // 兜底：正常路径会在失败瞬间级联，这里处理历史/异常数据
                String reason = blockingReason(blockingUpstreamId(job));
                log.warn("job={} tick 判定依赖 BLOCKED，兜底级联 UNREACHABLE，判定依据: {}",
                        job.getId(), reason);
                if (jobRepository.casStatus(job.getId(), ADMITTABLE,
                        JobStatus.UNREACHABLE, null, "UNREACHABLE: " + reason)) {
                    propagator.propagateUnreachable(job.getId(), reason);
                }
                continue;
            }
            if (verdict == DependencyVerdict.WAIT) {
                continue;
            }
            // READY：检查三维配额
            if (!quotaManager.tryAcquire(job.getQueueName(), job.getTenant())) {
                log.debug("job={} 依赖已满足但配额不足，保持 {} 排队", job.getId(), job.getStatus());
                continue;
            }
            // CAS 准入：成功即权威地成为 RUNNING 且 attempts+1
            boolean admitted = jobRepository.casStatus(
                    job.getId(), ADMITTABLE, JobStatus.RUNNING, null, null);
            if (!admitted) {
                log.info("job={} 准入 CAS 失败，判定依据: 已被并发迁移，跳过本轮", job.getId());
                continue;
            }
            cancelRegistry.clear(job.getId());
            log.info("job={} 准入成功 -> 派发到工作线程", job.getId());
            workerPool.submit(() -> {
                try {
                    jobRunner.run(job.getId());
                } catch (Throwable t) {
                    log.error("job={} 工作线程未捕获异常", job.getId(), t);
                }
            });
        }
    }

    /** 依赖判定：全部上游 SUCCESS → READY；出现 FAILED/CANCELLED/UNREACHABLE → BLOCKED；其余 WAIT。 */
    public enum DependencyVerdict { READY, WAIT, BLOCKED }

    public DependencyVerdict evaluateDependencies(Job job) {
        List<String> deps = job.getDependencies();
        if (deps == null || deps.isEmpty()) {
            return DependencyVerdict.READY;
        }
        List<Job> upstreams = jobRepository.findByIds(deps);
        Map<String, Job> byId = upstreams.stream()
                .collect(Collectors.toMap(Job::getId, j -> j));
        boolean waiting = false;
        for (String dep : deps) {
            Job up = byId.get(dep);
            if (up == null) {
                // 理论上提交期已拦截；防御性视为等待
                log.warn("job={} 上游 {} 不存在，按 WAIT 处理", job.getId(), dep);
                waiting = true;
                continue;
            }
            if (up.getStatus() == JobStatus.SUCCESS) {
                continue;
            }
            if (BLOCKING_UPSTREAM.contains(up.getStatus())) {
                log.debug("job={} 依赖判定 BLOCKED，判定依据: 上游 {} 状态为 {}",
                        job.getId(), dep, up.getStatus());
                return DependencyVerdict.BLOCKED;
            }
            waiting = true;
        }
        return waiting ? DependencyVerdict.WAIT : DependencyVerdict.READY;
    }

    /** 兼容旧签名：无作业对象时从库中装载后判定。 */
    public DependencyVerdict evaluateDependencies(String jobId) {
        Job job = jobRepository.findById(jobId);
        if (job == null) {
            return DependencyVerdict.WAIT;
        }
        return evaluateDependencies(job);
    }

    /** 返回第一个阻断性上游的 id（调用方应先确认判定为 BLOCKED）。 */
    private String blockingUpstreamId(Job job) {
        if (job.getDependencies() == null) {
            return null;
        }
        List<Job> upstreams = jobRepository.findByIds(job.getDependencies());
        Map<String, JobStatus> statuses = upstreams.stream()
                .collect(Collectors.toMap(Job::getId, Job::getStatus));
        for (String dep : job.getDependencies()) {
            JobStatus s = statuses.get(dep);
            if (s != null && BLOCKING_UPSTREAM.contains(s)) {
                return dep + "(" + s + ")";
            }
        }
        return null;
    }

    private String blockingReason(String upstream) {
        return "上游 " + upstream + " 已失败/取消/不可达（tick 兜底判定）";
    }
}
