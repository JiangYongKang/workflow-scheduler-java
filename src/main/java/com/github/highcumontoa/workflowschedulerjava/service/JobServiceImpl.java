package com.github.highcumontoa.workflowschedulerjava.service;

import com.github.highcumontoa.workflowschedulerjava.config.SchedulerProperties;
import com.github.highcumontoa.workflowschedulerjava.dag.DagValidator;
import com.github.highcumontoa.workflowschedulerjava.dto.BatchSubmitResponse;
import com.github.highcumontoa.workflowschedulerjava.dto.JobResponse;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitBatchRequest;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobRequest;
import com.github.highcumontoa.workflowschedulerjava.error.ErrorCode;
import com.github.highcumontoa.workflowschedulerjava.error.SchedulerException;
import com.github.highcumontoa.workflowschedulerjava.handler.HandlerRegistry;
import com.github.highcumontoa.workflowschedulerjava.model.Job;
import com.github.highcumontoa.workflowschedulerjava.model.JobStatus;
import com.github.highcumontoa.workflowschedulerjava.quota.CancelRegistry;
import com.github.highcumontoa.workflowschedulerjava.repo.JobRepository;
import com.github.highcumontoa.workflowschedulerjava.scheduler.DependencyPropagator;
import com.github.highcumontoa.workflowschedulerjava.scheduler.SchedulerSignals;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/** JobService 的持久化实现。 */
@Service
public class JobServiceImpl implements JobService {

    private static final Logger log = LoggerFactory.getLogger(JobServiceImpl.class);

    private static final String DEFAULT_QUEUE = "default";
    private static final String DEFAULT_TENANT = "default";

    private final JobRepository jobRepository;
    private final DagValidator dagValidator;
    private final HandlerRegistry handlerRegistry;
    private final CancelRegistry cancelRegistry;
    private final DependencyPropagator propagator;
    private final SchedulerSignals signals;
    private final SchedulerProperties properties;

    public JobServiceImpl(JobRepository jobRepository, DagValidator dagValidator,
                          HandlerRegistry handlerRegistry, CancelRegistry cancelRegistry,
                          DependencyPropagator propagator, SchedulerSignals signals,
                          SchedulerProperties properties) {
        this.jobRepository = jobRepository;
        this.dagValidator = dagValidator;
        this.handlerRegistry = handlerRegistry;
        this.cancelRegistry = cancelRegistry;
        this.propagator = propagator;
        this.signals = signals;
        this.properties = properties;
    }

    @Override
    public JobResponse submit(SubmitJobRequest request) {
        validateCommon(request);

        // 幂等：先查，命中则原样返回，绝不产生第二行 / 第二次计数
        if (request.idempotencyKey() != null && !request.idempotencyKey().isBlank()) {
            Job existing = jobRepository.findByIdempotencyKey(request.idempotencyKey());
            if (existing != null) {
                log.info("幂等命中 idempotencyKey={} -> 返回既有作业 job={} status={}（不重复执行/计数）",
                        request.idempotencyKey(), existing.getId(), existing.getStatus());
                return toResponse(existing, true);
            }
        }

        String id = (request.id() == null || request.id().isBlank())
                ? "job-" + UUID.randomUUID() : request.id();
        if (jobRepository.findById(id) != null) {
            throw new SchedulerException(ErrorCode.INVALID_REQUEST, "作业 id 已存在: " + id);
        }
        dagValidator.validateSingle(request);

        Job job = buildJob(id, request);
        Job saved = jobRepository.insertIfAbsent(job);
        if (saved == null) {
            // 并发下另一个相同幂等键的请求抢先落库
            Job existing = jobRepository.findByIdempotencyKey(request.idempotencyKey());
            if (existing != null) {
                log.info("幂等竞争命中 idempotencyKey={} -> 返回既有作业 job={}",
                        request.idempotencyKey(), existing.getId());
                return toResponse(existing, true);
            }
            throw new SchedulerException(ErrorCode.INVALID_REQUEST, "作业 id 已存在: " + id);
        }
        signals.wakeup();
        return toResponse(saved, false);
    }

    @Override
    @Transactional
    public BatchSubmitResponse submitBatch(SubmitBatchRequest request) {
        if (request == null || request.jobs() == null || request.jobs().isEmpty()) {
            throw new SchedulerException(ErrorCode.INVALID_REQUEST, "jobs 不能为空");
        }

        // 先解析幂等命中（同一批次内允许复用既有幂等键，不新建）
        List<JobResponse> responses = new ArrayList<>();
        List<SubmitJobRequest> toCreate = new ArrayList<>();
        Set<String> newIds = new HashSet<>();
        for (SubmitJobRequest r : request.jobs()) {
            validateCommon(r);
            if (r.idempotencyKey() != null && !r.idempotencyKey().isBlank()) {
                Job existing = jobRepository.findByIdempotencyKey(r.idempotencyKey());
                if (existing != null) {
                    log.info("批量提交幂等命中 idempotencyKey={} -> job={}",
                            r.idempotencyKey(), existing.getId());
                    responses.add(toResponse(existing, true));
                    continue;
                }
            }
            toCreate.add(r);
            if (r.id() != null && !r.id().isBlank()) {
                newIds.add(r.id());
            }
        }

        // 为缺省 id 的作业统一分配 id，并让同批次其他作业可以引用这些 id
        List<Job> built = new ArrayList<>();
        Set<String> generatedIds = new HashSet<>();
        for (SubmitJobRequest r : toCreate) {
            String id = (r.id() == null || r.id().isBlank())
                    ? "job-" + UUID.randomUUID() : r.id();
            generatedIds.add(id);
            built.add(buildJob(id, r));
        }
        newIds.addAll(generatedIds);

        // 幂等键在本批次内不得重复
        Set<String> batchKeys = new HashSet<>();
        for (SubmitJobRequest r : toCreate) {
            if (r.idempotencyKey() != null && !batchKeys.add(r.idempotencyKey())) {
                throw new SchedulerException(ErrorCode.INVALID_REQUEST,
                        "批内幂等键重复: " + r.idempotencyKey());
            }
        }

        // 在接受前统一做 DAG 校验（任何一个失败，整批拒绝，不产生部分作业）
        dagValidator.validateBatch(toCreate, newIds);

        for (Job job : built) {
            if (jobRepository.findById(job.getId()) != null) {
                throw new SchedulerException(ErrorCode.INVALID_REQUEST,
                        "作业 id 已存在: " + job.getId());
            }
            Job saved = jobRepository.insertIfAbsent(job);
            if (saved == null) {
                Job existing = job.getIdempotencyKey() == null ? null
                        : jobRepository.findByIdempotencyKey(job.getIdempotencyKey());
                if (existing != null) {
                    log.info("批量提交幂等竞争命中 idempotencyKey={} -> job={}",
                            job.getIdempotencyKey(), existing.getId());
                    responses.add(toResponse(existing, true));
                    continue;
                }
                throw new SchedulerException(ErrorCode.INVALID_REQUEST,
                        "作业 id 已存在: " + job.getId());
            }
            responses.add(toResponse(saved, false));
        }
        signals.wakeup();
        return new BatchSubmitResponse(responses);
    }

    @Override
    public JobResponse get(String id) {
        Job job = requireJob(id);
        return toResponse(job, false);
    }

    @Override
    public JobResponse getByIdempotencyKey(String key) {
        if (key == null || key.isBlank()) {
            throw new SchedulerException(ErrorCode.INVALID_REQUEST, "idempotencyKey 不能为空");
        }
        Job job = jobRepository.findByIdempotencyKey(key);
        if (job == null) {
            throw new SchedulerException(ErrorCode.JOB_NOT_FOUND,
                    "幂等键对应的作业不存在: " + key);
        }
        return toResponse(job, false);
    }

    @Override
    public JobResponse cancel(String id) {
        Job job = requireJob(id);
        if (job.getStatus().isTerminal()) {
            throw new SchedulerException(ErrorCode.ALREADY_TERMINAL,
                    "作业已处于终态 " + job.getStatus() + "，不可取消: " + id);
        }
        // 打上内存取消标记（若正在执行，执行体会尽快中止）
        cancelRegistry.mark(id);
        // CAS 直接收口非 RUNNING 作业；RUNNING 作业交由执行体检测标记后迁移
        boolean moved = jobRepository.casStatus(
                id, Set.of(JobStatus.PENDING, JobStatus.QUEUED),
                JobStatus.CANCELLED, null, "CANCELLED: 用户取消（尚未开始执行）");
        Job after = jobRepository.findById(id);
        if (moved) {
            log.info("job={} 用户取消 -> CANCELLED（排队中作业直接收口），并级联标记下游不可达", id);
            propagator.propagateUnreachable(id, "上游作业被用户取消");
            after = jobRepository.findById(id);
        } else {
            log.info("job={} 用户取消：作业处于 {}，已置取消标记，等待执行体收口",
                    id, after.getStatus());
        }
        signals.wakeup();
        return toResponse(after, false);
    }

    private void validateCommon(SubmitJobRequest r) {
        if (r == null) {
            throw new SchedulerException(ErrorCode.INVALID_REQUEST, "请求体为空");
        }
        if (r.jobType() == null || r.jobType().isBlank()) {
            throw new SchedulerException(ErrorCode.INVALID_REQUEST, "jobType 不能为空");
        }
        if (!handlerRegistry.exists(r.jobType())) {
            throw new SchedulerException(ErrorCode.UNKNOWN_JOB_TYPE,
                    "未知任务类型: " + r.jobType());
        }
    }

    private Job buildJob(String id, SubmitJobRequest r) {
        long now = System.currentTimeMillis();
        Job job = new Job();
        job.setId(id);
        job.setIdempotencyKey(blankToNull(r.idempotencyKey()));
        job.setJobType(r.jobType());
        job.setPayload(r.payload());
        job.setQueueName(blankToDefault(r.queue(), DEFAULT_QUEUE));
        job.setTenant(blankToDefault(r.tenant(), DEFAULT_TENANT));
        job.setStatus(hasDeps(r) ? JobStatus.PENDING : JobStatus.QUEUED);
        job.setAttempts(0);
        job.setMaxAttempts(r.maxAttempts() != null ? r.maxAttempts() : properties.getDefaultMaxAttempts());
        job.setBackoffBaseMs(r.backoffBaseMs() != null ? r.backoffBaseMs()
                : properties.getDefaultBackoffBaseMs());
        job.setBackoffMultiplier(r.backoffMultiplier() != null ? r.backoffMultiplier()
                : properties.getDefaultBackoffMultiplier());
        job.setRunAfter(0L);
        job.setCreatedAt(now);
        job.setUpdatedAt(now);
        if (r.dependencies() != null) {
            job.setDependencies(r.dependencies().stream().sorted().toList());
        }
        if (job.getMaxAttempts() < 1) {
            throw new SchedulerException(ErrorCode.INVALID_REQUEST, "maxAttempts 必须 >= 1");
        }
        return job;
    }

    private boolean hasDeps(SubmitJobRequest r) {
        return r.dependencies() != null && !r.dependencies().isEmpty();
    }

    private Job requireJob(String id) {
        Job job = jobRepository.findById(id);
        if (job == null) {
            throw new SchedulerException(ErrorCode.JOB_NOT_FOUND, "作业不存在: " + id);
        }
        return job;
    }

    private String blankToNull(String s) {
        return (s == null || s.isBlank()) ? null : s;
    }

    private String blankToDefault(String s, String dflt) {
        return (s == null || s.isBlank()) ? dflt : s;
    }

    private JobResponse toResponse(Job job, boolean deduplicated) {
        return new JobResponse(
                job.getId(),
                job.getIdempotencyKey(),
                job.getStatus().name(),
                job.getJobType(),
                job.getQueueName(),
                job.getTenant(),
                job.getAttempts(),
                job.getMaxAttempts(),
                job.getFailureReason(),
                job.getDependencies(),
                job.getCreatedAt(),
                job.getUpdatedAt(),
                deduplicated);
    }
}
