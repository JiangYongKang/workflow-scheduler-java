package com.github.highcumontoa.workflowschedulerjava.engine;

import com.github.highcumontoa.workflowschedulerjava.config.SchedulerProperties;
import com.github.highcumontoa.workflowschedulerjava.dto.JobView;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitBatchRequest;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobRequest;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobResponse;
import com.github.highcumontoa.workflowschedulerjava.exception.ErrorCode;
import com.github.highcumontoa.workflowschedulerjava.exception.SchedulerException;
import com.github.highcumontoa.workflowschedulerjava.model.AttemptRecord;
import com.github.highcumontoa.workflowschedulerjava.model.AttemptResult;
import com.github.highcumontoa.workflowschedulerjava.model.Job;
import com.github.highcumontoa.workflowschedulerjava.model.JobState;
import com.github.highcumontoa.workflowschedulerjava.model.RetryPolicy;
import com.github.highcumontoa.workflowschedulerjava.store.JobStore;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 调度核心。
 *
 * <p>所有作业图与配额账本的读写都在 {@link #lock} 内完成；{@link #tick()} 由单线程调度循环
 * 周期调用，作业实际执行交给 {@link #workers}。执行结果通过 {@link #outcomes} 回送调度线程，
 * 保证状态迁移串行化、可解释。提交/取消接口同样在锁内完成，先持久化后返回。</p>
 */
@Component
public class SchedulerEngine {

    private static final Logger log = LoggerFactory.getLogger(SchedulerEngine.class);

    private final JobStore store;
    private final TaskHandler taskHandler;
    private final SchedulerProperties props;
    private final DagValidator validator = new DagValidator();

    private final Object lock = new Object();
    private final Map<String, Job> jobs = new HashMap<>();
    /** idempotencyKey -> jobId，保证重复提交命中同一作业。 */
    private final Map<String, String> idempotencyIndex = new HashMap<>();
    /** 下游索引：jobId -> 直接下游。 */
    private final Map<String, List<String>> downstream = new HashMap<>();
    /**
     * 正在执行的运行槽位：记录该槽位属于哪一次尝试及其 Future。
     * 槽位与尝试一一对应——超时判定释放的是旧槽位，新尝试持有的是新槽位，
     * 迟到结果只能碰到旧槽位，绝不可能摘掉正在运行的那次尝试所占的名额。
     */
    private record RunningSlot(int attempt, Future<?> future) {
    }

    /** 正在执行的作业（锁外运行）：jobId -> 当前占着运行名额的槽位。 */
    private final Map<String, RunningSlot> running = new HashMap<>();
    /** 待重试就绪时间：jobId -> epoch ms。 */
    private final Map<String, Long> retryAfter = new HashMap<>();

    private final LinkedBlockingQueue<Outcome> outcomes = new LinkedBlockingQueue<>();

    private Thread schedulerThread;
    /** 调度器是否处于活跃（start 后、stop 前）状态。停机后一切后台回调都必须是空操作。 */
    private volatile boolean active;
    private volatile boolean runningFlag;
    /** 暂停开关：为 true 时不再启动新执行，运行中的作业自然跑完，排队作业保留。持久化到 meta。 */
    private volatile boolean paused;
    private ThreadPoolExecutor workers;
    /** 单作业执行超时的计时线程（仅对配置了 timeoutMillis 的作业生效）。 */
    private ScheduledExecutorService timeoutScheduler;
    private final AtomicLong idSeq = new AtomicLong();

    public SchedulerEngine(JobStore store, TaskHandler taskHandler, SchedulerProperties props) {
        this.store = store;
        this.taskHandler = taskHandler;
        this.props = props;
    }

    /* ============================= 生命周期 ============================= */

    @PostConstruct
    public void start() {
        int poolSize = Math.max(1, props.getGlobalConcurrency());
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "scheduler-worker");
            t.setDaemon(true);
            return t;
        };
        this.workers = new ThreadPoolExecutor(poolSize, poolSize,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), tf);
        this.timeoutScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "scheduler-timeout");
            t.setDaemon(true);
            return t;
        });
        recover();
        active = true;
        runningFlag = true;
        schedulerThread = new Thread(this::loop, "scheduler-loop");
        schedulerThread.setDaemon(true);
        schedulerThread.start();
        log.info("调度器已启动 globalConcurrency={} tickMillis={}", props.getGlobalConcurrency(), props.getTickMillis());
    }

    @PreDestroy
    public void stop() {
        // 先关掉活跃开关：此后工作线程迟到的结果、超时定时器的触发都不会再进入
        // 锁内做状态迁移或写盘，保证停机后无任何后台活动推进状态。
        active = false;
        runningFlag = false;
        if (schedulerThread != null) {
            schedulerThread.interrupt();
        }
        if (timeoutScheduler != null) {
            timeoutScheduler.shutdownNow();
        }
        workers.shutdownNow();
        try {
            workers.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** 启动恢复：重建索引，崩溃时 RUNNING/QUEUED 的作业安全回置并重新传播。 */
    private void recover() {
        synchronized (lock) {
            jobs.clear();
            idempotencyIndex.clear();
            downstream.clear();
            running.clear();
            retryAfter.clear();
            List<Job> loaded = store.loadAll();
            int recovered = 0;
            for (Job job : loaded) {
                jobs.put(job.getId(), job);
                rebuildIndices(job);
                if (job.getIdempotencyKey() != null) {
                    idempotencyIndex.put(job.getIdempotencyKey(), job.getId());
                }
                if (job.getState() == JobState.RUNNING) {
                    // 崩溃时正在执行：attempts 已持久化，回置 PENDING 重新调度（SUCCESS 永不重复，执行采用 at-least-once）
                    job.setState(JobState.PENDING);
                    job.setDetail("进程重启恢复：中断的执行重新调度，已尝试次数 " + job.getAttempts() + " 计入重试预算");
                    log.warn("job={} 重启恢复 RUNNING->PENDING（已尝试 {} 次）", job.getId(), job.getAttempts());
                    store.put(job);
                    closeInterruptedAttempt(job);
                    recovered++;
                } else if (job.getState() == JobState.QUEUED) {
                    job.setState(JobState.PENDING);
                    job.setDetail("进程重启恢复：排队作业重新等待配额");
                    store.put(job);
                    recovered++;
                }
            }
            long maxSeq = loaded.stream().map(Job::getId)
                    .filter(s -> s.startsWith("job-"))
                    .map(s -> s.substring(4))
                    .map(this::leadingDigits)
                    .filter(s -> !s.isEmpty())
                    .mapToLong(Long::parseLong).max().orElse(0);
            idSeq.set(maxSeq);
            // 暂停开关随持久化恢复：重启前暂停的，重启后仍保持暂停，作业不会被卡死或重复执行
            paused = Boolean.parseBoolean(store.getMeta("paused"));
            // 恢复后重新计算不可达传播，避免崩溃发生在传播中途
            propagateUnreachable();
            log.info("恢复完成：载入作业 {} 个，重新调度 {} 个，paused={}", loaded.size(), recovered, paused);
        }
    }

    private String leadingDigits(String s) {
        int i = 0;
        while (i < s.length() && Character.isDigit(s.charAt(i))) {
            i++;
        }
        return s.substring(0, i);
    }

    private void rebuildIndices(Job job) {
        for (String dep : job.getDependencies()) {
            downstream.computeIfAbsent(dep, k -> new ArrayList<>()).add(job.getId());
        }
    }

    /* ============================= 提交 ============================= */

    public List<SubmitJobResponse> submit(SubmitBatchRequest request) {
        if (request == null || request.getJobs() == null || request.getJobs().isEmpty()) {
            throw new SchedulerException(ErrorCode.INVALID_REQUEST, "jobs 不能为空");
        }
        synchronized (lock) {
            // 与请求一一对应的响应（按请求顺序返回）
            SubmitJobResponse[] responses = new SubmitJobResponse[request.getJobs().size()];
            List<Job> newJobs = new ArrayList<>();
            for (SubmitJobRequest r : request.getJobs()) {
                newJobs.add(toJob(r));
            }

            // 1) 与存量 id 冲突：仅当携带相同幂等键且指向同一作业时，允许走幂等返回
            for (Job job : newJobs) {
                Job existing = jobs.get(job.getId());
                if (existing != null
                        && (job.getIdempotencyKey() == null
                        || !job.getIdempotencyKey().equals(existing.getIdempotencyKey())
                        || !job.getId().equals(idempotencyIndex.get(job.getIdempotencyKey())))) {
                    throw new SchedulerException(ErrorCode.JOB_ALREADY_EXISTS,
                            "作业 id 已存在: " + job.getId());
                }
            }

            // 2) 幂等键：相同 key 命中既有/批内作业；key 相同但内容不一致 -> 冲突
            List<Job> toAccept = new ArrayList<>();
            Set<String> batchKeys = new HashSet<>();
            for (int i = 0; i < newJobs.size(); i++) {
                Job job = newJobs.get(i);
                String key = job.getIdempotencyKey();
                if (key != null && (batchKeys.contains(key) || idempotencyIndex.containsKey(key)
                        || jobs.containsKey(job.getId()))) {
                    Job hit = jobs.get(idempotencyIndex.get(key));
                    if (hit == null) {
                        hit = jobs.get(job.getId());
                    }
                    if (hit == null) {
                        final String k = key;
                        hit = toAccept.stream().filter(j -> k.equals(j.getIdempotencyKey())).findFirst().orElse(null);
                    }
                    if (hit != null && !sameSemanticPayload(hit, job)) {
                        throw new SchedulerException(ErrorCode.IDEMPOTENCY_KEY_CONFLICT,
                                "幂等键 " + key + " 已存在但请求内容不一致");
                    }
                    String hitId = hit != null ? hit.getId() : job.getId();
                    String state = hit != null ? hit.getState().name() : JobState.PENDING.name();
                    responses[i] = new SubmitJobResponse(hitId, state, true,
                            "幂等键 " + key + " 命中既有作业，未重复提交");
                    log.info("幂等命中 key={} -> job={} duplicate=true state={}", key, hitId, state);
                } else {
                    if (key != null) {
                        batchKeys.add(key);
                    }
                    toAccept.add(job);
                }
            }

            // 3) DAG 校验（批次内重复 id / 缺失上游 / 环），在任何持久化之前整批拒绝
            Set<String> knownIds = new HashSet<>(jobs.keySet());
            Map<String, List<String>> mergedGraph = new HashMap<>();
            for (Job j : jobs.values()) {
                mergedGraph.put(j.getId(), j.getDependencies());
            }
            for (Job j : toAccept) {
                mergedGraph.put(j.getId(), j.getDependencies());
            }
            validator.validate(toAccept, knownIds, mergedGraph::get);

            // 4) 接受：持久化 + 建索引（整批校验通过后才落盘）
            long now = System.currentTimeMillis();
            for (int i = 0; i < newJobs.size(); i++) {
                Job job = newJobs.get(i);
                if (responses[i] != null) {
                    continue;
                }
                job.setSubmittedAt(now);
                job.setDetail("已接受，等待上游完成与配额");
                jobs.put(job.getId(), job);
                rebuildIndices(job);
                if (job.getIdempotencyKey() != null) {
                    idempotencyIndex.put(job.getIdempotencyKey(), job.getId());
                }
                store.put(job);
                responses[i] = new SubmitJobResponse(job.getId(), job.getState().name(), false, "已接受");
                log.info("job={} 已接受 queue={} tenant={} deps={}",
                        job.getId(), job.getQueue(), job.getTenant(), job.getDependencies());
            }
            lock.notifyAll();
            return List.of(responses);
        }
    }

    private boolean sameSemanticPayload(Job a, Job b) {
        return java.util.Objects.equals(a.getTaskType(), b.getTaskType())
                && a.getTaskParam() == b.getTaskParam()
                && java.util.Objects.equals(a.getDependencies(), b.getDependencies())
                && java.util.Objects.equals(a.getQueue(), b.getQueue())
                && java.util.Objects.equals(a.getTenant(), b.getTenant())
                && java.util.Objects.equals(a.getTimeoutMillis(), b.getTimeoutMillis());
    }

    private Job toJob(SubmitJobRequest r) {
        Job job = new Job();
        String id = (r.getId() == null || r.getId().isBlank())
                ? "job-" + idSeq.incrementAndGet() + "-" + UUID.randomUUID().toString().substring(0, 8)
                : r.getId();
        job.setId(id);
        job.setName(r.getName() != null ? r.getName() : id);
        job.setIdempotencyKey(r.getIdempotencyKey());
        job.setQueue(r.getQueue() != null ? r.getQueue() : "default");
        job.setTenant(r.getTenant() != null ? r.getTenant() : "default");
        job.setDependencies(r.getDependencies() != null ? new ArrayList<>(r.getDependencies()) : new ArrayList<>());
        job.setTaskType(r.getTaskType() != null ? r.getTaskType() : "noop");
        job.setTaskParam(r.getTaskParam() != null ? r.getTaskParam() : 0L);
        RetryPolicy rp = r.getRetryPolicy();
        job.setRetryPolicy(new RetryPolicy(
                rp != null && rp.getMaxAttempts() > 0 ? rp.getMaxAttempts() : props.getDefaultMaxAttempts(),
                rp != null ? rp.getBackoffMillis() : props.getDefaultBackoffMillis()));
        Long timeout = r.getTimeoutMillis();
        job.setTimeoutMillis(timeout != null && timeout > 0 ? timeout : null);
        job.setState(JobState.PENDING);
        return job;
    }

    /* ============================= 查询 / 取消 ============================= */

    public JobView get(String id) {
        synchronized (lock) {
            Job job = jobs.get(id);
            if (job == null) {
                throw new SchedulerException(ErrorCode.JOB_NOT_FOUND, "作业不存在: " + id);
            }
            return JobView.of(job);
        }
    }

    public List<JobView> list() {
        synchronized (lock) {
            return jobs.values().stream()
                    .sorted((a, b) -> Long.compare(a.getSubmittedAt(), b.getSubmittedAt()))
                    .map(JobView::of).toList();
        }
    }

    /** 按尝试序号升序返回某作业的全部执行记录。 */
    public List<AttemptRecord> listAttempts(String id) {
        synchronized (lock) {
            if (!jobs.containsKey(id)) {
                throw new SchedulerException(ErrorCode.JOB_NOT_FOUND, "作业不存在: " + id);
            }
            return store.listAttempts(id);
        }
    }

    public JobView cancel(String id) {
        synchronized (lock) {
            Job job = jobs.get(id);
            if (job == null) {
                throw new SchedulerException(ErrorCode.JOB_NOT_FOUND, "作业不存在: " + id);
            }
            switch (job.getState()) {
                case SUCCESS -> throw new SchedulerException(ErrorCode.CANCEL_NOT_ALLOWED,
                        "作业已成功，不可取消: " + id);
                case FAILED, CANCELLED, UNREACHABLE -> {
                    log.info("job={} 取消请求被忽略，已处于终态 {}", id, job.getState());
                    return JobView.of(job);
                }
                case RUNNING -> {
                    job.setCancelRequested(true);
                    RunningSlot slot = running.get(id);
                    if (slot != null) {
                        slot.future().cancel(true);
                    }
                    job.setDetail("收到取消请求，正在协作中断执行");
                    store.put(job);
                    log.info("job={} 取消请求已送达执行线程（RUNNING，协作中断中）", id);
                    return JobView.of(job);
                }
                default -> {
                    transition(job, JobState.CANCELLED, null,
                            "PENDING/QUEUED 作业在执行前被取消");
                    propagateUnreachable();
                    wakeQueued();
                    lock.notifyAll();
                    return JobView.of(job);
                }
            }
        }
    }

    /* ============================= 暂停 / 恢复 ============================= */

    /** 暂停调度：不再启动新执行；运行中的作业自然跑完，排队作业保留原状态。 */
    public void pause() {
        synchronized (lock) {
            if (paused) {
                return;
            }
            paused = true;
            store.putMeta("paused", "true");
            log.info("调度器已暂停：不再启动新执行，运行中的作业继续自然完成");
        }
    }

    /** 恢复调度：排队/等待的作业按原有依赖与配额规则继续推进。 */
    public void resume() {
        synchronized (lock) {
            if (!paused) {
                return;
            }
            paused = false;
            store.putMeta("paused", "false");
            lock.notifyAll();
            log.info("调度器已恢复：继续按依赖与配额调度");
        }
    }

    public boolean isPaused() {
        return paused;
    }

    /* ============================= 调度循环 ============================= */

    private void loop() {
        while (runningFlag) {
            try {
                drainOutcomes();
                synchronized (lock) {
                    tick();
                    lock.wait(props.getTickMillis());
                }
            } catch (InterruptedException e) {
                if (runningFlag) {
                    Thread.currentThread().interrupt();
                }
                return;
            } catch (Throwable t) {
                log.error("调度循环异常", t);
            }
        }
    }

    private void drainOutcomes() {
        List<Outcome> batch = new ArrayList<>();
        outcomes.drainTo(batch);
        if (batch.isEmpty()) {
            return;
        }
        synchronized (lock) {
            for (Outcome o : batch) {
                handleOutcome(o);
            }
            // 任何执行结束都可能释放配额，唤醒排队作业重新参与判定
            wakeQueued();
            lock.notifyAll();
        }
    }

    /** QUEUED -> PENDING，使其在下一次 tick 重新做依赖与配额判定。 */
    private void wakeQueued() {
        for (Job j : jobs.values()) {
            if (j.getState() == JobState.QUEUED) {
                j.setState(JobState.PENDING);
                j.setDetail("配额可能已释放，重新参与调度判定");
                store.put(j);
            }
        }
    }

    /** 一次调度：传播不可达 -> 判定就绪 -> 配额内启动。 */
    void tick() {
        propagateUnreachable();
        sweepCancelledFutures();
        if (paused) {
            // 暂停期间不启动任何新执行；PENDING/QUEUED 作业原样保留，运行中的自然跑完
            return;
        }
        long now = System.currentTimeMillis();
        int globalRunning = running.size();
        Map<String, Integer> queueRunning = new HashMap<>();
        Map<String, Integer> tenantRunning = new HashMap<>();
        for (String id : running.keySet()) {
            Job j = jobs.get(id);
            if (j != null) {
                queueRunning.merge(j.getQueue(), 1, Integer::sum);
                tenantRunning.merge(j.getTenant(), 1, Integer::sum);
            }
        }
        for (Job job : jobs.values()) {
            if (job.getState() != JobState.PENDING) {
                continue;
            }
            Long until = retryAfter.get(job.getId());
            if (until != null && now < until) {
                continue;
            }
            if (until != null && now >= until) {
                retryAfter.remove(job.getId());
            }
            String waiting = waitingOnDeps(job);
            if (waiting != null) {
                job.setDetail(waiting);
                continue;
            }
            // 三道配额：全局、队列、租户，任一超限都排队而不是失败
            if (globalRunning >= props.getGlobalConcurrency()) {
                markQueued(job, "全局并发已达上限 " + props.getGlobalConcurrency() + "，排队等待（不失败、不丢弃）");
                continue;
            }
            int qLimit = props.getQueueConcurrency().getOrDefault(job.getQueue(), props.getDefaultQueueConcurrency());
            if (queueRunning.getOrDefault(job.getQueue(), 0) >= qLimit) {
                markQueued(job, "队列 " + job.getQueue() + " 并发已达上限 " + qLimit + "，排队等待");
                continue;
            }
            int tLimit = props.getTenantConcurrency().getOrDefault(job.getTenant(), props.getDefaultTenantConcurrency());
            if (tenantRunning.getOrDefault(job.getTenant(), 0) >= tLimit) {
                markQueued(job, "租户 " + job.getTenant() + " 并发已达上限 " + tLimit + "，排队等待");
                continue;
            }
            launch(job);
            globalRunning++;
            queueRunning.merge(job.getQueue(), 1, Integer::sum);
            tenantRunning.merge(job.getTenant(), 1, Integer::sum);
        }
    }

    /**
     * 兜底收尾：RUNNING 作业收到取消请求后，若其 Future 已结束（例如任务尚未开始就被
     * {@code Future.cancel}，执行线程不会再产生任何结果），直接在调度线程内收尾为
     * CANCELLED，避免作业永远卡在 RUNNING。正常完成的 Future 其结果仍在 outcomes 队列里，
     * 取消请求优先的语义与此处一致，二者只会有一个先生效（锁内串行）。
     */
    private void sweepCancelledFutures() {
        for (Job job : jobs.values()) {
            if (job.getState() != JobState.RUNNING || !job.isCancelRequested()) {
                continue;
            }
            RunningSlot slot = running.get(job.getId());
            if (slot != null && slot.future().isDone()) {
                running.remove(job.getId());
                completeAttempt(job, AttemptResult.CANCELLED, "取消请求生效，执行未产生结果");
                transition(job, JobState.CANCELLED, null,
                        "取消请求生效（执行线程未产生结果，调度线程兜底收尾）");
                propagateUnreachable();
            }
        }
    }

    /** 返回依赖等待原因；上游全部成功时返回 null（失败的情况由传播逻辑负责）。 */
    private String waitingOnDeps(Job job) {
        for (String depId : job.getDependencies()) {
            Job dep = jobs.get(depId);
            if (dep == null) {
                return "等待上游 " + depId + "（缺失）";
            }
            if (dep.getState() != JobState.SUCCESS) {
                return "等待上游 " + depId + " 成功（当前 " + dep.getState() + "）";
            }
        }
        return null;
    }

    private void markQueued(Job job, String reason) {
        if (job.getState() == JobState.PENDING) {
            job.setState(JobState.QUEUED);
            job.setDetail(reason);
            store.put(job);
            log.info("job={} 状态迁移 PENDING->QUEUED 依据: {}", job.getId(), reason);
        } else {
            job.setDetail(reason);
        }
    }

    private void launch(Job job) {
        int attempt = job.getAttempts() + 1;
        job.setAttempts(attempt);
        job.setState(JobState.RUNNING);
        job.setStartedAt(System.currentTimeMillis());
        job.setCancelRequested(false);
        job.setDetail("依赖全部成功且获得配额，第 " + attempt + " 次执行");
        store.put(job);
        // 每次真实进入 RUNNING 都追加一条执行记录（先落盘后执行，崩溃也能查到“开始过”）
        store.saveAttempt(new AttemptRecord(job.getId(), attempt, job.getStartedAt()));
        log.info("job={} 状态迁移 ->RUNNING 第 {} 次执行 queue={} tenant={}",
                job.getId(), attempt, job.getQueue(), job.getTenant());
        Job live = job;
        final int launchedAttempt = attempt;
        Future<?> future = workers.submit(() -> runAttempt(live, launchedAttempt));
        running.put(job.getId(), new RunningSlot(launchedAttempt, future));
        Long timeout = timeoutForAttempt(job, attempt);
        if (timeout != null && timeout > 0) {
            // 仅对 RUNNING 期间计时：排队与重试退避不消耗该预算
            timeoutScheduler.schedule(() -> onTimeout(job.getId(), launchedAttempt),
                    timeout, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 某次尝试生效的超时预算（毫秒）。默认每次尝试都使用作业配置的
     * {@link Job#getTimeoutMillis()}（null/&lt;=0 表示不限制，行为与旧版一致）；
     * 声明为 protected 以便测试覆盖出“只有第 1 次短超时、后续尝试窗口更宽”的场景。
     */
    protected Long timeoutForAttempt(Job job, int attempt) {
        return job.getTimeoutMillis();
    }

    /**
     * 超时判定（在调度锁内直接生效）：仅当作业仍处于本次尝试的 RUNNING 才按失败处理。
     *
     * <p>竞争规则（确定性）：取消请求优先于超时；若执行结果已回送并处理（状态已迁移），
     * 超时判定为空操作；若结果尚未回送，超时先生效，迟到的结果会因状态非 RUNNING 被丢弃，
     * 同一作业不会出现“一会儿成功一会儿失败”。</p>
     */
    private void onTimeout(String jobId, int attempt) {
        if (!active) {
            return;
        }
        synchronized (lock) {
            if (!active) {
                return;
            }
            Job job = jobs.get(jobId);
            if (job == null || job.getState() != JobState.RUNNING || job.getAttempts() != attempt) {
                return;
            }
            RunningSlot slot = running.remove(jobId);
            // 只摘除确实属于“这次超时尝试”的槽位（理论上一定匹配，这里再做一次防御）
            if (slot != null && slot.attempt() != attempt) {
                running.put(jobId, slot);
                return;
            }
            if (slot != null) {
                slot.future().cancel(true);
            }
            String error = "TimeoutException: 第 " + attempt + " 次执行超过最长执行时间 "
                    + job.getTimeoutMillis() + " ms";
            if (job.isCancelRequested()) {
                // 取消与超时撞车：取消请求优先，语义与“取消 vs 成功”一致
                completeAttempt(job, AttemptResult.CANCELLED, error);
                transition(job, JobState.CANCELLED, null, "执行中超时，但取消请求优先");
                propagateUnreachable();
            } else {
                log.warn("job={} 第 {} 次执行超时（{} ms），按本次失败处理并立即释放并发名额",
                        jobId, attempt, job.getTimeoutMillis());
                applyFailure(job, AttemptResult.TIMEOUT, error);
            }
            lock.notifyAll();
        }
    }

    private void runAttempt(Job live, int attempt) {
        String id = live.getId();
        boolean ok = false;
        String error = null;
        try {
            taskHandler.execute(live);
            ok = true;
        } catch (Throwable t) {
            error = t.getClass().getSimpleName() + ": " + t.getMessage();
        }
        // 停机后任务才结束（例如任务对中断不敏感）：结果直接丢弃，绝不回送、绝不改状态
        if (!active) {
            log.info("job={} 第 {} 次尝试在调度器停机后才结束，结果不回送、不迁移状态", id, attempt);
            return;
        }
        // 结果必须带上它所属的尝试序号：迟到结果只允许对自己那一次生效
        outcomes.add(new Outcome(id, attempt, ok, error, Thread.currentThread().isInterrupted()));
    }

    private void handleOutcome(Outcome o) {
        Job job = jobs.get(o.jobId());
        if (job == null) {
            running.remove(o.jobId());
            return;
        }
        // 尝试归属判定：结果只对“它自己那一次尝试”生效。
        // 典型场景：第 n 次被判超时后任务并未真正停下，稍后才回送结果；此时第 n+1 次
        // 可能正在运行。迟到结果既不能顶替第 n+1 次的结果，也不能摘掉它占着的运行名额。
        RunningSlot slot = running.get(o.jobId());
        boolean sameAttempt = job.getState() == JobState.RUNNING
                && slot != null && slot.attempt() == o.attempt()
                && job.getAttempts() == o.attempt();
        if (!sameAttempt) {
            // 只有账本里挂着的槽位正是产生该迟到结果的旧执行时才摘除；槽位已被超时
            // 判定摘走（常见）或已属于更新的尝试时，这里什么都不动——名额绝不重复归还。
            if (slot != null && slot.attempt() == o.attempt()) {
                running.remove(o.jobId(), slot);
            }
            log.warn("job={} 第 {} 次尝试的迟到结果被丢弃（当前 state={}, attempts={}, 占用槽位={}），"
                            + "不迁移状态、不释放名额、不影响其它尝试",
                    o.jobId(), o.attempt(), job.getState(), job.getAttempts(),
                    slot == null ? "无（名额已在超时判定时归还）" : ("第 " + slot.attempt() + " 次"));
            return;
        }
        running.remove(o.jobId());
        if (o.success()) {
            if (job.isCancelRequested()) {
                // 取消与成功竞争：取消请求优先，保证取消语义确定
                completeAttempt(job, AttemptResult.CANCELLED, "执行完成与取消竞争，取消请求优先");
                transition(job, JobState.CANCELLED, null,
                        "执行完成与取消竞争，取消请求优先");
                propagateUnreachable();
                return;
            }
            completeAttempt(job, AttemptResult.SUCCESS, null);
            transition(job, JobState.SUCCESS, null,
                    "第 " + job.getAttempts() + " 次尝试成功，下游解除阻塞");
            return;
        }
        // 失败：协作取消（含中断）优先判定为 CANCELLED
        if (job.isCancelRequested() || o.interrupted()) {
            completeAttempt(job, AttemptResult.CANCELLED, o.error());
            transition(job, JobState.CANCELLED, o.error(), "执行中被取消（中断信号）");
            propagateUnreachable();
            return;
        }
        applyFailure(job, AttemptResult.FAILED, o.error());
    }

    /**
     * 失败类尝试的统一收尾（普通失败与超时共用）：仍有重试额度则回置 PENDING 并安排退避，
     * 否则进入 FAILED 并向下游传播。调用前必须持有锁且作业处于 RUNNING。
     */
    private void applyFailure(Job job, AttemptResult result, String error) {
        int max = job.getRetryPolicy().getMaxAttempts();
        if (job.getAttempts() < max) {
            long backoff = job.getRetryPolicy().getBackoffMillis();
            completeAttempt(job, result, error);
            job.setState(JobState.PENDING);
            job.setFailureReason(error);
            job.setDetail("第 " + job.getAttempts() + " 次尝试失败（" + result + "），"
                    + backoff + "ms 后重试（上限 " + max + " 次）");
            retryAfter.put(job.getId(), System.currentTimeMillis() + Math.max(0, backoff));
            store.put(job);
            log.warn("job={} 状态迁移 RUNNING->PENDING 第 {}/{} 次尝试{}，安排重试: {}",
                    job.getId(), job.getAttempts(), max, result, error);
        } else {
            completeAttempt(job, result, error);
            transition(job, JobState.FAILED, error,
                    "重试耗尽（" + job.getAttempts() + "/" + max + "），作业终态 FAILED，下游将被标记不可达");
            propagateUnreachable();
        }
    }

    /** 回填当前尝试的执行记录（结束时间 + 结果 + 原因），与状态迁移同事务落盘。 */
    private void completeAttempt(Job job, AttemptResult result, String error) {
        AttemptRecord rec = new AttemptRecord(job.getId(), job.getAttempts(), job.getStartedAt());
        rec.setFinishedAt(System.currentTimeMillis());
        rec.setResult(result);
        rec.setError(error);
        store.saveAttempt(rec);
    }

    /** 重启恢复时，把崩溃前未留下结果的执行记录收尾为 INTERRUPTED（已有终态的记录原样保留）。 */
    private void closeInterruptedAttempt(Job job) {
        List<AttemptRecord> records = store.listAttempts(job.getId());
        for (AttemptRecord rec : records) {
            // 只收尾确实“开始过但没有任何结果”的记录；TIMEOUT/SUCCESS/FAILED/CANCELLED 等
            // 已落终态的记录必须原样保留（例如超时后在重试退避期被杀的场景）。
            if (rec.getResult() == AttemptResult.STARTED) {
                rec.setResult(AttemptResult.INTERRUPTED);
                rec.setFinishedAt(System.currentTimeMillis());
                rec.setError("进程重启，执行被中断");
                store.saveAttempt(rec);
                log.info("job={} 第 {} 次尝试记录收尾为 INTERRUPTED（进程重启）", job.getId(), rec.getAttempt());
            }
        }
    }

    /** 沿下游传播：上游 FAILED/CANCELLED/UNREACHABLE 的 PENDING/QUEUED 作业标记为 UNREACHABLE。 */
    private void propagateUnreachable() {
        Deque<String> queue = new ArrayDeque<>();
        Map<String, String> blockerOf = new HashMap<>();
        for (Job job : jobs.values()) {
            JobState st = job.getState();
            if (st == JobState.FAILED || st == JobState.CANCELLED) {
                for (String dn : downstream.getOrDefault(job.getId(), List.of())) {
                    blockerOf.putIfAbsent(dn, job.getId());
                    queue.add(dn);
                }
            }
        }
        Set<String> processed = new HashSet<>();
        while (!queue.isEmpty()) {
            String id = queue.poll();
            if (!processed.add(id)) {
                continue;
            }
            Job j = jobs.get(id);
            if (j == null) {
                continue;
            }
            if (j.getState() == JobState.PENDING || j.getState() == JobState.QUEUED) {
                String blocker = blockerOf.get(id);
                JobState before = j.getState();
                j.setBlockedBy(blocker);
                transition(j, JobState.UNREACHABLE, null,
                        "上游 " + blocker + " 未成功（FAILED/CANCELLED/UNREACHABLE），依赖链不可达");
                log.warn("下游传播: job={} {}->UNREACHABLE blockedBy={}", id, before, blocker);
                for (String dn : downstream.getOrDefault(id, List.of())) {
                    blockerOf.putIfAbsent(dn, id);
                    queue.add(dn);
                }
            }
        }
    }

    /* ============================= 辅助 ============================= */

    private void transition(Job job, JobState target, String failureReason, String note) {
        JobState from = job.getState();
        job.setState(target);
        if (failureReason != null) {
            job.setFailureReason(failureReason);
        }
        if (target == JobState.SUCCESS || target == JobState.FAILED
                || target == JobState.CANCELLED || target == JobState.UNREACHABLE) {
            job.setFinishedAt(System.currentTimeMillis());
        }
        job.setDetail(note);
        store.put(job);
        log.info("job={} 状态迁移 {}->{} 依据: {}", job.getId(), from, target, note);
    }

    /** 测试辅助：读取当前快照。 */
    public Map<String, JobView> snapshot() {
        synchronized (lock) {
            Map<String, JobView> m = new HashMap<>();
            jobs.forEach((k, v) -> m.put(k, JobView.of(v)));
            return m;
        }
    }

    private record Outcome(String jobId, int attempt, boolean success, String error, boolean interrupted) {
    }
}
