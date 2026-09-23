package com.github.highcumontoa.workflowschedulerjava.repo;

import com.github.highcumontoa.workflowschedulerjava.model.Job;
import com.github.highcumontoa.workflowschedulerjava.model.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 作业持久化仓储（H2 + JdbcTemplate）。
 * 所有状态迁移均通过条件更新（CAS）完成，保证并发下不会重复调度/重复计数。
 */
@Repository
public class JobRepository {

    private static final Logger log = LoggerFactory.getLogger(JobRepository.class);

    private static final String COLUMNS =
            "id, idempotency_key, job_type, payload, queue_name, tenant, status, attempts, "
            + "max_attempts, backoff_base_ms, backoff_multiplier, run_after, failure_reason, "
            + "created_at, updated_at";

    private final JdbcTemplate jdbcTemplate;

    public JobRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private static final RowMapper<Job> MAPPER = new RowMapper<>() {
        @Override
        public Job mapRow(ResultSet rs, int rowNum) throws SQLException {
            Job job = new Job();
            job.setId(rs.getString("id"));
            job.setIdempotencyKey(rs.getString("idempotency_key"));
            job.setJobType(rs.getString("job_type"));
            job.setPayload(rs.getString("payload"));
            job.setQueueName(rs.getString("queue_name"));
            job.setTenant(rs.getString("tenant"));
            job.setStatus(JobStatus.valueOf(rs.getString("status")));
            job.setAttempts(rs.getInt("attempts"));
            job.setMaxAttempts(rs.getInt("max_attempts"));
            job.setBackoffBaseMs(rs.getLong("backoff_base_ms"));
            job.setBackoffMultiplier(rs.getDouble("backoff_multiplier"));
            job.setRunAfter(rs.getLong("run_after"));
            job.setFailureReason(rs.getString("failure_reason"));
            job.setCreatedAt(rs.getLong("created_at"));
            job.setUpdatedAt(rs.getLong("updated_at"));
            return job;
        }
    };

    /** 插入新作业及其依赖边；幂等键冲突时返回 null（调用方按重复提交处理）。 */
    @Transactional
    public Job insertIfAbsent(Job job) {
        try {
            jdbcTemplate.update(
                    "INSERT INTO jobs (id, idempotency_key, job_type, payload, queue_name, tenant, status, "
                            + "attempts, max_attempts, backoff_base_ms, backoff_multiplier, run_after, "
                            + "failure_reason, created_at, updated_at) "
                            + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    job.getId(), job.getIdempotencyKey(), job.getJobType(), job.getPayload(),
                    job.getQueueName(), job.getTenant(), job.getStatus().name(), job.getAttempts(),
                    job.getMaxAttempts(), job.getBackoffBaseMs(), job.getBackoffMultiplier(),
                    job.getRunAfter(), job.getFailureReason(), job.getCreatedAt(), job.getUpdatedAt());
        } catch (DuplicateKeyException dup) {
            // 唯一键冲突：要么是幂等键重复，要么是 id 重复（id 重复属请求错误，由调用方进一步分辨）
            return null;
        }
        for (String up : job.getDependencies()) {
            jdbcTemplate.update("INSERT INTO job_deps (job_id, upstream_id) VALUES (?,?)", job.getId(), up);
        }
        log.info("job={} 持久化完成 status={} deps={} idempotencyKey={}",
                job.getId(), job.getStatus(), job.getDependencies(), job.getIdempotencyKey());
        return job;
    }

    /** 按 id 查询，不存在返回 null（含依赖列表）。 */
    public Job findById(String id) {
        List<Job> list = jdbcTemplate.query(
                "SELECT " + COLUMNS + " FROM jobs WHERE id = ?", MAPPER, id);
        if (list.isEmpty()) {
            return null;
        }
        Job job = list.get(0);
        job.setDependencies(loadDeps(id));
        return job;
    }

    /** 按幂等键查询，不存在返回 null（含依赖列表）。 */
    public Job findByIdempotencyKey(String key) {
        List<Job> list = jdbcTemplate.query(
                "SELECT " + COLUMNS + " FROM jobs WHERE idempotency_key = ?", MAPPER, key);
        if (list.isEmpty()) {
            return null;
        }
        Job job = list.get(0);
        job.setDependencies(loadDeps(job.getId()));
        return job;
    }

    /** 读取指定状态集合中的全部作业（含依赖列表）。 */
    public List<Job> findByStatuses(JobStatus... statuses) {
        String in = Arrays.stream(statuses).map(s -> "?").collect(Collectors.joining(","));
        Object[] args = Arrays.stream(statuses).map(Enum::name).toArray();
        List<Job> jobs = jdbcTemplate.query(
                "SELECT " + COLUMNS + " FROM jobs WHERE status IN (" + in + ")", MAPPER, args);
        for (Job job : jobs) {
            job.setDependencies(loadDeps(job.getId()));
        }
        return jobs;
    }

    /** 按 id 集合批量装载（含依赖列表），空集合返回空表。 */
    public List<Job> findByIds(java.util.List<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return java.util.List.of();
        }
        String inIds = ids.stream().map(x -> "?").collect(Collectors.joining(","));
        List<Job> jobs = jdbcTemplate.query(
                "SELECT " + COLUMNS + " FROM jobs WHERE id IN (" + inIds + ")", MAPPER,
                ids.toArray());
        for (Job job : jobs) {
            job.setDependencies(loadDeps(job.getId()));
        }
        return jobs;
    }

    private List<String> loadDeps(String jobId) {
        return jdbcTemplate.queryForList(
                "SELECT upstream_id FROM job_deps WHERE job_id = ? ORDER BY upstream_id",
                String.class, jobId);
    }

    /**
     * 条件状态迁移：仅当当前状态 ∈ expectedStatuses 时更新成功。
     * 迁移到 RUNNING 时 attempts+1；可同时更新 runAfter / failureReason。
     *
     * @return 是否更新成功（失败说明并发竞争中已被别人迁移）
     */
    public boolean casStatus(String id, Set<JobStatus> expectedStatuses,
                             JobStatus newStatus, Long runAfter, String failureReason) {
        String in = expectedStatuses.stream().map(s -> "?").collect(Collectors.joining(","));
        long now = System.currentTimeMillis();
        int increment = newStatus == JobStatus.RUNNING ? 1 : 0;
        int n = expectedStatuses.size();
        Object[] args = new Object[n + 6];
        // SET: status(1), updated_at(2), runAfter(3), failureReason(4), increment(5)
        args[0] = newStatus.name();
        args[1] = now;
        args[2] = runAfter;
        args[3] = failureReason;
        args[4] = increment;
        // WHERE: id(6), 随后是状态 IN 列表
        args[5] = id;
        int i = 6;
        for (JobStatus s : expectedStatuses) {
            args[i++] = s.name();
        }
        int updated = jdbcTemplate.update(
                "UPDATE jobs SET status = ?, updated_at = ?, "
                        + "run_after = COALESCE(?, run_after), "
                        + "failure_reason = COALESCE(?, failure_reason), "
                        + "attempts = attempts + ? "
                        + "WHERE id = ? AND status IN (" + in + ")",
                args);
        if (updated > 0) {
            log.info("job={} CAS 状态迁移成功 -> {} (判定依据: 旧状态∈{})", id, newStatus, expectedStatuses);
        }
        return updated > 0;
    }

    /** 标记一次执行尝试开始，返回 attempt 行 id。 */
    public long recordAttemptStart(String jobId, int attemptNo) {
        long now = System.currentTimeMillis();
        var keyHolder = new org.springframework.jdbc.support.GeneratedKeyHolder();
        jdbcTemplate.update(con -> {
            java.sql.PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO job_attempts (job_id, attempt_no, started_at, result) "
                            + "VALUES (?,?,?,?)",
                    java.sql.Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, jobId);
            ps.setInt(2, attemptNo);
            ps.setLong(3, now);
            ps.setString(4, "RUNNING");
            return ps;
        }, keyHolder);
        Number key = keyHolder.getKey();
        return key == null ? -1L : key.longValue();
    }

    /** 记录尝试结束结果。 */
    public void recordAttemptFinish(long attemptId, String result, String message) {
        jdbcTemplate.update(
                "UPDATE job_attempts SET finished_at = ?, result = ?, message = ? WHERE id = ?",
                System.currentTimeMillis(), result, message, attemptId);
    }

    /** 查询作业的尝试次数（已开始执行的尝试行数）。 */
    public int countAttempts(String jobId) {
        Integer n = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM job_attempts WHERE job_id = ?", Integer.class, jobId);
        return n == null ? 0 : n;
    }

    /** 查询直接依赖某上游的作业 id。 */
    public List<String> findDependents(String upstreamId) {
        return jdbcTemplate.queryForList(
                "SELECT job_id FROM job_deps WHERE upstream_id = ?", String.class, upstreamId);
    }

    /** 重启恢复：把遗留的 RUNNING 全部回滚为 QUEUED（崩溃时正在执行的作业需要重跑，at-least-once）。 */
    @Transactional
    public int recoverRunningToQueued() {
        List<String> ids = jdbcTemplate.queryForList(
                "SELECT id FROM jobs WHERE status = 'RUNNING'", String.class);
        for (String id : ids) {
            jdbcTemplate.update(
                    "UPDATE jobs SET status = 'QUEUED', run_after = ?, updated_at = ? WHERE id = ? AND status = 'RUNNING'",
                    System.currentTimeMillis(), System.currentTimeMillis(), id);
            log.info("job={} 重启恢复: RUNNING -> QUEUED（崩溃前在执行，按 at-least-once 重新执行）", id);
        }
        return ids.size();
    }
}
