package com.github.highcumontoa.workflowschedulerjava.quota;

import com.github.highcumontoa.workflowschedulerjava.config.QuotaLimitProperties;
import com.github.highcumontoa.workflowschedulerjava.config.SchedulerProperties;
import com.github.highcumontoa.workflowschedulerjava.dto.QuotaSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * 并发配额：全局 / 队列 / 租户三维上限。运行数以 RUNNING 状态落库计数为准，
 * 因而崩溃恢复后计数依然准确，不依赖进程内内存。
 *
 * <p>竞态说明：tryAcquire 与后续 CAS 到 RUNNING 不在同一事务里，理论上可能多个 tick
 * 同时“看到”有空位；但实际准入由单线程调度 tick 串行完成，且每次计数都实时查库，
 * 因此不会超发（单准入线程 + 数据库计数 + CAS 三重保证）。
 */
@Component
public class QuotaManager {

    private static final Logger log = LoggerFactory.getLogger(QuotaManager.class);

    private final JdbcTemplate jdbcTemplate;
    private final SchedulerProperties properties;
    private final QuotaLimitProperties limits;

    public QuotaManager(JdbcTemplate jdbcTemplate, SchedulerProperties properties,
                        QuotaLimitProperties limits) {
        this.jdbcTemplate = jdbcTemplate;
        this.properties = properties;
        this.limits = limits;
    }

    /** 若作业在当前时刻可获得配额（三维都未超限）返回 true。 */
    public boolean tryAcquire(String queue, String tenant) {
        int global = countRunning("1=1");
        if (global >= properties.getGlobalLimit()) {
            log.debug("配额判定拒绝: 全局 {}/{} 已满 (queue={}, tenant={})",
                    global, properties.getGlobalLimit(), queue, tenant);
            return false;
        }
        int byQueue = countRunning("queue_name = ?", queue);
        int qLimit = queueLimit(queue);
        if (byQueue >= qLimit) {
            log.debug("配额判定拒绝: 队列 {} {}/{} 已满 (tenant={})", queue, byQueue, qLimit, tenant);
            return false;
        }
        int byTenant = countRunning("tenant = ?", tenant);
        int tLimit = tenantLimit(tenant);
        if (byTenant >= tLimit) {
            log.debug("配额判定拒绝: 租户 {} {}/{} 已满 (queue={})", tenant, byTenant, tLimit, queue);
            return false;
        }
        log.debug("配额判定通过: global={}/{} queue={} {}/{} tenant={} {}/{}",
                global, properties.getGlobalLimit(), queue, byQueue, qLimit,
                tenant, byTenant, tLimit);
        return true;
    }

    private int countRunning(String where, Object... args) {
        Integer n = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM jobs WHERE status = 'RUNNING' AND " + where,
                Integer.class, args);
        return n == null ? 0 : n;
    }

    public int queueLimit(String queue) {
        return limits.getQueue().getOrDefault(queue, properties.getDefaultQueueLimit());
    }

    public int tenantLimit(String tenant) {
        return limits.getTenant().getOrDefault(tenant, properties.getDefaultTenantLimit());
    }

    public QuotaSnapshot snapshot() {
        int global = countRunning("1=1");
        Map<String, Integer> qRunning = groupedRunning("queue_name");
        Map<String, Integer> tRunning = groupedRunning("tenant");
        Map<String, Integer> qLimits = new TreeMap<>();
        for (String q : qRunning.keySet()) {
            qLimits.put(q, queueLimit(q));
        }
        Map<String, Integer> tLimits = new TreeMap<>();
        for (String t : tRunning.keySet()) {
            tLimits.put(t, tenantLimit(t));
        }
        return new QuotaSnapshot(global, properties.getGlobalLimit(),
                new TreeMap<>(qRunning), qLimits,
                new TreeMap<>(tRunning), tLimits);
    }

    private Map<String, Integer> groupedRunning(String column) {
        Map<String, Integer> out = new LinkedHashMap<>();
        jdbcTemplate.query("SELECT " + column + " AS k, COUNT(*) AS c FROM jobs "
                        + "WHERE status = 'RUNNING' GROUP BY " + column,
                rs -> {
                    out.put(rs.getString("k"), rs.getInt("c"));
                });
        return out;
    }
}
