package com.github.highcumontoa.workflowschedulerjava.dag;

import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobRequest;
import com.github.highcumontoa.workflowschedulerjava.error.ErrorCode;
import com.github.highcumontoa.workflowschedulerjava.error.SchedulerException;
import com.github.highcumontoa.workflowschedulerjava.repo.JobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DAG 校验：引用不存在的上游 → UNKNOWN_DEPENDENCY；存在环（含自环）→ CYCLE_DETECTED。
 * 校验在作业被接受（落库）之前完成，拒绝时不产生任何作业。
 */
@Component
public class DagValidator {

    private static final Logger log = LoggerFactory.getLogger(DagValidator.class);

    private final JobRepository jobRepository;

    public DagValidator(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    /** 校验单作业：其 dependencies 必须引用已存在作业；自引用直接判定为环。 */
    public void validateSingle(SubmitJobRequest request) {
        if (request.dependencies() == null) {
            return;
        }
        for (String dep : request.dependencies()) {
            if (dep != null && dep.equals(request.id())) {
                log.warn("提交被拒绝 CYCLE_DETECTED: 作业 {} 依赖自身", request.id());
                throw new SchedulerException(ErrorCode.CYCLE_DETECTED,
                        "作业 " + dep + " 依赖自身，构成环");
            }
            if (jobRepository.findById(dep) == null) {
                log.warn("提交被拒绝 UNKNOWN_DEPENDENCY: 作业 {} 引用不存在的上游 {}",
                        request.id(), dep);
                throw new SchedulerException(ErrorCode.UNKNOWN_DEPENDENCY,
                        "上游作业不存在: " + dep);
            }
        }
    }

    /**
     * 校验一批作业：
     * 1) 批内 id 不得重复；
     * 2) 每条依赖必须指向批内新作业或已存在作业，自引用为环；
     * 3) 新作业之间的依赖图必须无环（DFS 三色标记）。
     */
    public void validateBatch(List<SubmitJobRequest> requests, Set<String> newIds) {
        // 1) 批内重复 id
        Set<String> seen = new HashSet<>();
        for (SubmitJobRequest r : requests) {
            if (r.id() != null && !seen.add(r.id())) {
                log.warn("批量提交被拒绝 INVALID_REQUEST: 批内作业 id 重复 {}", r.id());
                throw new SchedulerException(ErrorCode.INVALID_REQUEST,
                        "批内作业 id 重复: " + r.id());
            }
        }

        // 2) 依赖存在性（同时收集“新作业之间”的邻接表）
        Map<String, Set<String>> adjacency = new HashMap<>();
        for (String id : newIds) {
            adjacency.put(id, new HashSet<>());
        }
        for (SubmitJobRequest r : requests) {
            if (r.dependencies() == null) {
                continue;
            }
            for (String dep : r.dependencies()) {
                if (dep != null && dep.equals(r.id())) {
                    log.warn("批量提交被拒绝 CYCLE_DETECTED: 作业 {} 依赖自身", r.id());
                    throw new SchedulerException(ErrorCode.CYCLE_DETECTED,
                            "作业 " + dep + " 依赖自身，构成环");
                }
                if (!newIds.contains(dep) && jobRepository.findById(dep) == null) {
                    log.warn("批量提交被拒绝 UNKNOWN_DEPENDENCY: 作业 {} 引用不存在的上游 {}",
                            r.id(), dep);
                    throw new SchedulerException(ErrorCode.UNKNOWN_DEPENDENCY,
                            "上游作业不存在: " + dep);
                }
                if (newIds.contains(dep)) {
                    adjacency.get(r.id()).add(dep);
                }
            }
        }

        // 3) 环检测：0=未访问 1=访问中（再次遇到即有环） 2=已完成
        Map<String, Integer> color = new HashMap<>();
        for (String id : newIds) {
            color.put(id, 0);
        }
        for (String id : newIds) {
            if (color.get(id) == 0 && hasCycle(id, adjacency, color)) {
                log.warn("批量提交被拒绝 CYCLE_DETECTED: 新作业子图存在经过 {} 的环", id);
                throw new SchedulerException(ErrorCode.CYCLE_DETECTED,
                        "依赖图存在环（检测起点: " + id + "）");
            }
        }
    }

    private boolean hasCycle(String node, Map<String, Set<String>> adjacency, Map<String, Integer> color) {
        color.put(node, 1);
        for (String next : adjacency.getOrDefault(node, Set.of())) {
            int c = color.getOrDefault(next, 2);
            if (c == 1) {
                return true;
            }
            if (c == 0 && hasCycle(next, adjacency, color)) {
                return true;
            }
        }
        color.put(node, 2);
        return false;
    }
}
