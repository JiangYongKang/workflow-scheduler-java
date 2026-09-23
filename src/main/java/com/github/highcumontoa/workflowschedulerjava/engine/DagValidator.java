package com.github.highcumontoa.workflowschedulerjava.engine;

import com.github.highcumontoa.workflowschedulerjava.exception.ErrorCode;
import com.github.highcumontoa.workflowschedulerjava.exception.SchedulerException;
import com.github.highcumontoa.workflowschedulerjava.model.Job;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * DAG 准入校验：
 * <ol>
 *   <li>作业 id 非空且批次内不允许重复；</li>
 *   <li>每个上游引用必须能在“已存在作业 + 本批作业”中解析；</li>
 *   <li>依赖图不允许存在环（三色 DFS，环路径写入异常信息）。</li>
 * </ol>
 * 任一条件不满足都在作业被接受（持久化）之前拒绝。
 */
public class DagValidator {

    /**
     * @param newJobs     本批待接受作业
     * @param knownIds    已持久化的作业 id 集合
     * @param depResolver 作业 id -> 其上游 id 列表（覆盖已存在与本批作业）
     */
    public void validate(List<Job> newJobs, Set<String> knownIds,
                         Function<String, List<String>> depResolver) {
        // 1) id 非空 + 批次内重复 id
        Set<String> batchIds = new HashSet<>();
        for (Job job : newJobs) {
            if (job.getId() == null || job.getId().isBlank()) {
                throw new SchedulerException(ErrorCode.INVALID_REQUEST, "作业 id 不能为空");
            }
            if (!batchIds.add(job.getId())) {
                throw new SchedulerException(ErrorCode.DUPLICATE_JOB_ID,
                        "批次内存在重复的作业 id: " + job.getId());
            }
        }

        // 2) 上游必须存在
        Set<String> allIds = new HashSet<>(knownIds);
        allIds.addAll(batchIds);
        for (Job job : newJobs) {
            for (String dep : job.getDependencies()) {
                if (!allIds.contains(dep)) {
                    throw new SchedulerException(ErrorCode.UPSTREAM_NOT_FOUND,
                            "作业 " + job.getId() + " 引用了不存在的上游: " + dep);
                }
            }
        }

        // 3) 环检测：以本批作业为起点三色 DFS（已存在子图在准入时已保证无环）
        Map<String, Integer> color = new HashMap<>(); // 0=white 1=gray 2=black
        List<String> path = new ArrayList<>();
        for (String id : batchIds) {
            if (color.getOrDefault(id, 0) == 0) {
                dfs(id, depResolver, color, path);
            }
        }
    }

    private void dfs(String node, Function<String, List<String>> depResolver,
                     Map<String, Integer> color, List<String> path) {
        color.put(node, 1);
        path.add(node);
        List<String> deps = depResolver.apply(node);
        if (deps != null) {
            for (String dep : deps) {
                int c = color.getOrDefault(dep, 0);
                if (c == 1) {
                    // path 中从 dep 到当前节点构成环
                    int start = path.indexOf(dep);
                    List<String> cycle = new ArrayList<>(path.subList(start, path.size()));
                    cycle.add(dep);
                    throw new SchedulerException(ErrorCode.CYCLIC_DEPENDENCY,
                            "检测到环依赖: " + String.join(" -> ", cycle));
                }
                if (c == 0) {
                    dfs(dep, depResolver, color, path);
                }
            }
        }
        color.put(node, 2);
        path.remove(path.size() - 1);
    }
}
