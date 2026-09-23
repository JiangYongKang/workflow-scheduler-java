package com.github.highcumontoa.workflowschedulerjava.scheduler;

import com.github.highcumontoa.workflowschedulerjava.model.Job;
import com.github.highcumontoa.workflowschedulerjava.model.JobStatus;
import com.github.highcumontoa.workflowschedulerjava.repo.JobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Set;

/**
 * 依赖传播：上游进入 FAILED / CANCELLED 后，
 * 递归把仍处于 PENDING / QUEUED 的下游标记为 UNREACHABLE，并携带可解释原因。
 * 已在 RUNNING 的作业不受影响（它拿到的是此前全部 SUCCESS 的快照，继续执行）。
 */
@Component
public class DependencyPropagator {

    private static final Logger log = LoggerFactory.getLogger(DependencyPropagator.class);

    /** 可被级联标记为不可达的非终态状态。 */
    private static final Set<JobStatus> REACHABLE = Set.of(JobStatus.PENDING, JobStatus.QUEUED);

    private final JobRepository jobRepository;

    public DependencyPropagator(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    /** 从 upstreamId 出发级联标记不可达。 */
    public void propagateUnreachable(String upstreamId, String reason) {
        ArrayDeque<String> frontier = new ArrayDeque<>();
        frontier.add(upstreamId);
        while (!frontier.isEmpty()) {
            String up = frontier.poll();
            List<String> dependentIds = jobRepository.findDependents(up);
            if (dependentIds.isEmpty()) {
                continue;
            }
            List<Job> dependents = jobRepository.findByIds(dependentIds);
            for (Job dep : dependents) {
                if (!REACHABLE.contains(dep.getStatus())) {
                    continue; // RUNNING / 终态：不处理
                }
                // 判定依据：该下游至少有一个上游已不可达（即当前 up）
                String why = "UNREACHABLE: 上游 " + up + " 不可达（" + reason + "）";
                boolean moved = jobRepository.casStatus(
                        dep.getId(), REACHABLE, JobStatus.UNREACHABLE, null, why);
                if (moved) {
                    log.info("job={} 状态迁移 -> UNREACHABLE，判定依据: {}", dep.getId(), why);
                    frontier.add(dep.getId()); // 继续向下游传播
                }
            }
        }
    }
}
