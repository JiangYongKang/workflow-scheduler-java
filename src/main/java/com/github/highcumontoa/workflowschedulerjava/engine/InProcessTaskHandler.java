package com.github.highcumontoa.workflowschedulerjava.engine;

import com.github.highcumontoa.workflowschedulerjava.model.Job;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 内置内存执行器，用于本地验证：
 * <ul>
 *   <li>{@code noop}：立即成功；</li>
 *   <li>{@code sleep}：睡眠 {@code taskParam} 毫秒，期间响应中断（用于取消 RUNNING 作业）；</li>
 *   <li>{@code flaky}：前 {@code taskParam} 次尝试抛异常，之后成功（用于验证重试与传播）。</li>
 *   <li>{@code slowOnce}：仅第 1 次尝试睡眠 {@code taskParam} 毫秒，之后立即成功（用于验证超时重试）。</li>
 * </ul>
 */
@Component
public class InProcessTaskHandler implements TaskHandler {

    private static final Logger log = LoggerFactory.getLogger(InProcessTaskHandler.class);

    @Override
    public void execute(Job job) throws Exception {
        String type = job.getTaskType() == null ? "noop" : job.getTaskType().toLowerCase();
        int attempt = job.getAttempts();
        switch (type) {
            case "noop" -> {
                log.info("job={} attempt={} noop 执行成功", job.getId(), attempt);
            }
            case "sleep" -> {
                long ms = job.getTaskParam();
                log.info("job={} attempt={} sleep {}ms 开始", job.getId(), attempt, ms);
                Thread.sleep(ms);
                log.info("job={} attempt={} sleep 执行成功", job.getId(), attempt);
            }
            case "flaky" -> {
                long failTimes = job.getTaskParam();
                if (attempt <= failTimes) {
                    String msg = "job=" + job.getId() + " attempt=" + attempt
                            + " 模拟失败 (计划前 " + failTimes + " 次失败)";
                    log.warn(msg);
                    throw new IllegalStateException("模拟失败: 第 " + attempt + " 次尝试");
                }
                log.info("job={} attempt={} flaky 重试后成功", job.getId(), attempt);
            }
            case "slowonce" -> {
                // 仅第 1 次尝试睡眠 taskParam 毫秒（响应中断），后续尝试立即成功：用于验证超时后重试成功
                if (attempt == 1) {
                    log.info("job={} attempt=1 slowOnce 睡眠 {}ms", job.getId(), job.getTaskParam());
                    Thread.sleep(job.getTaskParam());
                }
                log.info("job={} attempt={} slowOnce 执行成功", job.getId(), attempt);
            }
            default -> throw new IllegalArgumentException("未知任务类型: " + type);
        }
    }
}
