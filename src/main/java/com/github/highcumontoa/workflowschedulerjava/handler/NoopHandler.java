package com.github.highcumontoa.workflowschedulerjava.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** 空操作任务：立即成功。用于基础链路、配额与重复提交验证。 */
@Component
public class NoopHandler implements TaskHandler {

    private static final Logger log = LoggerFactory.getLogger(NoopHandler.class);

    @Override
    public String type() {
        return "NOOP";
    }

    @Override
    public HandlerResult handle(TaskContext context) {
        log.info("job={} 执行 NOOP 任务 payload={}", context.jobId(), context.payload());
        return HandlerResult.ok();
    }
}
