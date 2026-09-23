package com.github.highcumontoa.workflowschedulerjava.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** 恒定失败任务：payload 即失败信息（缺省有默认原因），用于失败传播与重试验证。 */
@Component
public class FailHandler implements TaskHandler {

    private static final Logger log = LoggerFactory.getLogger(FailHandler.class);

    @Override
    public String type() {
        return "FAIL";
    }

    @Override
    public HandlerResult handle(TaskContext context) {
        String msg = (context.payload() == null || context.payload().isBlank())
                ? "任务恒定失败（FAIL handler）" : context.payload();
        log.info("job={} 执行 FAIL 任务 reason={}", context.jobId(), msg);
        return HandlerResult.fail(msg);
    }
}
