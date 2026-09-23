package com.github.highcumontoa.workflowschedulerjava.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 睡眠任务：payload 为毫秒数（缺省 1000ms）。
 * 每 50ms 轮询一次取消标记，取消后返回失败结果（携带 CANCELLED 判定依据）。
 */
@Component
public class SleepHandler implements TaskHandler {

    private static final Logger log = LoggerFactory.getLogger(SleepHandler.class);

    @Override
    public String type() {
        return "SLEEP";
    }

    @Override
    public HandlerResult handle(TaskContext context) throws InterruptedException {
        long ms = parseDuration(context.payload());
        log.info("job={} 执行 SLEEP 任务 durationMs={}", context.jobId(), ms);
        long deadline = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < deadline) {
            if (context.cancelled().getAsBoolean()) {
                log.info("job={} SLEEP 任务检测到取消标记，中止执行", context.jobId());
                return HandlerResult.fail("CANCELLED: 任务在执行中被取消");
            }
            Thread.sleep(Math.min(50L, Math.max(0L, deadline - System.currentTimeMillis())));
        }
        return HandlerResult.ok();
    }

    private long parseDuration(String payload) {
        if (payload == null || payload.isBlank()) {
            return 1000L;
        }
        try {
            return Long.parseLong(payload.trim());
        } catch (NumberFormatException e) {
            return 1000L;
        }
    }
}
