package com.github.highcumontoa.workflowschedulerjava.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 抖动任务：当前尝试序号 ≤ failFirst 时失败、之后成功（payload 形如 "failFirst=2"，缺省 1）。
 * 判定依据是调度器传入的 attemptNo（数据库权威计数），不依赖进程内状态，
 * 因而测试间互不干扰、进程重启后语义也一致。
 */
@Component
public class FlakyHandler implements TaskHandler {

    private static final Logger log = LoggerFactory.getLogger(FlakyHandler.class);

    @Override
    public String type() {
        return "FLAKY";
    }

    @Override
    public HandlerResult handle(TaskContext context) {
        int failFirst = parseFailFirst(context.payload());
        int attemptNo = context.attemptNo();
        if (attemptNo <= failFirst) {
            log.info("job={} 执行 FLAKY 任务 第{}次尝试 ≤ failFirst={} -> FAIL",
                    context.jobId(), attemptNo, failFirst);
            return HandlerResult.fail("FLAKY: 第 " + attemptNo + " 次尝试失败（前 "
                    + failFirst + " 次预期失败）");
        }
        log.info("job={} 执行 FLAKY 任务 第{}次尝试 > failFirst={} -> SUCCESS",
                context.jobId(), attemptNo, failFirst);
        return HandlerResult.ok();
    }

    private int parseFailFirst(String payload) {
        if (payload == null || payload.isBlank()) {
            return 1;
        }
        String p = payload.trim();
        try {
            if (p.startsWith("failFirst=")) {
                return Integer.parseInt(p.substring("failFirst=".length()).trim());
            }
            return Integer.parseInt(p);
        } catch (NumberFormatException e) {
            return 1;
        }
    }
}
