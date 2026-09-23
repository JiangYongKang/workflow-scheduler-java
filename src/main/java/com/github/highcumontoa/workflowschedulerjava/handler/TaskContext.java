package com.github.highcumontoa.workflowschedulerjava.handler;

import java.util.function.BooleanSupplier;

/**
 * 执行上下文：作业标识、当前是第几次尝试、取消检查。
 * attemptNo 由调度器从数据库权威计数传入，处理器可据此实现确定性的“前 N 次失败”。
 */
public record TaskContext(String jobId, int attemptNo, String payload,
                          BooleanSupplier cancelled) {
}
