package com.github.highcumontoa.workflowschedulerjava.error;

import java.time.Instant;

/** 统一错误响应体。 */
public record ApiError(String code, String message, String path, Instant time) {
}
