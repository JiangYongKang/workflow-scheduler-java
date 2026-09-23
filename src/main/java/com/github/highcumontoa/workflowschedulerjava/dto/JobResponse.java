package com.github.highcumontoa.workflowschedulerjava.dto;

import java.util.List;

/** 作业查询/提交通用响应。deduplicated=true 表示命中了幂等键，本次请求未产生新作业。 */
public record JobResponse(
        String id,
        String idempotencyKey,
        String status,
        String jobType,
        String queue,
        String tenant,
        int attempts,
        int maxAttempts,
        String failureReason,
        List<String> dependencies,
        long createdAt,
        long updatedAt,
        boolean deduplicated
) {
}
