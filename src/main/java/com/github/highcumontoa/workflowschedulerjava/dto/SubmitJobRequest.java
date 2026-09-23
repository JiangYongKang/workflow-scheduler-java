package com.github.highcumontoa.workflowschedulerjava.dto;

import java.util.List;

/** 提交单个作业的请求体；id 可选（缺省由服务端生成）。 */
public record SubmitJobRequest(
        String id,
        String idempotencyKey,
        String jobType,
        String payload,
        List<String> dependencies,
        String queue,
        String tenant,
        Integer maxAttempts,
        Long backoffBaseMs,
        Double backoffMultiplier
) {
}
