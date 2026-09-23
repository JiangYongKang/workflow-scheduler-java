package com.github.highcumontoa.workflowschedulerjava.dto;

import java.util.List;

/** 批量提交一个 DAG（内部依赖可引用同批次其他作业的 id）。 */
public record SubmitBatchRequest(List<SubmitJobRequest> jobs) {
}
