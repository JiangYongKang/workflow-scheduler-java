package com.github.highcumontoa.workflowschedulerjava.dto;

import java.util.List;

/** 批量提交结果。 */
public record BatchSubmitResponse(List<JobResponse> jobs) {
}
