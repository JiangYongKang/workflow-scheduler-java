package com.github.highcumontoa.workflowschedulerjava.service;

import com.github.highcumontoa.workflowschedulerjava.dto.BatchSubmitResponse;
import com.github.highcumontoa.workflowschedulerjava.dto.JobResponse;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitBatchRequest;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobRequest;

/** 作业应用服务：提交、查询、取消。 */
public interface JobService {

    JobResponse submit(SubmitJobRequest request);

    BatchSubmitResponse submitBatch(SubmitBatchRequest request);

    JobResponse get(String id);

    JobResponse getByIdempotencyKey(String key);

    /** 取消非终态作业；成功返回新的响应；已终态抛 ALREADY_TERMINAL。 */
    JobResponse cancel(String id);
}
