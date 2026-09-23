package com.github.highcumontoa.workflowschedulerjava.web;

import com.github.highcumontoa.workflowschedulerjava.dto.BatchSubmitResponse;
import com.github.highcumontoa.workflowschedulerjava.dto.JobResponse;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitBatchRequest;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobRequest;
import com.github.highcumontoa.workflowschedulerjava.service.JobService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/** 作业提交 / 查询 / 取消 HTTP 接口。 */
@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    @PostMapping
    public ResponseEntity<JobResponse> submit(@RequestBody SubmitJobRequest request) {
        return ResponseEntity.ok(jobService.submit(request));
    }

    @PostMapping("/batch")
    public BatchSubmitResponse submitBatch(@RequestBody SubmitBatchRequest request) {
        return jobService.submitBatch(request);
    }

    @GetMapping("/{id}")
    public JobResponse get(@PathVariable String id) {
        return jobService.get(id);
    }

    @GetMapping
    public JobResponse getByKey(@RequestParam String idempotencyKey) {
        return jobService.getByIdempotencyKey(idempotencyKey);
    }

    @DeleteMapping("/{id}")
    public JobResponse cancel(@PathVariable String id) {
        return jobService.cancel(id);
    }
}
