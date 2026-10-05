package com.github.highcumontoa.workflowschedulerjava.web;

import com.github.highcumontoa.workflowschedulerjava.dto.*;
import com.github.highcumontoa.workflowschedulerjava.engine.SchedulerEngine;
import com.github.highcumontoa.workflowschedulerjava.model.AttemptRecord;
import java.util.List;
import org.springframework.web.bind.annotation.*;

/** 作业 HTTP 接口（骨架占位）。 */
@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final SchedulerEngine engine;

    public JobController(SchedulerEngine engine) {
        this.engine = engine;
    }

    @PostMapping
    public List<SubmitJobResponse> submit(@RequestBody SubmitBatchRequest request) {
        return engine.submit(request);
    }

    @GetMapping("/{id}")
    public JobView get(@PathVariable String id) {
        return engine.get(id);
    }

    @GetMapping
    public List<JobView> list() {
        return engine.list();
    }

    @PostMapping("/{id}/cancel")
    public JobView cancel(@PathVariable String id) {
        return engine.cancel(id);
    }

    @GetMapping("/{id}/attempts")
    public List<AttemptRecord> attempts(@PathVariable String id) {
        return engine.attempts(id);
    }
}
