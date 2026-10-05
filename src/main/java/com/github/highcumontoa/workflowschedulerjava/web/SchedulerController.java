package com.github.highcumontoa.workflowschedulerjava.web;

import com.github.highcumontoa.workflowschedulerjava.engine.SchedulerEngine;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 调度器级开关：暂停 / 恢复 / 状态查询。 */
@RestController
@RequestMapping("/api/scheduler")
public class SchedulerController {

    private final SchedulerEngine engine;

    public SchedulerController(SchedulerEngine engine) {
        this.engine = engine;
    }

    @PostMapping("/pause")
    public Map<String, Object> pause() {
        engine.pause();
        return Map.of("paused", engine.isPaused());
    }

    @PostMapping("/resume")
    public Map<String, Object> resume() {
        engine.resume();
        return Map.of("paused", engine.isPaused());
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        return Map.of("paused", engine.isPaused());
    }
}
