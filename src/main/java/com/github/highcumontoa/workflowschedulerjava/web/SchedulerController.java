package com.github.highcumontoa.workflowschedulerjava.web;

import com.github.highcumontoa.workflowschedulerjava.engine.SchedulerEngine;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 调度器级开关：暂停 / 恢复 / 状态查询。
 *
 * <p>暂停后不启动新执行，运行中的作业自然跑完，排队作业保留；开关状态持久化，
 * 进程重启后保持。</p>
 */
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
        return status();
    }

    @PostMapping("/resume")
    public Map<String, Object> resume() {
        engine.resume();
        return status();
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        return Map.of("paused", engine.isPaused());
    }
}
