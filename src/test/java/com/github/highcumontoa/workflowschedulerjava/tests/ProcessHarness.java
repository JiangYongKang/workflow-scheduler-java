package com.github.highcumontoa.workflowschedulerjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.workflowschedulerjava.config.SchedulerProperties;
import com.github.highcumontoa.workflowschedulerjava.dto.JobView;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitBatchRequest;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobRequest;
import com.github.highcumontoa.workflowschedulerjava.engine.InProcessTaskHandler;
import com.github.highcumontoa.workflowschedulerjava.engine.SchedulerEngine;
import com.github.highcumontoa.workflowschedulerjava.model.AttemptRecord;
import com.github.highcumontoa.workflowschedulerjava.store.JsonFileJobStore;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

/**
 * 真实进程重启测试的载体进程（不是测试类，本身不含断言）。
 *
 * <p>用法：{@code java ... ProcessHarness <storeDir> <mode>}，通过标准输出与父进程通信：</p>
 * <ul>
 *   <li>{@code run}：提交 ok(noop) 与 slow(sleep 1500ms)，待 slow 进入 RUNNING 后打印
 *       {@code READY} 并挂住，等待父进程 kill；</li>
 *   <li>{@code run-paused}：先暂停调度器，提交 held(noop)，打印 {@code READY} 并挂住；</li>
 *   <li>{@code verify}：从同一目录恢复，等待 slow 到达 SUCCESS（最多 15s），打印全部作业
 *       状态与执行记录后退出；</li>
 *   <li>{@code verify-resume}：从同一目录恢复，打印暂停标记与 held 状态，随后恢复调度并
 *       等待 held 到达 SUCCESS，打印后退出。</li>
 * </ul>
 *
 * <p>输出协议（行）：{@code READY} / {@code PAUSED <bool>} /
 * {@code JOB id=<id> state=<state> attempts=<n>} /
 * {@code ATTEMPT jobId=<id> attempt=<n> result=<result>} / {@code DONE}。</p>
 */
public final class ProcessHarness {

    private ProcessHarness() {
    }

    public static void main(String[] args) throws Exception {
        Path dir = Path.of(args[0]);
        String mode = args[1];
        switch (mode) {
            case "run" -> run(dir, false);
            case "run-paused" -> run(dir, true);
            case "verify" -> verify(dir);
            case "verify-resume" -> verifyResume(dir);
            default -> throw new IllegalArgumentException("未知模式: " + mode);
        }
    }

    private static SchedulerEngine newEngine(Path dir) {
        SchedulerProperties p = new SchedulerProperties();
        p.setStoreDir(dir.toString());
        p.setTickMillis(20);
        p.setGlobalConcurrency(2);
        p.setDefaultQueueConcurrency(2);
        p.setDefaultTenantConcurrency(2);
        SchedulerEngine e = new SchedulerEngine(new JsonFileJobStore(dir, new ObjectMapper()),
                new InProcessTaskHandler(), p);
        e.start();
        return e;
    }

    private static SubmitJobRequest job(String id, String type, long param) {
        SubmitJobRequest r = new SubmitJobRequest();
        r.setId(id);
        r.setTaskType(type);
        r.setTaskParam(param);
        return r;
    }

    private static void submit(SchedulerEngine e, SubmitJobRequest... jobs) {
        SubmitBatchRequest batch = new SubmitBatchRequest();
        batch.setJobs(List.of(jobs));
        e.submit(batch);
    }

    private static void run(Path dir, boolean paused) throws Exception {
        SchedulerEngine e = newEngine(dir);
        if (paused) {
            e.pause();
            submit(e, job("held", "noop", 0));
            Thread.sleep(300);
            System.out.println("PAUSED " + e.isPaused());
            System.out.println("READY");
        } else {
            submit(e, job("ok", "noop", 0), job("slow", "sleep", 1500));
            await(e, "slow", "RUNNING", Duration.ofSeconds(10));
            await(e, "ok", "SUCCESS", Duration.ofSeconds(10));
            System.out.println("READY");
        }
        // 挂住，等待父进程 destroyForcibly（真实进程被杀）
        Thread.sleep(Duration.ofHours(1).toMillis());
    }

    private static void verify(Path dir) throws Exception {
        SchedulerEngine e = newEngine(dir);
        try {
            await(e, "slow", "SUCCESS", Duration.ofSeconds(15));
            printAll(e, List.of("ok", "slow"));
        } finally {
            e.stop();
        }
        System.out.println("DONE");
    }

    private static void verifyResume(Path dir) throws Exception {
        SchedulerEngine e = newEngine(dir);
        try {
            System.out.println("PAUSED " + e.isPaused());
            Thread.sleep(400);
            printAll(e, List.of("held"));
            e.resume();
            await(e, "held", "SUCCESS", Duration.ofSeconds(10));
            printAll(e, List.of("held"));
        } finally {
            e.stop();
        }
        System.out.println("DONE");
    }

    private static void printAll(SchedulerEngine e, List<String> ids) {
        for (String id : ids) {
            JobView v = e.get(id);
            System.out.println("JOB id=" + v.getId() + " state=" + v.getState()
                    + " attempts=" + v.getAttempts());
            for (AttemptRecord r : e.listAttempts(id)) {
                System.out.println("ATTEMPT jobId=" + r.getJobId() + " attempt=" + r.getAttempt()
                        + " result=" + r.getResult());
            }
        }
    }

    private static void await(SchedulerEngine e, String id, String state, Duration timeout)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (e.get(id).getState().equals(state)) {
                return;
            }
            Thread.sleep(20);
        }
        throw new IllegalStateException("等待超时: " + id + " 未到 " + state + "，实际=" + e.get(id).getState());
    }
}
