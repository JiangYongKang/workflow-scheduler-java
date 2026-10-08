package com.github.highcumontoa.workflowschedulerjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.workflowschedulerjava.config.SchedulerProperties;
import com.github.highcumontoa.workflowschedulerjava.dto.JobView;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitBatchRequest;
import com.github.highcumontoa.workflowschedulerjava.dto.SubmitJobRequest;
import com.github.highcumontoa.workflowschedulerjava.engine.InProcessTaskHandler;
import com.github.highcumontoa.workflowschedulerjava.engine.SchedulerEngine;
import com.github.highcumontoa.workflowschedulerjava.model.AttemptRecord;
import com.github.highcumontoa.workflowschedulerjava.model.RetryPolicy;
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
 *   <li>{@code run-timeout-retry}：提交一个超时 200ms、退避 5000ms、上限 3 的 slowOnce 作业，
 *       待其第 1 次尝试被判定 TIMEOUT 后打印 {@code READY} 并挂住（处于重试退避窗口，等被杀）；</li>
 *   <li>{@code verify}：从同一目录恢复，等待 slow 到达 SUCCESS（最多 15s），打印全部作业
 *       状态与执行记录后退出；</li>
 *   <li>{@code verify-resume}：从同一目录恢复，打印暂停标记与 held 状态，随后恢复调度并
 *       等待 held 到达 SUCCESS，打印后退出。</li>
 *   <li>{@code verify-timeout-retry}：从同一目录恢复，等待作业终态（slowOnce 第 2 次成功），
 *       打印状态与每条执行记录后退出，供父进程核对记录条数跨重启不虚增不丢失。</li>
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
            case "run-timeout-retry" -> runTimeoutRetry(dir);
            case "verify" -> verify(dir);
            case "verify-resume" -> verifyResume(dir);
            case "verify-timeout-retry" -> verifyTimeoutRetry(dir);
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

    /**
     * 超时重试场景的“被杀前”进程：tjob 为 slowOnce（第 1 次睡眠 5000ms 且响应中断），
     * 超时 200ms、退避 8000ms、最多 3 次。第 1 次尝试被判定 TIMEOUT 后作业进入长退避窗口
     * （PENDING），此时打印 READY 挂住，等待父进程杀掉。落盘的应恰为 1 条 TIMEOUT 记录。
     */
    private static void runTimeoutRetry(Path dir) throws Exception {
        SchedulerEngine e = newEngine(dir);
        SubmitJobRequest r = job("tjob", "slowonce", 5000);
        r.setTimeoutMillis(200L);
        r.setRetryPolicy(new RetryPolicy(3, 8000));
        submit(e, r);
        // 等到第 1 次尝试的 TIMEOUT 记录落盘（作业随后进入 8s 退避窗口，不会启动第 2 次）
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            List<AttemptRecord> records = e.listAttempts("tjob");
            if (records.size() == 1 && records.get(0).getResult()
                    == com.github.highcumontoa.workflowschedulerjava.model.AttemptResult.TIMEOUT) {
                break;
            }
            Thread.sleep(20);
        }
        List<AttemptRecord> records = e.listAttempts("tjob");
        if (records.size() != 1 || records.get(0).getResult()
                != com.github.highcumontoa.workflowschedulerjava.model.AttemptResult.TIMEOUT) {
            throw new IllegalStateException("杀进程前未观察到 1 条 TIMEOUT 记录: " + records);
        }
        System.out.println("JOB id=tjob state=" + e.get("tjob").getState()
                + " attempts=" + e.get("tjob").getAttempts());
        System.out.println("READY");
        Thread.sleep(Duration.ofHours(1).toMillis());
    }

    /** “被杀后”恢复进程：slowOnce 第 2 次尝试立即成功；打印终态与全部执行记录后退出。 */
    private static void verifyTimeoutRetry(Path dir) throws Exception {
        SchedulerEngine e = newEngine(dir);
        try {
            await(e, "tjob", "SUCCESS", Duration.ofSeconds(15));
            printAll(e, List.of("tjob"));
        } finally {
            e.stop();
        }
        System.out.println("DONE");
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
