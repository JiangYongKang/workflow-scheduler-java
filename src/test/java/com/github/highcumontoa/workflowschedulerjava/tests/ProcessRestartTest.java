package com.github.highcumontoa.workflowschedulerjava.tests;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 真实进程重启验证：用子进程拉起调度器，destroyForcibly 杀掉后再起新进程检查状态，
 * 而不是在同一进程里重建引擎“假装重启”。
 */
class ProcessRestartTest {

    private static final Logger log = LoggerFactory.getLogger(ProcessRestartTest.class);

    /** 启动一个 harness 子进程，后台持续收集其标准输出行。 */
    private static Proc spawn(Path dir, String mode) throws Exception {
        String javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("java.class.path");
        ProcessBuilder pb = new ProcessBuilder(javaBin, "-cp", classpath,
                ProcessHarness.class.getName(), dir.toString(), mode);
        pb.redirectErrorStream(true);
        Process process = pb.start();
        ConcurrentLinkedQueue<String> lines = new ConcurrentLinkedQueue<>();
        Thread reader = new Thread(() -> {
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    lines.add(line);
                    log.info("[子进程:{}] {}", mode, line);
                }
            } catch (Exception ignored) {
                // 进程被杀时读流关闭，属正常
            }
        });
        reader.setDaemon(true);
        reader.start();
        return new Proc(process, lines);
    }

    private record Proc(Process process, ConcurrentLinkedQueue<String> lines) {

        String awaitLine(String prefix, Duration timeout) throws Exception {
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            List<String> seen = new ArrayList<>();
            while (System.currentTimeMillis() < deadline) {
                for (String l : lines) {
                    if (!seen.contains(l) && l.startsWith(prefix)) {
                        return l;
                    }
                    seen.add(l);
                }
                Thread.sleep(20);
            }
            throw new AssertionError("子进程未在 " + timeout + " 内输出 " + prefix + "*，已收到: " + lines);
        }

        void killForcibly() throws Exception {
            process.destroyForcibly();
            boolean exited = process.waitFor(15, TimeUnit.SECONDS);
            assertTrue(exited, "子进程应能被强制结束");
            log.info("[真实重启] 子进程 pid={} 已被 destroyForcibly 杀掉", process.pid());
        }

        List<String> awaitDoneAndCollect(Duration timeout) throws Exception {
            boolean exited = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
            assertTrue(exited, "verify 子进程应自行退出，输出: " + lines);
            assertEquals(0, process.exitValue(), "verify 子进程应正常退出，输出: " + lines);
            return List.copyOf(lines);
        }
    }

    private static Map<String, String> jobStates(List<String> lines) {
        Map<String, String> m = new java.util.HashMap<>();
        for (String l : lines) {
            if (l.startsWith("JOB ")) {
                String id = l.replaceAll(".*id=(\\S+).*", "$1");
                String state = l.replaceAll(".*state=(\\S+).*", "$1");
                String attempts = l.replaceAll(".*attempts=(\\d+).*", "$1");
                m.put(id, state + "/" + attempts);
            }
        }
        return m;
    }

    private static List<String> attemptLines(List<String> lines, String jobId) {
        return lines.stream()
                .filter(l -> l.startsWith("ATTEMPT jobId=" + jobId + " "))
                .toList();
    }

    @Test
    void runningJobRecoversAndSuccessNotRepeatedAfterRealKill(@TempDir Path dir) throws Exception {
        // 第一个进程：ok 成功、slow 正在执行时被真实杀掉
        Proc first = spawn(dir, "run");
        first.awaitLine("READY", Duration.ofSeconds(30));
        first.killForcibly();

        // 第二个进程：从同一目录恢复
        Proc second = spawn(dir, "verify");
        List<String> out = second.awaitDoneAndCollect(Duration.ofSeconds(60));

        Map<String, String> jobs = jobStates(out);
        log.info("[真实重启判定] 恢复后作业状态: {}", jobs);
        assertEquals("SUCCESS/1", jobs.get("ok"), "已成功作业不得重复执行");
        assertEquals("SUCCESS/2", jobs.get("slow"), "被杀时 RUNNING 的作业应重新执行并成功（共 2 次尝试）");

        List<String> okAttempts = attemptLines(out, "ok");
        assertEquals(1, okAttempts.size(), "ok 的执行记录不得因重启增多");
        assertTrue(okAttempts.get(0).contains("result=SUCCESS"));

        List<String> slowAttempts = attemptLines(out, "slow");
        log.info("[真实重启判定] slow 执行记录: {}", slowAttempts);
        assertEquals(2, slowAttempts.size(), "被杀 1 次 + 重新执行 1 次，记录必须真实");
        assertTrue(slowAttempts.get(0).contains("result=INTERRUPTED"), "被杀的那次执行应收尾为 INTERRUPTED");
        assertTrue(slowAttempts.get(1).contains("result=SUCCESS"));
    }

    @Test
    void pauseSurvivesRealKillAndResumeContinues(@TempDir Path dir) throws Exception {
        // 第一个进程：暂停调度器并提交 held，随后被真实杀掉
        Proc first = spawn(dir, "run-paused");
        first.awaitLine("READY", Duration.ofSeconds(30));
        first.killForcibly();

        // 第二个进程：重启后必须仍是暂停，held 不得被偷偷执行；恢复后继续推进
        Proc second = spawn(dir, "verify-resume");
        List<String> out = second.awaitDoneAndCollect(Duration.ofSeconds(60));

        assertTrue(out.contains("PAUSED true"), "暂停开关必须在真实重启后保持，输出: " + out);
        List<String> jobLines = out.stream().filter(l -> l.startsWith("JOB ")).toList();
        log.info("[真实重启判定] 暂停期与恢复后的 held 状态: {}", jobLines);
        assertEquals(2, jobLines.size(), "应打印恢复前、后各一次 held 状态");
        assertTrue(jobLines.get(0).contains("state=PENDING attempts=0")
                || jobLines.get(0).contains("state=QUEUED attempts=0"),
                "暂停期间（含重启后）held 不得被执行: " + jobLines.get(0));
        assertTrue(jobLines.get(1).contains("state=SUCCESS attempts=1"),
                "恢复后 held 应恰好执行一次: " + jobLines.get(1));
        assertEquals(1, attemptLines(out, "held").size(), "held 只执行一次，记录只有一条");
    }

    @Test
    void timeoutRetryKeepsExactAttemptRecordsAcrossRealKill(@TempDir Path dir) throws Exception {
        // 第一个进程：第 1 次尝试已判 TIMEOUT（1 条记录），作业处于重试退避窗口时被真实杀掉
        Proc first = spawn(dir, "run-timeout-retry");
        first.awaitLine("READY", Duration.ofSeconds(30));
        first.killForcibly();

        // 第二个进程：从同一目录恢复，第 2 次尝试立即成功（slowOnce 只有第 1 次睡眠）
        Proc second = spawn(dir, "verify-timeout-retry");
        List<String> out = second.awaitDoneAndCollect(Duration.ofSeconds(60));

        Map<String, String> jobs = jobStates(out);
        log.info("[真实重启判定] 超时重试作业恢复后状态: {}", jobs);
        // 第 1 次 TIMEOUT（杀前已落盘）+ 第 2 次 SUCCESS，重启不虚增、不丢失
        assertEquals("SUCCESS/2", jobs.get("tjob"), "恢复后应恰好再执行一次并成功: " + jobs);

        List<String> attempts = attemptLines(out, "tjob");
        log.info("[真实重启判定] tjob 执行记录: {}", attempts);
        assertEquals(2, attempts.size(), "记录条数必须等于真实尝试次数（杀前 1 + 恢复后 1），不虚增不丢失");
        assertTrue(attempts.get(0).contains("attempt=1 result=TIMEOUT"),
                "杀前已判定的 TIMEOUT 记录必须原样保留: " + attempts.get(0));
        assertTrue(attempts.get(1).contains("attempt=2 result=SUCCESS"),
                "恢复后的第 2 次尝试成功: " + attempts.get(1));
    }
}
