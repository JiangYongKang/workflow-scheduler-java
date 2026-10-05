package com.github.highcumontoa.workflowschedulerjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 真实进程重启验证：启动独立 JVM 运行调度服务，kill 掉再重新拉起，
 * 验证暂停状态、作业状态与执行记录在重启后的表现（不是同进程内模拟重启）。
 */
class ProcessRestartTest {

    private static final Logger log = LoggerFactory.getLogger(ProcessRestartTest.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @Test
    void pauseStateAndAttemptRecordsSurviveRealProcessRestart(@TempDir Path dir) throws Exception {
        Path storeDir = dir.resolve("store");
        Path logDir = dir.resolve("logs");

        // ---- 第一个进程：完成一个作业，暂停，然后被杀掉 ----
        int port1 = freePort();
        Process p1 = startApp(port1, storeDir, logDir.resolve("p1.log"));
        try {
            awaitHealth(port1);
            submit(port1, "{\"jobs\":[{\"id\":\"done\",\"taskType\":\"noop\"}]}");
            awaitJobState(port1, "done", "SUCCESS", Duration.ofSeconds(15));

            // 暂停后提交的作业不得执行；再提交一个带超时配置的作业验证配置持久化
            post(port1, "/api/scheduler/pause");
            submit(port1, "{\"jobs\":["
                    + "{\"id\":\"held\",\"taskType\":\"sleep\",\"taskParam\":1500},"
                    + "{\"id\":\"tjob\",\"taskType\":\"sleep\",\"taskParam\":30000,\"timeoutMillis\":300}"
                    + "]}");
            Thread.sleep(400);
            assertEquals(0, getJob(port1, "held").get("attempts").asInt(), "暂停期间不得启动新执行");
            log.info("[真实重启] 进程 1 已暂停并提交 held/tjob，准备 kill");
        } finally {
            p1.destroyForcibly();
            p1.waitFor();
        }
        assertTrue(p1.exitValue() != 0, "进程应被强制杀掉（模拟崩溃而非优雅退出）");

        // ---- 第二个进程：同目录拉起，验证暂停保持、记录真实、恢复后继续 ----
        int port2 = freePort();
        Process p2 = startApp(port2, storeDir, logDir.resolve("p2.log"));
        try {
            awaitHealth(port2);
            assertTrue(getStatus(port2).get("paused").asBoolean(), "重启后暂停状态必须保持");

            // 已成功作业绝不重复执行
            JsonNode done = getJob(port2, "done");
            assertEquals("SUCCESS", done.get("state").asText());
            assertEquals(1, done.get("attempts").asInt(), "已成功作业重启后不得重复执行");

            // 暂停保持：held 不得被偷跑
            Thread.sleep(400);
            JsonNode held = getJob(port2, "held");
            assertEquals(0, held.get("attempts").asInt(), "暂停保持期间不得启动新执行");
            assertTrue(held.get("state").asText().equals("PENDING")
                    || held.get("state").asText().equals("QUEUED"));

            // 执行记录跨重启不丢不重：done 恰好 1 条成功记录
            JsonNode doneAttempts = getAttempts(port2, "done");
            assertEquals(1, doneAttempts.size(), "done 的执行记录应恰好 1 条");
            assertEquals("SUCCESS", doneAttempts.get(0).get("result").asText());

            // 恢复：held 正常跑完；tjob 按持久化的超时配置失败
            post(port2, "/api/scheduler/resume");
            awaitJobState(port2, "held", "SUCCESS", Duration.ofSeconds(15));
            JsonNode tjob = awaitJobState(port2, "tjob", "FAILED", Duration.ofSeconds(15));
            assertTrue(tjob.get("failureReason").asText().contains("超时"),
                    "重启后超时配置仍生效，实际: " + tjob.get("failureReason").asText());

            // 恢复后记录条数真实：held 1 条成功，tjob 1 条超时失败，done 仍只有 1 条
            assertEquals(1, getAttempts(port2, "held").size());
            JsonNode tjobAttempts = getAttempts(port2, "tjob");
            assertEquals(1, tjobAttempts.size());
            assertEquals("FAILED", tjobAttempts.get(0).get("result").asText());
            assertEquals(1, getAttempts(port2, "done").size(), "done 不得因重启多出记录");
            log.info("[真实重启] 恢复后 held 成功、tjob 超时失败、记录条数真实");
        } finally {
            p2.destroyForcibly();
            p2.waitFor();
        }
    }

    @Test
    void runningJobInterruptedByKillIsRecordedAndRerun(@TempDir Path dir) throws Exception {
        Path storeDir = dir.resolve("store");
        Path logDir = dir.resolve("logs");

        int port1 = freePort();
        Process p1 = startApp(port1, storeDir, logDir.resolve("p1.log"));
        try {
            awaitHealth(port1);
            submit(port1, "{\"jobs\":[{\"id\":\"crash\",\"taskType\":\"sleep\",\"taskParam\":30000}]}");
            awaitJobState(port1, "crash", "RUNNING", Duration.ofSeconds(15));
            log.info("[真实重启] crash 运行中，kill 进程模拟崩溃");
        } finally {
            p1.destroyForcibly();
            p1.waitFor();
        }

        int port2 = freePort();
        Process p2 = startApp(port2, storeDir, logDir.resolve("p2.log"));
        try {
            awaitHealth(port2);
            // 崩溃中断的尝试必须补一条 CANCELLED 记录，且作业被重新调度（at-least-once）
            JsonNode attempts = getAttempts(port2, "crash");
            assertEquals(1, attempts.size(), "被 kill 的尝试应补一条记录");
            assertEquals("CANCELLED", attempts.get(0).get("result").asText());
            assertEquals(1, attempts.get(0).get("attempt").asInt());

            // 缩短耗时不可能（任务是 30s sleep），这里只验证它被重新拉起执行
            JsonNode crash = awaitJobState(port2, "crash", "RUNNING", Duration.ofSeconds(15));
            assertEquals(2, crash.get("attempts").asInt(), "重启后应重新调度执行");
            log.info("[真实重启] crash 中断记录已补，第 2 次尝试运行中");
        } finally {
            p2.destroyForcibly();
            p2.waitFor();
        }
    }

    /* ============================= 进程与 HTTP 辅助 ============================= */

    private static Process startApp(int port, Path storeDir, Path logFile) throws IOException {
        java.nio.file.Files.createDirectories(storeDir);
        java.nio.file.Files.createDirectories(logFile.getParent());
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        List<String> cmd = new ArrayList<>(List.of(
                java, "-cp", System.getProperty("java.class.path"),
                "com.github.highcumontoa.workflowschedulerjava.Application",
                "--server.port=" + port,
                "--scheduler.store-dir=" + storeDir,
                "--scheduler.tick-millis=20",
                "--spring.main.banner-mode=off",
                "--logging.level.root=WARN"));
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        pb.redirectOutput(logFile.toFile());
        return pb.start();
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static void awaitHealth(int port) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                HttpResponse<String> resp = HTTP.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + "/api/health"))
                        .timeout(Duration.ofSeconds(2)).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() == 200) {
                    return;
                }
            } catch (IOException ignored) {
                // 进程尚未就绪
            }
            Thread.sleep(200);
        }
        throw new AssertionError("端口 " + port + " 的服务未在 30s 内就绪");
    }

    private static JsonNode awaitJobState(int port, String id, String expected, Duration timeout)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        JsonNode last = null;
        while (System.currentTimeMillis() < deadline) {
            last = getJob(port, id);
            if (last.get("state").asText().equals(expected)) {
                return last;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("job=" + id + " 未在 " + timeout + " 内到达 " + expected
                + "，实际=" + (last == null ? "null" : last.get("state").asText()));
    }

    private static JsonNode getJob(int port, String id) throws Exception {
        return MAPPER.readTree(get(port, "/api/jobs/" + id));
    }

    private static JsonNode getAttempts(int port, String id) throws Exception {
        return MAPPER.readTree(get(port, "/api/jobs/" + id + "/attempts"));
    }

    private static JsonNode getStatus(int port) throws Exception {
        return MAPPER.readTree(get(port, "/api/scheduler/status"));
    }

    private static String get(int port, String path) throws Exception {
        HttpResponse<String> resp = HTTP.send(HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(5)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode(), "GET " + path + " 应成功，body=" + resp.body());
        return resp.body();
    }

    private static void post(int port, String path) throws Exception {
        HttpResponse<String> resp = HTTP.send(HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode(), "POST " + path + " 应成功，body=" + resp.body());
    }

    private static void submit(int port, String json) throws Exception {
        HttpResponse<String> resp = HTTP.send(HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/jobs"))
                .timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode(), "提交应成功，body=" + resp.body());
    }
}
