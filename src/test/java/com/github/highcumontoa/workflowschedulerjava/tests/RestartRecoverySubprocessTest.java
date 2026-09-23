package com.github.highcumontoa.workflowschedulerjava.tests;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 进程崩溃-重启恢复（真实子进程验证）：
 * <pre>
 * 进程1：提交 rr-done(NOOP)、rr-long(SLEEP 15s)、rr-down(依赖 rr-long)
 *        等 rr-done=SUCCESS、rr-long=RUNNING 后 kill -9 强杀；
 * 进程2：用同一个 H2 文件库重启，断言：
 *   rr-done 仍 SUCCESS 且只执行过 1 次；
 *   rr-long 被恢复并重新执行至 SUCCESS（attempts=2，at-least-once）；
 *   rr-down 未被跳过，在 rr-long 恢复成功后 SUCCESS（attempts=1）。
 * </pre>
 * 需要先 package 出可执行 jar；CI 无 jar 时该用例自动跳过。
 */
class RestartRecoverySubprocessTest {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoverySubprocessTest.class);

    private static final Path JAR = Path.of("target", "workflow-scheduler-java-1.0-SNAPSHOT-exec.jar");

    static boolean jarReady() {
        return Files.exists(JAR);
    }

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2)).build();

    @Test
    @EnabledIf("jarReady")
    @DisplayName("kill -9 崩溃后重启：已成功不重跑、在执行作业重跑、下游不跳过")
    void crashAndRecover() throws Exception {
        Path tmpDir = Files.createTempDirectory("scheduler-recovery-");
        Path dbDir = tmpDir.resolve("data");
        Files.createDirectories(dbDir);
        int port = 18000 + (int) (Math.random() * 200);
        String base = "http://localhost:" + port;
        String dbUrl = "jdbc:h2:file:" + dbDir.toAbsolutePath() + "/sched;WRITE_DELAY=0";

        Process first = startApp(port, dbUrl, tmpDir.resolve("app1.log"));
        try {
            waitUntilUp(base, 30_000);
            submit(base, "rr-done", "NOOP", null, null);
            submit(base, "rr-long", "SLEEP", "15000", null);
            submit(base, "rr-down", "NOOP", null, List.of("rr-long"));

            String done = awaitStatus(base, "rr-done", "SUCCESS", 10_000);
            String running = awaitStatus(base, "rr-long", "RUNNING", 10_000);
            log.info("崩溃前快照: rr-done={} rr-long={} rr-down=PENDING",
                    extract(done, "status"), extract(running, "status"));
            assertEquals(1, extractInt(done, "attempts"));

            log.info("判定依据: 强杀进程（kill -9，无优雅停机、无内存恢复）");
            first.destroyForcibly();
            assertTrue(first.waitFor(10, java.util.concurrent.TimeUnit.SECONDS),
                    "进程应被强杀退出");
        } finally {
            if (first.isAlive()) {
                first.destroyForcibly();
            }
        }

        // 进程 2：同一文件库重启
        Process second = startApp(port, dbUrl, tmpDir.resolve("app2.log"));
        try {
            waitUntilUp(base, 30_000);

            String rrDone = awaitStatus(base, "rr-done", "SUCCESS", 10_000);
            assertEquals(1, extractInt(rrDone, "attempts"),
                    "已成功作业重启后不得重复执行");

            String rrLong = awaitStatus(base, "rr-long", "SUCCESS", 25_000);
            assertEquals(2, extractInt(rrLong, "attempts"),
                    "崩溃时在执行的作业应按 at-least-once 重跑：第1次崩溃 + 恢复后1次");

            String rrDown = awaitStatus(base, "rr-down", "SUCCESS", 10_000);
            assertEquals(1, extractInt(rrDown, "attempts"),
                    "可恢复作业（下游）不得被跳过，且只在依赖满足后执行一次");

            log.info("重启恢复判定依据: rr-done attempts=1, rr-long attempts=2, rr-down attempts=1");
        } finally {
            second.destroy();
            second.waitFor(10, java.util.concurrent.TimeUnit.SECONDS);
            if (second.isAlive()) {
                second.destroyForcibly();
            }
        }
    }

    private Process startApp(int port, String dbUrl, Path logFile) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-jar", JAR.toAbsolutePath().toString(),
                "--server.port=" + port,
                "--spring.datasource.url=" + dbUrl,
                // 子进程恢复用稍快 tick
                "--scheduler.tick-interval-ms=100",
                "--scheduler.quota.global=4",
                "--scheduler.quota.default-queue=4",
                "--scheduler.quota.default-tenant=4",
                "--logging.level.com.github.highcumontoa=INFO");
        pb.redirectErrorStream(true);
        pb.redirectOutput(logFile.toFile());
        return pb.start();
    }

    private void waitUntilUp(String base, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        Exception last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                HttpResponse<String> r = http.send(
                        HttpRequest.newBuilder(URI.create(base + "/api/quota")).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
                if (r.statusCode() == 200) {
                    log.info("应用已就绪: {}", base);
                    return;
                }
            } catch (Exception e) {
                last = e;
            }
            Thread.sleep(300);
        }
        throw new IllegalStateException("应用在 " + timeoutMs + "ms 内未启动: " + base, last);
    }

    private void submit(String base, String id, String type, String payload, List<String> deps)
            throws Exception {
        String depJson = deps == null ? "null"
                : "[" + String.join(",", deps.stream().map(d -> "\"" + d + "\"").toList()) + "]";
        String body = "{\"id\":\"" + id + "\",\"jobType\":\"" + type + "\""
                + (payload == null ? "" : ",\"payload\":\"" + payload + "\"")
                + ",\"dependencies\":" + depJson + "}";
        HttpRequest req = HttpRequest.newBuilder(URI.create(base + "/api/jobs"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode(), () -> "提交失败: " + resp.body());
        log.info("已提交 job={} type={} payload={} deps={}", id, type, payload, depJson);
    }

    private String awaitStatus(String base, String id, String expected, long timeoutMs)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        String body = null;
        while (System.currentTimeMillis() < deadline) {
            HttpResponse<String> resp = http.send(
                    HttpRequest.newBuilder(URI.create(base + "/api/jobs/" + id)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            body = resp.body();
            String status = extract(body, "status");
            log.info("await job={} 期望={} 当前={} body={}", id, expected, status, body);
            if (expected.equals(status)) {
                return body;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("作业 " + id + " 未到达 " + expected + "，最终=" + body);
    }

    /** 极简 JSON 字段提取（响应体受控，无需引入 JSON 库）。 */
    private String extract(String json, String field) {
        String key = "\"" + field + "\":\"";
        int i = json.indexOf(key);
        if (i < 0) {
            return null;
        }
        int start = i + key.length();
        int end = json.indexOf('"', start);
        return json.substring(start, end);
    }

    private int extractInt(String json, String field) {
        String key = "\"" + field + "\":";
        int i = json.indexOf(key);
        if (i < 0) {
            return -1;
        }
        int start = i + key.length();
        int end = start;
        while (end < json.length() && Character.isDigit(json.charAt(end))) {
            end++;
        }
        return Integer.parseInt(json.substring(start, end));
    }
}
