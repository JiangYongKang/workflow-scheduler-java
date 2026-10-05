package com.github.highcumontoa.workflowschedulerjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 新能力的 HTTP 端到端：执行记录查询、暂停/恢复/状态、单作业超时。 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "scheduler.store-dir=target/test-data/scheduler-api",
        "scheduler.global-concurrency=2",
        "scheduler.tick-millis=10"
})
class SchedulerApiTest {

    static {
        // 类加载即清理（早于 Spring 上下文创建），保证同一台机器反复执行结果一致
        EngineTestSupport.deleteRecursively(Path.of("target/test-data/scheduler-api"));
    }

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @Autowired
    ObjectMapper mapper;

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private HttpEntity<String> json(String body) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        return new HttpEntity<>(body, h);
    }

    @Test
    void attemptRecordsAreQueryableInOrderOverHttp() throws Exception {
        String body = "{\"jobs\":[{\"id\":\"api-flaky\",\"taskType\":\"flaky\",\"taskParam\":1,"
                + "\"retryPolicy\":{\"maxAttempts\":2,\"backoffMillis\":20}}]}";
        rest.postForEntity(url("/api/jobs"), json(body), String.class);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            JsonNode j = mapper.readTree(rest.getForEntity(url("/api/jobs/api-flaky"), String.class).getBody());
            assertEquals("SUCCESS", j.get("state").asText());
        });
        ResponseEntity<String> resp = rest.getForEntity(url("/api/jobs/api-flaky/attempts"), String.class);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        JsonNode records = mapper.readTree(resp.getBody());
        assertEquals(2, records.size(), "失败 1 次 + 成功 1 次，各一条记录");
        assertEquals(1, records.get(0).get("attempt").asInt());
        assertEquals("FAILED", records.get(0).get("result").asText());
        assertTrue(records.get(0).get("error").asText().contains("模拟失败"));
        assertEquals(2, records.get(1).get("attempt").asInt());
        assertEquals("SUCCESS", records.get(1).get("result").asText());
        assertTrue(records.get(0).get("startedAt").asLong() > 0);
        assertTrue(records.get(0).get("finishedAt").asLong() >= records.get(0).get("startedAt").asLong());
    }

    @Test
    void attemptsOfUnknownJobReturn404() {
        ResponseEntity<String> resp = rest.getForEntity(url("/api/jobs/nope/attempts"), String.class);
        assertEquals(HttpStatus.NOT_FOUND, resp.getStatusCode());
    }

    @Test
    void pauseResumeAndStatusOverHttp() throws Exception {
        ResponseEntity<String> paused = rest.exchange(
                url("/api/scheduler/pause"), HttpMethod.POST, HttpEntity.EMPTY, String.class);
        assertEquals(HttpStatus.OK, paused.getStatusCode());
        assertTrue(mapper.readTree(paused.getBody()).get("paused").asBoolean());

        // 暂停期间提交的作业不得启动
        rest.postForEntity(url("/api/jobs"), json("{\"jobs\":[{\"id\":\"api-held\"}]}"), String.class);
        Thread.sleep(300);
        JsonNode held = mapper.readTree(rest.getForEntity(url("/api/jobs/api-held"), String.class).getBody());
        assertEquals(0, held.get("attempts").asInt(), "暂停期间不得启动新执行");

        ResponseEntity<String> resumed = rest.exchange(
                url("/api/scheduler/resume"), HttpMethod.POST, HttpEntity.EMPTY, String.class);
        assertFalse(mapper.readTree(resumed.getBody()).get("paused").asBoolean());
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            JsonNode j = mapper.readTree(rest.getForEntity(url("/api/jobs/api-held"), String.class).getBody());
            assertEquals("SUCCESS", j.get("state").asText());
        });
        JsonNode status = mapper.readTree(rest.getForEntity(url("/api/scheduler/status"), String.class).getBody());
        assertFalse(status.get("paused").asBoolean());
    }

    @Test
    void timeoutFailsJobOverHttp() throws Exception {
        String body = "{\"jobs\":[{\"id\":\"api-timeout\",\"taskType\":\"sleep\",\"taskParam\":3000,"
                + "\"timeoutMillis\":150}]}";
        rest.postForEntity(url("/api/jobs"), json(body), String.class);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            JsonNode j = mapper.readTree(rest.getForEntity(url("/api/jobs/api-timeout"), String.class).getBody());
            assertEquals("FAILED", j.get("state").asText());
            assertTrue(j.get("failureReason").asText().contains("TimeoutException"));
            assertEquals(150, j.get("timeoutMillis").asInt());
        });
        JsonNode records = mapper.readTree(
                rest.getForEntity(url("/api/jobs/api-timeout/attempts"), String.class).getBody());
        assertEquals(1, records.size());
        assertEquals("TIMEOUT", records.get(0).get("result").asText());
    }
}
