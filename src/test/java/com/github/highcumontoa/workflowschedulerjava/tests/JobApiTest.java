package com.github.highcumontoa.workflowschedulerjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端 HTTP：提交/查询/取消，以及各类准入拒绝的可区分 code。
 * 每个类用独立 store 目录，避免历史数据干扰。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "scheduler.store-dir=target/test-data/api-1790139400",
        "scheduler.global-concurrency=2",
        "scheduler.tick-millis=10"
})
class JobApiTest {

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
    void submitQueryAndCancelOverHttp() throws Exception {
        String sleepJob = "{\"jobs\":[{\"id\":\"http-sleep\",\"taskType\":\"sleep\",\"taskParam\":3000}]}";
        ResponseEntity<String> created = rest.postForEntity(url("/api/jobs"), json(sleepJob), String.class);
        assertEquals(HttpStatus.OK, created.getStatusCode());
        JsonNode arr = mapper.readTree(created.getBody());
        assertEquals("http-sleep", arr.get(0).get("jobId").asText());

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            ResponseEntity<String> got = rest.getForEntity(url("/api/jobs/http-sleep"), String.class);
            assertEquals("RUNNING", mapper.readTree(got.getBody()).get("state").asText());
        });

        ResponseEntity<String> cancelled = rest.exchange(
                url("/api/jobs/http-sleep/cancel"), HttpMethod.POST, HttpEntity.EMPTY, String.class);
        assertEquals(HttpStatus.OK, cancelled.getStatusCode());

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            JsonNode j = mapper.readTree(rest.getForEntity(url("/api/jobs/http-sleep"), String.class).getBody());
            assertEquals("CANCELLED", j.get("state").asText());
        });
        // 取消已成功作业应得到 CANCEL_NOT_ALLOWED（先提交一个必成功作业）
        String okJob = "{\"jobs\":[{\"id\":\"http-ok\",\"taskType\":\"noop\"}]}";
        rest.postForEntity(url("/api/jobs"), json(okJob), String.class);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            JsonNode j = mapper.readTree(rest.getForEntity(url("/api/jobs/http-ok"), String.class).getBody());
            assertEquals("SUCCESS", j.get("state").asText());
        });
        ResponseEntity<String> badCancel = rest.exchange(
                url("/api/jobs/http-ok/cancel"), HttpMethod.POST, HttpEntity.EMPTY, String.class);
        assertEquals(HttpStatus.CONFLICT, badCancel.getStatusCode());
        JsonNode err = mapper.readTree(badCancel.getBody());
        assertEquals("CANCEL_NOT_ALLOWED", err.get("code").asText());
    }

    @Test
    void missingUpstreamReturnsDistinctCode() throws Exception {
        String body = "{\"jobs\":[{\"id\":\"m1\",\"dependencies\":[\"nope\"]}]}";
        ResponseEntity<String> resp = rest.postForEntity(url("/api/jobs"), json(body), String.class);
        assertEquals(HttpStatus.BAD_REQUEST, resp.getStatusCode());
        JsonNode err = mapper.readTree(resp.getBody());
        assertEquals("UPSTREAM_NOT_FOUND", err.get("code").asText());
        assertTrue(err.get("message").asText().contains("nope"));
    }

    @Test
    void cycleReturnsDistinctCode() throws Exception {
        String body = "{\"jobs\":["
                + "{\"id\":\"c1\",\"dependencies\":[\"c3\"]},"
                + "{\"id\":\"c2\",\"dependencies\":[\"c1\"]},"
                + "{\"id\":\"c3\",\"dependencies\":[\"c2\"]}]}";
        ResponseEntity<String> resp = rest.postForEntity(url("/api/jobs"), json(body), String.class);
        assertEquals(HttpStatus.BAD_REQUEST, resp.getStatusCode());
        assertEquals("CYCLIC_DEPENDENCY", mapper.readTree(resp.getBody()).get("code").asText());
    }

    @Test
    void duplicateIdInBatchReturnsDistinctCode() throws Exception {
        String body = "{\"jobs\":[{\"id\":\"d1\"},{\"id\":\"d1\"}]}";
        ResponseEntity<String> resp = rest.postForEntity(url("/api/jobs"), json(body), String.class);
        assertEquals(HttpStatus.BAD_REQUEST, resp.getStatusCode());
        assertEquals("DUPLICATE_JOB_ID", mapper.readTree(resp.getBody()).get("code").asText());
    }

    @Test
    void listAndHealthEndpointsWork() {
        ResponseEntity<Map> health = rest.getForEntity(url("/api/health"), Map.class);
        assertEquals(HttpStatus.OK, health.getStatusCode());
        ResponseEntity<List> list = rest.getForEntity(url("/api/jobs"), List.class);
        assertEquals(HttpStatus.OK, list.getStatusCode());
    }
}
