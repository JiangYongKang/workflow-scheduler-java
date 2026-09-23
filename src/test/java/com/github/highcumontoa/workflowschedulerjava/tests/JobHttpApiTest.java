package com.github.highcumontoa.workflowschedulerjava.tests;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.highcumontoa.workflowschedulerjava.service.JobService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** HTTP 接口契约：状态码、错误码可区分、幂等标记透出。 */
@SpringBootTest
@AutoConfigureMockMvc
class JobHttpApiTest {

    private static final Logger log = LoggerFactory.getLogger(JobHttpApiTest.class);

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JobService jobService;

    @Test
    @DisplayName("提交-查询-未知任务类型/未知上游/环：HTTP 状态码与错误码正确")
    void httpContracts() throws Exception {
        // 正常提交 200
        mockMvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\":\"http-1\",\"jobType\":\"NOOP\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value("http-1"))
                .andExpect(jsonPath("$.deduplicated").value(false));
        log.info("HTTP 正常提交 200 校验通过");

        // 查询 200
        mockMvc.perform(get("/api/jobs/http-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jobType").value("NOOP"));

        // 查询不存在 404 + JOB_NOT_FOUND
        MvcResult nf = mockMvc.perform(get("/api/jobs/ghost-http"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("JOB_NOT_FOUND"))
                .andReturn();
        log.info("HTTP 404 body={}", nf.getResponse().getContentAsString());

        // 未知任务类型 400 + UNKNOWN_JOB_TYPE
        mockMvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\":\"http-bad\",\"jobType\":\"NOPE\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("UNKNOWN_JOB_TYPE"));

        // 未知上游 400 + UNKNOWN_DEPENDENCY（与 UNKNOWN_JOB_TYPE 可区分）
        mockMvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\":\"http-bad2\",\"jobType\":\"NOOP\",\"dependencies\":[\"ghost\"]}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("UNKNOWN_DEPENDENCY"));

        // 环 400 + CYCLE_DETECTED
        mockMvc.perform(post("/api/jobs/batch").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"jobs\":["
                                + "{\"id\":\"cy1\",\"jobType\":\"NOOP\",\"dependencies\":[\"cy2\"]},"
                                + "{\"id\":\"cy2\",\"jobType\":\"NOOP\",\"dependencies\":[\"cy1\"]}]}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("CYCLE_DETECTED"));
        log.info("HTTP 三类 400 错误码可区分校验通过");
    }

    @Test
    @DisplayName("相同幂等键重复提交：HTTP 200 且 deduplicated=true，作业不重复")
    void httpIdempotent() throws Exception {
        String body = "{\"id\":\"http-idem-1\",\"idempotencyKey\":\"http-key-1\",\"jobType\":\"NOOP\"}";
        mockMvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value("http-idem-1"))
                .andExpect(jsonPath("$.deduplicated").value(false));
        String dup = "{\"id\":\"http-idem-2\",\"idempotencyKey\":\"http-key-1\",\"jobType\":\"NOOP\"}";
        MvcResult r = mockMvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON).content(dup))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value("http-idem-1"))
                .andExpect(jsonPath("$.deduplicated").value(true))
                .andReturn();
        log.info("HTTP 幂等重复提交返回既有作业 body={}", r.getResponse().getContentAsString());
    }

    @Test
    @DisplayName("终态后取消返回 409 ALREADY_TERMINAL")
    void httpCancelTerminal() throws Exception {
        mockMvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\":\"http-term\",\"jobType\":\"NOOP\"}"))
                .andExpect(status().isOk());
        TestAwait.awaitStatus(jobService, "http-term", "SUCCESS", 5000);
        mockMvc.perform(delete("/api/jobs/http-term"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("ALREADY_TERMINAL"));
        log.info("HTTP 409 ALREADY_TERMINAL 校验通过");
    }
}
