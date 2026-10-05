package com.github.highcumontoa.workflowschedulerjava.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.workflowschedulerjava.model.AttemptRecord;
import com.github.highcumontoa.workflowschedulerjava.model.Job;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于单个 JSON 文件的持久化实现。
 *
 * <p>每次状态迁移都会重写 {@code jobs.json}（先写临时文件再原子 move），保证进程崩溃时
 * 文件要么是旧内容要么是新内容。调度引擎自身持有锁，因此这里不再加锁；内存 map 作为
 * 读缓存，{@link #loadAll()} 仅在启动恢复时调用。</p>
 */
public class JsonFileJobStore implements JobStore {

    private static final Logger log = LoggerFactory.getLogger(JsonFileJobStore.class);

    private final Path file;
    private final Path attemptsFile;
    private final ObjectMapper mapper;
    private final java.util.Map<String, Job> cache = new java.util.HashMap<>();
    private final List<AttemptRecord> attempts = new ArrayList<>();

    public JsonFileJobStore(Path dir, ObjectMapper mapper) {
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("无法创建存储目录: " + dir, e);
        }
        this.file = dir.resolve("jobs.json");
        this.attemptsFile = dir.resolve("attempts.json");
        this.mapper = mapper;
    }

    @Override
    public synchronized List<Job> loadAll() {
        cache.clear();
        if (!Files.exists(file)) {
            log.info("持久化文件不存在，按空状态启动: {}", file);
            return new ArrayList<>();
        }
        try {
            String json = Files.readString(file, StandardCharsets.UTF_8);
            if (json.isBlank()) {
                return new ArrayList<>();
            }
            List<Job> jobs = mapper.readValue(json, new TypeReference<List<Job>>() {
            });
            for (Job job : jobs) {
                cache.put(job.getId(), job);
            }
            log.info("从 {} 恢复作业 {} 个", file, jobs.size());
            return jobs;
        } catch (IOException e) {
            throw new UncheckedIOException("读取持久化文件失败: " + file, e);
        }
    }

    @Override
    public synchronized void put(Job job) {
        Objects.requireNonNull(job.getId(), "job id");
        cache.put(job.getId(), job);
        persist();
    }

    @Override
    public synchronized Job get(String id) {
        return cache.get(id);
    }

    @Override
    public synchronized List<Job> all() {
        return new ArrayList<>(cache.values());
    }

    private void persist() {
        try {
            Path tmp = file.resolveSibling("jobs.json.tmp");
            String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(new ArrayList<>(cache.values()));
            Files.writeString(tmp, json, StandardCharsets.UTF_8);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("持久化失败: " + file, e);
        }
    }

    /* ===================== 执行记录 ===================== */

    @Override
    public synchronized void appendAttempt(AttemptRecord record) {
        Objects.requireNonNull(record.getJobId(), "job id");
        attempts.add(record);
        persistAttempts();
    }

    @Override
    public synchronized List<AttemptRecord> loadAttempts() {
        attempts.clear();
        if (!Files.exists(attemptsFile)) {
            log.info("执行记录文件不存在，按空记录启动: {}", attemptsFile);
            return new ArrayList<>();
        }
        try {
            String json = Files.readString(attemptsFile, StandardCharsets.UTF_8);
            if (json.isBlank()) {
                return new ArrayList<>();
            }
            List<AttemptRecord> loaded = mapper.readValue(json, new TypeReference<List<AttemptRecord>>() {
            });
            attempts.addAll(loaded);
            log.info("从 {} 恢复执行记录 {} 条", attemptsFile, loaded.size());
            return new ArrayList<>(attempts);
        } catch (IOException e) {
            throw new UncheckedIOException("读取执行记录文件失败: " + attemptsFile, e);
        }
    }

    @Override
    public synchronized List<AttemptRecord> attemptsOf(String jobId) {
        return attempts.stream()
                .filter(r -> r.getJobId().equals(jobId))
                .sorted(java.util.Comparator.comparingInt(AttemptRecord::getAttempt))
                .toList();
    }

    private void persistAttempts() {
        try {
            Path tmp = attemptsFile.resolveSibling("attempts.json.tmp");
            String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(attempts);
            Files.writeString(tmp, json, StandardCharsets.UTF_8);
            Files.move(tmp, attemptsFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("执行记录持久化失败: " + attemptsFile, e);
        }
    }
}
