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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    private final Path metaFile;
    private final ObjectMapper mapper;
    private final java.util.Map<String, Job> cache = new java.util.HashMap<>();
    /** 执行记录缓存：jobId -> (attempt -> record)，LinkedHashMap 保持插入序。 */
    private final Map<String, Map<Integer, AttemptRecord>> attempts = new java.util.HashMap<>();
    /** 调度器级元数据（如暂停开关），随 meta.json 持久化。 */
    private final Map<String, String> meta = new java.util.HashMap<>();

    public JsonFileJobStore(Path dir, ObjectMapper mapper) {
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("无法创建存储目录: " + dir, e);
        }
        this.file = dir.resolve("jobs.json");
        this.attemptsFile = dir.resolve("attempts.json");
        this.metaFile = dir.resolve("meta.json");
        this.mapper = mapper;
    }

    @Override
    public synchronized List<Job> loadAll() {
        cache.clear();
        attempts.clear();
        meta.clear();
        loadAttempts();
        loadMeta();
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

    @Override
    public synchronized void saveAttempt(AttemptRecord record) {
        Objects.requireNonNull(record.getJobId(), "jobId");
        attempts.computeIfAbsent(record.getJobId(), k -> new LinkedHashMap<>())
                .put(record.getAttempt(), record);
        persistAttempts();
    }

    @Override
    public synchronized List<AttemptRecord> listAttempts(String jobId) {
        return attempts.getOrDefault(jobId, Map.of()).values().stream()
                .sorted(Comparator.comparingInt(AttemptRecord::getAttempt))
                .toList();
    }

    private void loadAttempts() {
        if (!Files.exists(attemptsFile)) {
            return;
        }
        try {
            String json = Files.readString(attemptsFile, StandardCharsets.UTF_8);
            if (json.isBlank()) {
                return;
            }
            List<AttemptRecord> records = mapper.readValue(json, new TypeReference<List<AttemptRecord>>() {
            });
            for (AttemptRecord r : records) {
                attempts.computeIfAbsent(r.getJobId(), k -> new LinkedHashMap<>())
                        .put(r.getAttempt(), r);
            }
            log.info("从 {} 恢复执行记录 {} 条", attemptsFile, records.size());
        } catch (IOException e) {
            throw new UncheckedIOException("读取执行记录文件失败: " + attemptsFile, e);
        }
    }

    private void persistAttempts() {
        try {
            Path tmp = attemptsFile.resolveSibling("attempts.json.tmp");
            List<AttemptRecord> all = new ArrayList<>();
            attempts.values().forEach(m -> all.addAll(m.values()));
            String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(all);
            Files.writeString(tmp, json, StandardCharsets.UTF_8);
            Files.move(tmp, attemptsFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("执行记录持久化失败: " + attemptsFile, e);
        }
    }

    @Override
    public synchronized String getMeta(String key) {
        return meta.get(key);
    }

    @Override
    public synchronized void putMeta(String key, String value) {
        meta.put(key, value);
        persistMeta();
    }

    private void loadMeta() {
        if (!Files.exists(metaFile)) {
            return;
        }
        try {
            String json = Files.readString(metaFile, StandardCharsets.UTF_8);
            if (json.isBlank()) {
                return;
            }
            meta.putAll(mapper.readValue(json, new TypeReference<Map<String, String>>() {
            }));
            log.info("从 {} 恢复元数据 {} 项", metaFile, meta.size());
        } catch (IOException e) {
            throw new UncheckedIOException("读取元数据文件失败: " + metaFile, e);
        }
    }

    private void persistMeta() {
        try {
            Path tmp = metaFile.resolveSibling("meta.json.tmp");
            String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(meta);
            Files.writeString(tmp, json, StandardCharsets.UTF_8);
            Files.move(tmp, metaFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("元数据持久化失败: " + metaFile, e);
        }
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
}
