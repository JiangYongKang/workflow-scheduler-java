package com.github.highcumontoa.workflowschedulerjava.tests;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * 上下文加载冒烟测试。store 目录每次运行新建，避免读写本地 data/scheduler，
 * 保证同一台机器上反复执行结果一致。
 */
@SpringBootTest
public class ApplicationTests {

    private static final Path STORE_DIR = createStoreDir();

    private static Path createStoreDir() {
        try {
            return Files.createTempDirectory("scheduler-app-test-");
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @DynamicPropertySource
    static void storeDir(DynamicPropertyRegistry registry) {
        registry.add("scheduler.store-dir", STORE_DIR::toString);
    }

    @Test
    public void contentLoad() {
    }
}
