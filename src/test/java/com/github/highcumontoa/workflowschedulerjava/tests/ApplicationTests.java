package com.github.highcumontoa.workflowschedulerjava.tests;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

/**
 * Spring 上下文装载冒烟测试。使用独立 store 目录并在上下文启动前清理，
 * 既不污染仓库的 data/ 目录，也不受历史数据影响，可反复执行。
 */
@SpringBootTest
@TestPropertySource(properties = {
        "scheduler.store-dir=target/test-data/app-context"
})
public class ApplicationTests {

    static {
        EngineTestSupport.deleteRecursively(Path.of("target/test-data/app-context"));
    }

    @Test
    public void contentLoad() {
    }
}
