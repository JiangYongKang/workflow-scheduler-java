package com.github.highcumontoa.workflowschedulerjava.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.workflowschedulerjava.store.JobStore;
import com.github.highcumontoa.workflowschedulerjava.store.JsonFileJobStore;
import java.nio.file.Path;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Bean 装配：TaskHandler 与 SchedulerEngine 由组件扫描注册，这里只装配存储。 */
@Configuration
@EnableConfigurationProperties(SchedulerProperties.class)
public class SchedulerConfig {

    @Bean
    public JobStore jobStore(SchedulerProperties props, ObjectMapper mapper) {
        return new JsonFileJobStore(Path.of(props.getStoreDir()), mapper);
    }
}
