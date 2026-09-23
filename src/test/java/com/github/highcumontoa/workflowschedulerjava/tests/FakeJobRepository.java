package com.github.highcumontoa.workflowschedulerjava.tests;

import com.github.highcumontoa.workflowschedulerjava.model.Job;
import com.github.highcumontoa.workflowschedulerjava.repo.JobRepository;

import java.util.HashSet;
import java.util.Set;

/** 单测用假仓储：内存集合模拟“已存在作业”，无需 Mockito/数据库。 */
class FakeJobRepository extends JobRepository {

    private final Set<String> existingIds;

    FakeJobRepository(String... ids) {
        super(null);
        this.existingIds = new HashSet<>(java.util.Arrays.asList(ids));
    }

    @Override
    public Job findById(String id) {
        return existingIds.contains(id) ? new Job() : null;
    }
}
