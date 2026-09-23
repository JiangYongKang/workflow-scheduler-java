package com.github.highcumontoa.workflowschedulerjava.store;

import com.github.highcumontoa.workflowschedulerjava.model.Job;
import java.util.List;

/** 持久化存储（骨架占位）。 */
public interface JobStore {
    List<Job> loadAll();
    void put(Job job);
    Job get(String id);
    List<Job> all();
}
