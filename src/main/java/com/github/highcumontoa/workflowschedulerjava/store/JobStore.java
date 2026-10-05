package com.github.highcumontoa.workflowschedulerjava.store;

import com.github.highcumontoa.workflowschedulerjava.model.AttemptRecord;
import com.github.highcumontoa.workflowschedulerjava.model.Job;
import java.util.List;

/** 持久化存储。 */
public interface JobStore {
    List<Job> loadAll();
    void put(Job job);
    Job get(String id);
    List<Job> all();

    /** 追加一条执行尝试记录（只增不改，立即持久化）。 */
    void appendAttempt(AttemptRecord record);

    /** 启动时载入全部执行记录（按追加顺序）。 */
    List<AttemptRecord> loadAttempts();

    /** 按作业 id 查询执行记录，按尝试序号升序。 */
    List<AttemptRecord> attemptsOf(String jobId);
}
