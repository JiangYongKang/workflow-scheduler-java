package com.github.highcumontoa.workflowschedulerjava.store;

import com.github.highcumontoa.workflowschedulerjava.model.AttemptRecord;
import com.github.highcumontoa.workflowschedulerjava.model.Job;
import java.util.List;

/** 持久化存储（骨架占位）。 */
public interface JobStore {
    List<Job> loadAll();
    void put(Job job);
    Job get(String id);
    List<Job> all();

    /** 追加或更新一条执行记录（按 jobId+attempt 唯一）。 */
    void saveAttempt(AttemptRecord record);

    /** 按尝试序号升序返回某作业的全部执行记录。 */
    List<AttemptRecord> listAttempts(String jobId);

    /** 读取调度器级元数据（如暂停开关）；不存在返回 null。 */
    String getMeta(String key);

    /** 写入调度器级元数据并持久化。 */
    void putMeta(String key, String value);
}
