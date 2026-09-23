package com.github.highcumontoa.workflowschedulerjava.scheduler;

import org.springframework.stereotype.Component;

/** 调度唤醒信号：事件驱动 tick 尽快推进（提交/取消/重试退避结束），退化为最多等一个 tick。 */
@Component
public class SchedulerSignals {

    private final Object lock = new Object();

    /** 阻塞至被唤醒或超时。 */
    public void await(long timeoutMs) throws InterruptedException {
        synchronized (lock) {
            lock.wait(timeoutMs);
        }
    }

    public void wakeup() {
        synchronized (lock) {
            lock.notifyAll();
        }
    }
}
