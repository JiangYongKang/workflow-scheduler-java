package com.github.highcumontoa.workflowschedulerjava.model;

/**
 * 单次执行尝试的结果。
 *
 * <ul>
 *   <li>{@code STARTED}：已开始、尚未有结果（执行中，或崩溃后来不及落终态）；</li>
 *   <li>{@code SUCCESS}：本次执行成功；</li>
 *   <li>{@code FAILED}：本次执行失败（含重试中的失败）；</li>
 *   <li>{@code CANCELLED}：本次执行被取消（协作中断）；</li>
 *   <li>{@code TIMEOUT}：本次执行超过作业配置的最长执行时间，按失败处理；</li>
 *   <li>{@code INTERRUPTED}：进程重启时发现上次执行未留下结果，按被中断收尾。</li>
 * </ul>
 */
public enum AttemptResult {
    STARTED,
    SUCCESS,
    FAILED,
    CANCELLED,
    TIMEOUT,
    INTERRUPTED
}
