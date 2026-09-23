package com.github.highcumontoa.workflowschedulerjava.exception;

import org.springframework.http.HttpStatus;

/**
 * 业务错误码：每种拒绝/失败原因都可通过 {@link #code()} 区分。
 */
public enum ErrorCode {

    /** 请求体不合法（空批次、字段非法等）。 */
    INVALID_REQUEST(HttpStatus.BAD_REQUEST),

    /** 引用了不存在的上游作业。 */
    UPSTREAM_NOT_FOUND(HttpStatus.BAD_REQUEST),

    /** 依赖关系中存在环。 */
    CYCLIC_DEPENDENCY(HttpStatus.BAD_REQUEST),

    /** 批次内存在重复的作业 id。 */
    DUPLICATE_JOB_ID(HttpStatus.BAD_REQUEST),

    /** 作业 id 已存在（与已持久化作业冲突）。 */
    JOB_ALREADY_EXISTS(HttpStatus.CONFLICT),

    /** 相同幂等键但请求内容不一致。 */
    IDEMPOTENCY_KEY_CONFLICT(HttpStatus.CONFLICT),

    /** 作业不存在（查询/取消未知 id）。 */
    JOB_NOT_FOUND(HttpStatus.NOT_FOUND),

    /** 当前状态不允许取消（已成功等）。 */
    CANCEL_NOT_ALLOWED(HttpStatus.CONFLICT),

    /** 服务端内部错误。 */
    INTERNAL_ERROR(HttpStatus.INTERNAL_SERVER_ERROR);

    private final HttpStatus httpStatus;

    ErrorCode(HttpStatus httpStatus) {
        this.httpStatus = httpStatus;
    }

    public String code() {
        return name();
    }

    public HttpStatus httpStatus() {
        return httpStatus;
    }
}
