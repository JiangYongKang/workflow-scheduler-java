package com.github.highcumontoa.workflowschedulerjava.web;

import com.github.highcumontoa.workflowschedulerjava.error.ApiError;
import com.github.highcumontoa.workflowschedulerjava.error.ErrorCode;
import com.github.highcumontoa.workflowschedulerjava.error.SchedulerException;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;

/** 业务异常 → 稳定 HTTP 状态码 + 可区分错误码。 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(SchedulerException.class)
    public ResponseEntity<ApiError> handle(SchedulerException ex, HttpServletRequest req) {
        HttpStatus status = mapStatus(ex.getCode());
        if (status.is5xxServerError()) {
            log.error("请求失败 code={} path={} message={}", ex.getCode(),
                    req.getRequestURI(), ex.getMessage());
        } else {
            log.warn("请求被拒绝 code={} path={} message={}", ex.getCode(),
                    req.getRequestURI(), ex.getMessage());
        }
        return ResponseEntity.status(status)
                .body(new ApiError(ex.getCode().name(), ex.getMessage(),
                        req.getRequestURI(), Instant.now()));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiError> handleBadBody(Exception ex, HttpServletRequest req) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(new ApiError(ErrorCode.INVALID_REQUEST.name(),
                        "请求体无法解析: " + ex.getMessage(), req.getRequestURI(), Instant.now()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleOther(Exception ex, HttpServletRequest req) {
        log.error("未预期异常 path={}", req.getRequestURI(), ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ApiError(ErrorCode.INTERNAL_ERROR.name(),
                        ex.getClass().getSimpleName() + ": " + ex.getMessage(),
                        req.getRequestURI(), Instant.now()));
    }

    private HttpStatus mapStatus(ErrorCode code) {
        return switch (code) {
            case UNKNOWN_DEPENDENCY, CYCLE_DETECTED, UNKNOWN_JOB_TYPE, INVALID_REQUEST ->
                    HttpStatus.BAD_REQUEST;
            case JOB_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case ALREADY_TERMINAL -> HttpStatus.CONFLICT;
            case INTERNAL_ERROR -> HttpStatus.INTERNAL_SERVER_ERROR;
        };
    }
}
