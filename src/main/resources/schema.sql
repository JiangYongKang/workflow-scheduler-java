-- DAG 作业调度持久化结构（H2 方言，幂等创建）

CREATE TABLE IF NOT EXISTS jobs (
    id                  VARCHAR(64)  PRIMARY KEY,
    idempotency_key     VARCHAR(128),
    job_type            VARCHAR(64)  NOT NULL,
    payload             CLOB,
    queue_name          VARCHAR(64)  NOT NULL,
    tenant              VARCHAR(64)  NOT NULL,
    status              VARCHAR(32)  NOT NULL,
    attempts            INT          NOT NULL DEFAULT 0,
    max_attempts        INT          NOT NULL,
    backoff_base_ms     BIGINT       NOT NULL,
    backoff_multiplier  DOUBLE       NOT NULL,
    run_after           BIGINT       NOT NULL DEFAULT 0,
    failure_reason      CLOB,
    created_at          BIGINT       NOT NULL,
    updated_at          BIGINT       NOT NULL
);

-- 幂等键唯一约束：相同键重复提交只会命中同一行
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idem ON jobs (idempotency_key);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs (queue_name, status);
CREATE INDEX IF NOT EXISTS idx_jobs_tenant ON jobs (tenant, status);

-- 作业依赖边：job_id 依赖 upstream_id
CREATE TABLE IF NOT EXISTS job_deps (
    job_id       VARCHAR(64) NOT NULL,
    upstream_id  VARCHAR(64) NOT NULL,
    PRIMARY KEY (job_id, upstream_id)
);
CREATE INDEX IF NOT EXISTS idx_deps_up ON job_deps (upstream_id);

-- 每次实际执行尝试落一行，用于跨重启的“只执行一次”校验与审计
CREATE TABLE IF NOT EXISTS job_attempts (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_id      VARCHAR(64) NOT NULL,
    attempt_no  INT         NOT NULL,
    node        VARCHAR(64),
    started_at  BIGINT      NOT NULL,
    finished_at BIGINT,
    result      VARCHAR(16),
    message     CLOB
);
CREATE INDEX IF NOT EXISTS idx_attempts_job ON job_attempts (job_id);
