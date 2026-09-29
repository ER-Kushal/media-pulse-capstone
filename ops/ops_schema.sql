-- Operational run log (section 13): written by Airflow task callbacks,
-- read by GET /api/data-quality. Safe to re-run (IF NOT EXISTS).
CREATE TABLE IF NOT EXISTS pipeline_run_log (
    id          BIGSERIAL PRIMARY KEY,
    dag_id      VARCHAR(100) NOT NULL,
    task_id     VARCHAR(100) NOT NULL,
    run_id      VARCHAR(200),
    status      VARCHAR(20) NOT NULL,      -- success | failed | retry
    message     TEXT,
    logged_at   TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_run_log_time ON pipeline_run_log(logged_at DESC);
