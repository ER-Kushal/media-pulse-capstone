-- =====================================================================
-- MediaPulse - ML output tables & automation/alerts tables
-- Run this AFTER dbt has built the marts (mart_engagement_daily,
-- mart_watch_time, mart_ad_performance, mart_content_performance).
-- =====================================================================

DROP TABLE IF EXISTS alerts CASCADE;
DROP TABLE IF EXISTS watch_time_forecast CASCADE;
DROP TABLE IF EXISTS content_trend_anomalies CASCADE;
DROP TABLE IF EXISTS ad_anomalies CASCADE;
DROP TABLE IF EXISTS engagement_anomalies CASCADE;
DROP TABLE IF EXISTS churn_risk_scores CASCADE;

CREATE TABLE churn_risk_scores (
    id                  BIGSERIAL PRIMARY KEY,
    user_key            INTEGER NOT NULL,
    churn_probability   NUMERIC(6,5) NOT NULL,
    risk_tier           VARCHAR(10) NOT NULL,   -- 'low' | 'medium' | 'high'
    features_used       JSONB,
    model_version       VARCHAR(50) NOT NULL,
    computed_at         TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX idx_churn_user ON churn_risk_scores(user_key);
CREATE INDEX idx_churn_tier ON churn_risk_scores(risk_tier);

CREATE TABLE engagement_anomalies (
    id              BIGSERIAL PRIMARY KEY,
    detected_date   DATE NOT NULL,
    metric          VARCHAR(50) NOT NULL,      -- e.g. 'total_watch_seconds'
    actual_value    NUMERIC(14,2),
    expected_value  NUMERIC(14,2),
    z_score         NUMERIC(8,4),
    severity        VARCHAR(10) NOT NULL,      -- 'warning' | 'critical'
    detected_at     TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE ad_anomalies (
    id              BIGSERIAL PRIMARY KEY,
    detected_date   DATE NOT NULL,
    campaign_id     VARCHAR(50),
    metric          VARCHAR(50) NOT NULL,      -- 'ctr' | 'ad_fill_rate'
    actual_value    NUMERIC(8,4),
    expected_value  NUMERIC(8,4),
    z_score         NUMERIC(8,4),
    severity        VARCHAR(10) NOT NULL,
    detected_at     TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE content_trend_anomalies (
    id              BIGSERIAL PRIMARY KEY,
    detected_date   DATE NOT NULL,
    content_id      VARCHAR(50) NOT NULL,
    view_count      INTEGER,
    expected_views  NUMERIC(10,2),
    z_score         NUMERIC(8,4),
    trend_type      VARCHAR(10) NOT NULL,      -- 'surge' | 'drop'
    detected_at     TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE watch_time_forecast (
    id                      BIGSERIAL PRIMARY KEY,
    forecast_date           DATE NOT NULL,
    predicted_watch_seconds NUMERIC(14,2) NOT NULL,
    lower_bound             NUMERIC(14,2),
    upper_bound             NUMERIC(14,2),
    model_version           VARCHAR(50) NOT NULL,
    generated_at            TIMESTAMP NOT NULL DEFAULT now()
);

-- The decision engine's output. Every automated action lands here first,
-- satisfying the "timestamp, severity, owner/status, trigger reason, and
-- audit history" requirement in section 10.
CREATE TABLE alerts (
    alert_id        BIGSERIAL PRIMARY KEY,
    alert_type      VARCHAR(30) NOT NULL,   -- content_alert | monetization_alert
                                             -- | retention_campaign | editorial_notification
    severity        VARCHAR(10) NOT NULL,   -- info | warning | critical
    status          VARCHAR(20) NOT NULL DEFAULT 'open',  -- open | acknowledged | resolved
    owner           VARCHAR(50) NOT NULL DEFAULT 'unassigned',
    trigger_reason  TEXT NOT NULL,
    related_entity  VARCHAR(100),           -- e.g. a user_id, content_id, or campaign_id
    notified        BOOLEAN NOT NULL DEFAULT false,
    notify_channel  VARCHAR(20),            -- 'slack' | 'email' | null
    audit_log       JSONB NOT NULL DEFAULT '[]',
    triggered_at    TIMESTAMP NOT NULL DEFAULT now(),
    resolved_at     TIMESTAMP
);
CREATE INDEX idx_alerts_status ON alerts(status);
CREATE INDEX idx_alerts_type ON alerts(alert_type);
