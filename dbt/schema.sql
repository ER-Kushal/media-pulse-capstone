-- =====================================================================
-- MediaPulse Warehouse - Star Schema
-- =====================================================================
-- Dimensions: dim_user, dim_content, dim_device, dim_geography,
--             dim_campaign, dim_date
-- Facts:      fact_view, fact_ad, fact_subscription, fact_engagement
--
-- Design notes:
--  - Every table has a surrogate integer key (xxx_key) as PRIMARY KEY,
--    plus the original business key (e.g. user_id) kept UNIQUE.
--    Surrogate keys are standard warehouse practice: they never change
--    even if a source system's ID format changes.
--  - Every table has audit columns (loaded_at, source_file) so you can
--    always trace a row back to when/where it came from.
--  - Foreign keys enforce referential integrity: a fact table row
--    cannot reference a user/content/etc. that doesn't exist.
-- =====================================================================

DROP TABLE IF EXISTS fact_engagement CASCADE;
DROP TABLE IF EXISTS fact_subscription CASCADE;
DROP TABLE IF EXISTS fact_ad CASCADE;
DROP TABLE IF EXISTS fact_view CASCADE;
DROP TABLE IF EXISTS dim_campaign CASCADE;
DROP TABLE IF EXISTS dim_geography CASCADE;
DROP TABLE IF EXISTS dim_device CASCADE;
DROP TABLE IF EXISTS dim_content CASCADE;
DROP TABLE IF EXISTS dim_user CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

-- =====================================================================
-- DIMENSIONS
-- =====================================================================

CREATE TABLE dim_date (
    date_key        INTEGER PRIMARY KEY,        -- YYYYMMDD, e.g. 20260315
    full_date       DATE NOT NULL UNIQUE,
    year            SMALLINT NOT NULL,
    quarter         SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    month_name      VARCHAR(10) NOT NULL,
    day             SMALLINT NOT NULL,
    day_of_week     SMALLINT NOT NULL,          -- 0=Monday ... 6=Sunday
    day_name        VARCHAR(10) NOT NULL,
    is_weekend      BOOLEAN NOT NULL
);

-- Note: region/device are also kept here (denormalized) as the user's
-- current profile values, since the raw data only records device/region
-- per USER, not per individual event. fact tables additionally link to
-- dim_device/dim_geography directly (the user's device/region at load
-- time) so those dimensions are usable independently, e.g. "views by
-- device" without joining through dim_user.
CREATE TABLE dim_user (
    user_key            SERIAL PRIMARY KEY,
    user_id             VARCHAR(50) NOT NULL UNIQUE,   -- business key
    subscription_type   VARCHAR(30) NOT NULL DEFAULT 'Unknown',
    region              VARCHAR(30) NOT NULL DEFAULT 'Unknown',
    device              VARCHAR(30) NOT NULL DEFAULT 'Unknown',
    loaded_at           TIMESTAMP NOT NULL DEFAULT now(),
    source_file         VARCHAR(100)
);

CREATE TABLE dim_content (
    content_key     SERIAL PRIMARY KEY,
    content_id      VARCHAR(50) NOT NULL UNIQUE,
    genre           VARCHAR(50) NOT NULL DEFAULT 'Unknown',
    language        VARCHAR(30) NOT NULL DEFAULT 'Unknown',
    release_date    DATE,
    duration_min    NUMERIC(8,2),
    loaded_at       TIMESTAMP NOT NULL DEFAULT now(),
    source_file     VARCHAR(100)
);

CREATE TABLE dim_device (
    device_key      SERIAL PRIMARY KEY,
    device_name     VARCHAR(30) NOT NULL UNIQUE
);

CREATE TABLE dim_geography (
    geography_key   SERIAL PRIMARY KEY,
    region          VARCHAR(30) NOT NULL UNIQUE
);

CREATE TABLE dim_campaign (
    campaign_key    SERIAL PRIMARY KEY,
    campaign_id     VARCHAR(50) NOT NULL UNIQUE
);

-- =====================================================================
-- FACTS
-- =====================================================================

-- One row per view/play event
CREATE TABLE fact_view (
    view_key        BIGSERIAL PRIMARY KEY,
    event_id        VARCHAR(50) NOT NULL UNIQUE,       -- business key
    user_key        INTEGER NOT NULL REFERENCES dim_user(user_key),
    content_key     INTEGER NOT NULL REFERENCES dim_content(content_key),
    device_key      INTEGER NOT NULL REFERENCES dim_device(device_key),
    geography_key   INTEGER NOT NULL REFERENCES dim_geography(geography_key),
    date_key        INTEGER NOT NULL REFERENCES dim_date(date_key),
    event_timestamp TIMESTAMP NOT NULL,
    event_type      VARCHAR(20) NOT NULL,
    watch_seconds   NUMERIC(10,2),                     -- null = unknown, never imputed
    loaded_at       TIMESTAMP NOT NULL DEFAULT now(),
    source_file     VARCHAR(100)
);
CREATE INDEX idx_fact_view_date ON fact_view(date_key);
CREATE INDEX idx_fact_view_user ON fact_view(user_key);
CREATE INDEX idx_fact_view_content ON fact_view(content_key);
CREATE INDEX idx_fact_view_device ON fact_view(device_key);
CREATE INDEX idx_fact_view_geography ON fact_view(geography_key);

-- One row per ad event (impression/click)
CREATE TABLE fact_ad (
    ad_key          BIGSERIAL PRIMARY KEY,
    ad_event_id     VARCHAR(50) NOT NULL UNIQUE,
    user_key        INTEGER NOT NULL REFERENCES dim_user(user_key),
    content_key     INTEGER NOT NULL REFERENCES dim_content(content_key),
    campaign_key    INTEGER NOT NULL REFERENCES dim_campaign(campaign_key),
    geography_key   INTEGER NOT NULL REFERENCES dim_geography(geography_key),
    date_key        INTEGER NOT NULL REFERENCES dim_date(date_key),
    event_timestamp TIMESTAMP NOT NULL,
    impression      SMALLINT,       -- 0/1, null = unknown
    click           SMALLINT,       -- 0/1, null = unknown
    loaded_at       TIMESTAMP NOT NULL DEFAULT now(),
    source_file     VARCHAR(100)
);
CREATE INDEX idx_fact_ad_date ON fact_ad(date_key);
CREATE INDEX idx_fact_ad_campaign ON fact_ad(campaign_key);
CREATE INDEX idx_fact_ad_geography ON fact_ad(geography_key);

-- One row per subscription record (slowly-changing: status can change over time)
CREATE TABLE fact_subscription (
    subscription_key    BIGSERIAL PRIMARY KEY,
    subscription_id     VARCHAR(50) NOT NULL UNIQUE,
    user_key             INTEGER NOT NULL REFERENCES dim_user(user_key),
    start_date_key       INTEGER REFERENCES dim_date(date_key),
    plan                 VARCHAR(30) NOT NULL DEFAULT 'Unknown',
    status               VARCHAR(20) NOT NULL DEFAULT 'unknown',
    loaded_at            TIMESTAMP NOT NULL DEFAULT now(),
    source_file          VARCHAR(100)
);
CREATE INDEX idx_fact_subscription_user ON fact_subscription(user_key);

-- Aggregated daily engagement per user (built later by dbt from fact_view;
-- table defined now so downstream models/API have a stable target)
CREATE TABLE fact_engagement (
    engagement_key      BIGSERIAL PRIMARY KEY,
    user_key            INTEGER NOT NULL REFERENCES dim_user(user_key),
    date_key            INTEGER NOT NULL REFERENCES dim_date(date_key),
    total_watch_seconds NUMERIC(12,2) DEFAULT 0,
    session_count       INTEGER DEFAULT 0,
    completion_rate     NUMERIC(5,4),               -- 0.0 - 1.0
    computed_at         TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (user_key, date_key)
);
CREATE INDEX idx_fact_engagement_date ON fact_engagement(date_key);
