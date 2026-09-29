# Data Dictionary

_Generated from the live database by `scripts/gen_data_dictionary.py`._

Conventions: `*_key` = surrogate integer key (primary key), `*_id` = business key from the source system, `loaded_at` / `source_file` = audit columns. Referential integrity is enforced with foreign keys.

## `public.dim_date`

Calendar dimension - one row per date that appears in the data (built by the loader; extended automatically by the Kafka consumer).

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `date_key` | integer | no | PK | Surrogate key in YYYYMMDD form (e.g. 20260315). |
| `full_date` | date | no | UK | Calendar date. |
| `year` | smallint | no |  | Calendar year. |
| `quarter` | smallint | no |  | Quarter 1-4. |
| `month` | smallint | no |  | Month number 1-12. |
| `month_name` | character varying(10) | no |  | Month name. |
| `day` | smallint | no |  | Day of month. |
| `day_of_week` | smallint | no |  | 0 = Monday ... 6 = Sunday. |
| `day_name` | character varying(10) | no |  | Weekday name. |
| `is_weekend` | boolean | no |  | True for Saturday/Sunday. |

## `public.dim_user`

Subscriber dimension (one row per user). region/device kept here as the user's profile values.

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `user_key` | integer | no | PK | Surrogate key (warehouse-generated integer). |
| `user_id` | character varying(50) | no | UK | Business key from the source system (e.g. U00010). |
| `subscription_type` | character varying(30) | no |  | Account type from the users source (Free/Basic/Premium/Family/Unknown). |
| `region` | character varying(30) | no |  | Region name (North/South/East/West/Central/Unknown). |
| `device` | character varying(30) | no |  | Device name (Mobile/TV/Web/...). |
| `loaded_at` | timestamp without time zone | no |  | Audit: when the row was loaded into the warehouse. |
| `source_file` | character varying(100) | yes |  | Audit: which source file/stream produced the row. |

## `public.dim_content`

Content catalogue dimension (titles).

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `content_key` | integer | no | PK | Surrogate key. |
| `content_id` | character varying(50) | no | UK | Business key from the catalogue (e.g. C00001). |
| `genre` | character varying(50) | no |  | Content genre. |
| `language` | character varying(30) | no |  | Content language. |
| `release_date` | date | yes |  | Original release date. |
| `duration_min` | numeric | yes |  | Runtime in minutes (missing values filled with the median during cleaning). |
| `loaded_at` | timestamp without time zone | no |  | Audit: when the row was loaded into the warehouse. |
| `source_file` | character varying(100) | yes |  | Audit: which source file/stream produced the row. |

## `public.dim_device`

Device dimension (Mobile, TV, Web ...).

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `device_key` | integer | no | PK | Surrogate key. |
| `device_name` | character varying(30) | no | UK | Device name. |

## `public.dim_geography`

Geography dimension (region).

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `geography_key` | integer | no | PK | Surrogate key. |
| `region` | character varying(30) | no | UK | Region name (North/South/East/West/Central/Unknown). |

## `public.dim_campaign`

Ad campaign dimension.

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `campaign_key` | integer | no | PK | Surrogate key. |
| `campaign_id` | character varying(50) | no | UK | Business key of the ad campaign (UNKNOWN when missing in source). |

## `public.fact_view`

One row per viewing event (play/pause/complete...). Grain: event.

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `view_key` | bigint | no | PK | Surrogate key. |
| `event_id` | character varying(50) | no | UK | Business key of the viewing event (UNIQUE - duplicate protection). |
| `user_key` | integer | no | FK | Surrogate key (warehouse-generated integer). |
| `content_key` | integer | no | FK | Surrogate key. |
| `device_key` | integer | no | FK | Surrogate key. |
| `geography_key` | integer | no | FK | Surrogate key. |
| `date_key` | integer | no | FK | Surrogate key in YYYYMMDD form (e.g. 20260315). |
| `event_timestamp` | timestamp without time zone | no |  | When the event happened. |
| `event_type` | character varying(20) | no |  | play / pause / skip / complete / ... (lower-cased) or 'unknown'. |
| `watch_seconds` | numeric | yes |  | Seconds watched in the event. NULL = unknown (never imputed, to avoid biasing KPIs). Valid range 0-21600. |
| `loaded_at` | timestamp without time zone | no |  | Audit: when the row was loaded into the warehouse. |
| `source_file` | character varying(100) | yes |  | Audit: which source file/stream produced the row. |

## `public.fact_ad`

One row per ad opportunity/event (impression, click). Grain: ad event.

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `ad_key` | bigint | no | PK | Surrogate key. |
| `ad_event_id` | character varying(50) | no | UK | Business key of the ad event (UNIQUE). |
| `user_key` | integer | no | FK | Surrogate key (warehouse-generated integer). |
| `content_key` | integer | no | FK | Surrogate key. |
| `campaign_key` | integer | no | FK | Surrogate key. |
| `geography_key` | integer | no | FK | Surrogate key. |
| `date_key` | integer | no | FK | Surrogate key in YYYYMMDD form (e.g. 20260315). |
| `event_timestamp` | timestamp without time zone | no |  | When the event happened. |
| `impression` | smallint | yes |  | 1 = ad shown, 0 = not filled, NULL = unknown. |
| `click` | smallint | yes |  | 1 = clicked, 0 = no click. Business rule: click=1 implies impression=1. |
| `loaded_at` | timestamp without time zone | no |  | Audit: when the row was loaded into the warehouse. |
| `source_file` | character varying(100) | yes |  | Audit: which source file/stream produced the row. |

## `public.fact_subscription`

One row per subscription record with plan and status.

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `subscription_key` | bigint | no | PK | Surrogate key. |
| `subscription_id` | character varying(50) | no | UK | Business key of the subscription. |
| `user_key` | integer | no | FK | Surrogate key (warehouse-generated integer). |
| `start_date_key` | integer | yes | FK | FK to dim_date (subscription start). |
| `plan` | character varying(30) | no |  | Subscription plan (Basic/Standard/Premium/Family/Unknown; PREM and Gold normalised to Premium). |
| `status` | character varying(20) | no |  | Subscription status (active/cancelled/expired/paused/unknown) or alert status (open/acknowledged/resolved). |
| `loaded_at` | timestamp without time zone | no |  | Audit: when the row was loaded into the warehouse. |
| `source_file` | character varying(100) | yes |  | Audit: which source file/stream produced the row. |

## `public.fact_engagement`

Reserved daily per-user aggregate table (the live equivalent is dbt model mart_engagement_daily).

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `engagement_key` | bigint | no | PK | Surrogate key. |
| `user_key` | integer | no | FK UK | Surrogate key (warehouse-generated integer). |
| `date_key` | integer | no | FK UK | Surrogate key in YYYYMMDD form (e.g. 20260315). |
| `total_watch_seconds` | numeric | yes |  | Sum of watch seconds. |
| `session_count` | integer | yes |  | Number of viewing events. |
| `completion_rate` | numeric | yes |  | Average of min(watch_seconds / duration, 1) - range 0-1. |
| `computed_at` | timestamp without time zone | no |  | When the value was computed. |

## `public.churn_risk_scores`

Model output: churn probability and risk tier per user.

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `id` | bigint | no | PK | Surrogate key. |
| `user_key` | integer | no |  | Surrogate key (warehouse-generated integer). |
| `churn_probability` | numeric | no |  | Model probability that the user churns (0-1). |
| `risk_tier` | character varying(10) | no |  | low (<0.33) / medium / high (>=0.66). |
| `features_used` | jsonb | yes |  | JSON of the feature values used for the score (explainability). |
| `model_version` | character varying(50) | no |  | Model identifier/version. |
| `computed_at` | timestamp without time zone | no |  | When the value was computed. |

## `public.engagement_anomalies`

Detected days with abnormally low total watch time (rolling z-score).

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `id` | bigint | no | PK | Surrogate key. |
| `detected_date` | date | no |  | Date the anomaly refers to. |
| `metric` | character varying(50) | no |  | Which metric was anomalous. |
| `actual_value` | numeric | yes |  | Observed value. |
| `expected_value` | numeric | yes |  | Expected value (mean of the previous 7 days). |
| `z_score` | numeric | yes |  | Distance from expected in standard deviations. |
| `severity` | character varying(10) | no |  | info / warning / critical (|z|>2 warning, >3 critical). |
| `detected_at` | timestamp without time zone | no |  | When the anomaly was detected. |

## `public.ad_anomalies`

Detected abnormal CTR / ad-fill values per campaign per day.

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `id` | bigint | no | PK | Surrogate key. |
| `detected_date` | date | no |  | Date the anomaly refers to. |
| `campaign_id` | character varying(50) | yes |  | Business key of the ad campaign (UNKNOWN when missing in source). |
| `metric` | character varying(50) | no |  | Which metric was anomalous. |
| `actual_value` | numeric | yes |  | Observed value. |
| `expected_value` | numeric | yes |  | Expected value (mean of the previous 7 days). |
| `z_score` | numeric | yes |  | Distance from expected in standard deviations. |
| `severity` | character varying(10) | no |  | info / warning / critical (|z|>2 warning, >3 critical). |
| `detected_at` | timestamp without time zone | no |  | When the anomaly was detected. |

## `public.content_trend_anomalies`

Detected view-count surges per title.

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `id` | bigint | no | PK | Surrogate key. |
| `detected_date` | date | no |  | Date the anomaly refers to. |
| `content_id` | character varying(50) | no |  | Business key from the catalogue (e.g. C00001). |
| `view_count` | integer | yes |  | Number of views. |
| `expected_views` | numeric | yes |  | Expected views (rolling mean). |
| `z_score` | numeric | yes |  | Distance from expected in standard deviations. |
| `trend_type` | character varying(10) | no |  | surge or drop. |
| `detected_at` | timestamp without time zone | no |  | When the anomaly was detected. |

## `public.watch_time_forecast`

7-day watch-time forecast with a simple confidence band.

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `id` | bigint | no | PK | Surrogate key. |
| `forecast_date` | date | no |  | Date being forecast. |
| `predicted_watch_seconds` | numeric | no |  | Forecast total watch seconds. |
| `lower_bound` | numeric | yes |  | Lower band (prediction - 1 std of residuals). |
| `upper_bound` | numeric | yes |  | Upper band (prediction + 1 std of residuals). |
| `model_version` | character varying(50) | no |  | Model identifier/version. |
| `generated_at` | timestamp without time zone | no |  | When the forecast was generated. |

## `public.alerts`

Decision-engine output: every automated alert with severity, owner, status, trigger reason and JSON audit history.

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `alert_id` | bigint | no | PK | Surrogate key. |
| `alert_type` | character varying(30) | no |  | content_alert / monetization_alert / retention_campaign / editorial_notification. |
| `severity` | character varying(10) | no |  | info / warning / critical (|z|>2 warning, >3 critical). |
| `status` | character varying(20) | no |  | Subscription status (active/cancelled/expired/paused/unknown) or alert status (open/acknowledged/resolved). |
| `owner` | character varying(50) | no |  | Team or user responsible (set to the acknowledging user on acknowledge). |
| `trigger_reason` | text | no |  | Human-readable reason the rule fired. |
| `related_entity` | character varying(100) | yes |  | The user / content / campaign / date the alert is about. |
| `notified` | boolean | no |  | True if a notification was sent. |
| `notify_channel` | character varying(20) | yes |  | Channel used (slack) if notified. |
| `audit_log` | jsonb | no |  | JSON array of every state change (created, notified, acknowledged...). |
| `triggered_at` | timestamp without time zone | no |  | When the alert was raised. |
| `resolved_at` | timestamp without time zone | yes |  | When the alert was resolved. |

## `public.pipeline_run_log`

Operational run log written by Airflow callbacks (success / retry / failed per task).

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `id` | bigint | no | PK | Surrogate key. |
| `dag_id` | character varying(100) | no |  | Airflow DAG id. |
| `task_id` | character varying(100) | no |  | Airflow task id. |
| `run_id` | character varying(200) | yes |  | Airflow run id. |
| `status` | character varying(20) | no |  | Subscription status (active/cancelled/expired/paused/unknown) or alert status (open/acknowledged/resolved). |
| `message` | text | yes |  | Error / info message. |
| `logged_at` | timestamp without time zone | no |  | When the row was logged. |

## `public_marts.mart_watch_time`

KPI mart: watch time per day x genre x device with day-over-day change.

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `full_date` | date | yes |  | Calendar date. |
| `genre` | character varying(50) | yes |  | Content genre. |
| `device_name` | character varying(30) | yes |  | Device name. |
| `total_watch_seconds` | numeric | yes |  | Sum of watch seconds. |
| `view_count` | bigint | yes |  | Number of views. |
| `avg_watch_seconds` | numeric | yes |  | Average watch seconds per view. |
| `change_vs_prev_day` | numeric | yes |  | Value minus previous day's value. |

## `public_marts.mart_completion_rate`

KPI mart: average completion rate per day x genre (watch seconds / duration, capped at 1).

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `full_date` | date | yes |  | Calendar date. |
| `genre` | character varying(50) | yes |  | Content genre. |
| `avg_completion_rate` | numeric | yes |  | Average completion rate. |
| `sample_size` | bigint | yes |  | Rows behind the average. |
| `change_vs_prev_day` | numeric | yes |  | Value minus previous day's value. |

## `public_marts.mart_retention`

KPI mart: daily active users and day-over-day retention.

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `full_date` | date | yes |  | Calendar date. |
| `active_users` | bigint | yes |  | Distinct users active that day. |
| `retained_from_prev_day` | bigint | yes |  | Users active both yesterday and today. |
| `day_over_day_retention_rate` | numeric | yes |  | retained / yesterday's active users. |

## `public_marts.mart_subscriber_growth`

KPI mart: new / active / cancelled subscriptions per day and cumulative total.

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `full_date` | date | yes |  | Calendar date. |
| `new_subscriptions` | bigint | yes |  | New subscription records that day. |
| `new_active_subscriptions` | bigint | yes |  | ...of which status = active. |
| `new_cancelled_subscriptions` | bigint | yes |  | ...of which status = cancelled. |
| `cumulative_subscriptions` | numeric | yes |  | Running total. |
| `change_vs_prev_day` | bigint | yes |  | Value minus previous day's value. |

## `public_marts.mart_ad_performance`

KPI mart: ad fill rate, CTR and estimated revenue (assumed $2 CPM) per day x campaign.

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `full_date` | date | yes |  | Calendar date. |
| `campaign_id` | character varying(50) | yes |  | Business key of the ad campaign (UNKNOWN when missing in source). |
| `ad_opportunities` | bigint | yes |  | Number of ad events. |
| `impressions` | bigint | yes |  | Sum of impression = 1. |
| `clicks` | bigint | yes |  | Sum of click = 1. |
| `ad_fill_rate` | numeric | yes |  | impressions / ad_opportunities. |
| `ctr` | numeric | yes |  | clicks / impressions (NULL if no impressions). |
| `estimated_revenue_usd` | numeric | yes |  | impressions / 1000 x assumed CPM (assumption - no revenue in source data). |
| `assumed_cpm_usd` | numeric | yes |  | The CPM assumption used ($2.00). |

## `public_marts.mart_content_performance`

KPI mart: per-title views, watch time, viewers and completions.

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `content_id` | character varying(50) | yes |  | Business key from the catalogue (e.g. C00001). |
| `genre` | character varying(50) | yes |  | Content genre. |
| `language` | character varying(30) | yes |  | Content language. |
| `total_views` | bigint | yes |  | Number of views. |
| `total_watch_seconds` | numeric | yes |  | Sum of watch seconds. |
| `avg_watch_seconds` | numeric | yes |  | Average watch seconds per view. |
| `unique_viewers` | bigint | yes |  | Distinct viewers. |
| `completions` | bigint | yes |  | Views with event_type = complete. |

## `public_marts.mart_engagement_daily`

Per-user daily engagement aggregate (watch seconds, sessions, completion rate).

| Column | Type | Null? | Key | Description |
|---|---|---|---|---|
| `user_key` | integer | yes |  | Surrogate key (warehouse-generated integer). |
| `date_key` | integer | yes |  | Surrogate key in YYYYMMDD form (e.g. 20260315). |
| `full_date` | date | yes |  | Calendar date. |
| `total_watch_seconds` | numeric | yes |  | Sum of watch seconds. |
| `session_count` | bigint | yes |  | Number of viewing events. |
| `completion_rate` | numeric | yes |  | Average of min(watch_seconds / duration, 1) - range 0-1. |
