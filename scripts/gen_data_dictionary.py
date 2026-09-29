"""Generates docs/data_dictionary.md straight from the live database (so it can never drift).
Usage: python scripts/gen_data_dictionary.py"""
import os
from sqlalchemy import create_engine, text

url = "postgresql+psycopg2://{u}:{p}@{h}:{port}/{d}".format(
    u=os.environ.get("PGUSER", "mediapulse"), p=os.environ.get("PGPASSWORD", "mediapulse_dev_pw"),
    h=os.environ.get("PGHOST", "localhost"), port=os.environ.get("PGPORT", "5432"),
    d=os.environ.get("PGDATABASE", "mediapulse"))
engine = create_engine(url)

TABLES = {
 "public.dim_date": "Calendar dimension - one row per date that appears in the data (built by the loader; extended automatically by the Kafka consumer).",
 "public.dim_user": "Subscriber dimension (one row per user). region/device kept here as the user's profile values.",
 "public.dim_content": "Content catalogue dimension (titles).",
 "public.dim_device": "Device dimension (Mobile, TV, Web ...).",
 "public.dim_geography": "Geography dimension (region).",
 "public.dim_campaign": "Ad campaign dimension.",
 "public.fact_view": "One row per viewing event (play/pause/complete...). Grain: event.",
 "public.fact_ad": "One row per ad opportunity/event (impression, click). Grain: ad event.",
 "public.fact_subscription": "One row per subscription record with plan and status.",
 "public.fact_support": "One row per support ticket (issue type, date) - shown in Subscriber 360.",
 "public.fact_engagement": "Reserved daily per-user aggregate table (the live equivalent is dbt model mart_engagement_daily).",
 "public.churn_risk_scores": "Model output: churn probability and risk tier per user.",
 "public.engagement_anomalies": "Detected days with abnormally low total watch time (rolling z-score).",
 "public.ad_anomalies": "Detected abnormal CTR / ad-fill values per campaign per day.",
 "public.content_trend_anomalies": "Detected view-count surges per title.",
 "public.watch_time_forecast": "7-day watch-time forecast with a simple confidence band.",
 "public.alerts": "Decision-engine output: every automated alert with severity, owner, status, trigger reason and JSON audit history.",
 "public.pipeline_run_log": "Operational run log written by Airflow callbacks (success / retry / failed per task).",
 "public_marts.mart_watch_time": "KPI mart: watch time per day x genre x device with day-over-day change.",
 "public_marts.mart_completion_rate": "KPI mart: average completion rate per day x genre (watch seconds / duration, capped at 1).",
 "public_marts.mart_retention": "KPI mart: daily active users and day-over-day retention.",
 "public_marts.mart_subscriber_growth": "KPI mart: new / active / cancelled subscriptions per day and cumulative total.",
 "public_marts.mart_ad_performance": "KPI mart: ad fill rate, CTR and estimated revenue (assumed $2 CPM) per day x campaign.",
 "public_marts.mart_content_performance": "KPI mart: per-title views, watch time, viewers and completions.",
 "public_marts.mart_engagement_daily": "Per-user daily engagement aggregate (watch seconds, sessions, completion rate).",
}
COLS = {
 "date_key": "Surrogate key in YYYYMMDD form (e.g. 20260315).", "full_date": "Calendar date.", "year": "Calendar year.",
 "quarter": "Quarter 1-4.", "month": "Month number 1-12.", "month_name": "Month name.", "day": "Day of month.",
 "day_of_week": "0 = Monday ... 6 = Sunday.", "day_name": "Weekday name.", "is_weekend": "True for Saturday/Sunday.",
 "user_key": "Surrogate key (warehouse-generated integer).", "user_id": "Business key from the source system (e.g. U00010).",
 "subscription_type": "Account type from the users source (Free/Basic/Premium/Family/Unknown).",
 "region": "Region name (North/South/East/West/Central/Unknown).", "device": "Device name (Mobile/TV/Web/...).",
 "loaded_at": "Audit: when the row was loaded into the warehouse.", "source_file": "Audit: which source file/stream produced the row.",
 "content_key": "Surrogate key.", "content_id": "Business key from the catalogue (e.g. C00001).", "genre": "Content genre.",
 "language": "Content language.", "release_date": "Original release date.", "duration_min": "Runtime in minutes (missing values filled with the median during cleaning).",
 "device_key": "Surrogate key.", "device_name": "Device name.", "geography_key": "Surrogate key.", "campaign_key": "Surrogate key.",
 "campaign_id": "Business key of the ad campaign (UNKNOWN when missing in source).",
 "view_key": "Surrogate key.", "event_id": "Business key of the viewing event (UNIQUE - duplicate protection).",
 "event_timestamp": "When the event happened.", "event_type": "play / pause / skip / complete / ... (lower-cased) or 'unknown'.",
 "watch_seconds": "Seconds watched in the event. NULL = unknown (never imputed, to avoid biasing KPIs). Valid range 0-21600.",
 "ad_key": "Surrogate key.", "ad_event_id": "Business key of the ad event (UNIQUE).", "impression": "1 = ad shown, 0 = not filled, NULL = unknown.",
 "click": "1 = clicked, 0 = no click. Business rule: click=1 implies impression=1.",
 "subscription_key": "Surrogate key.", "subscription_id": "Business key of the subscription.", "start_date_key": "FK to dim_date (subscription start).",
 "plan": "Subscription plan (Basic/Standard/Premium/Family/Unknown; PREM and Gold normalised to Premium).",
 "status": "Subscription status (active/cancelled/expired/paused/unknown) or alert status (open/acknowledged/resolved).",
 "engagement_key": "Surrogate key.", "total_watch_seconds": "Sum of watch seconds.", "session_count": "Number of viewing events.",
 "completion_rate": "Average of min(watch_seconds / duration, 1) - range 0-1.", "computed_at": "When the value was computed.",
 "id": "Surrogate key.", "churn_probability": "Model probability that the user churns (0-1).", "risk_tier": "low (<0.33) / medium / high (>=0.66).",
 "features_used": "JSON of the feature values used for the score (explainability).", "model_version": "Model identifier/version.",
 "detected_date": "Date the anomaly refers to.", "metric": "Which metric was anomalous.", "actual_value": "Observed value.",
 "expected_value": "Expected value (mean of the previous 7 days).", "z_score": "Distance from expected in standard deviations.",
 "severity": "info / warning / critical (|z|>2 warning, >3 critical).", "detected_at": "When the anomaly was detected.",
 "view_count": "Number of views.", "expected_views": "Expected views (rolling mean).", "trend_type": "surge or drop.",
 "forecast_date": "Date being forecast.", "predicted_watch_seconds": "Forecast total watch seconds.", "lower_bound": "Lower band (prediction - 1 std of residuals).",
 "upper_bound": "Upper band (prediction + 1 std of residuals).", "generated_at": "When the forecast was generated.",
 "alert_id": "Surrogate key.", "alert_type": "content_alert / monetization_alert / retention_campaign / editorial_notification.",
 "owner": "Team or user responsible (set to the acknowledging user on acknowledge).", "trigger_reason": "Human-readable reason the rule fired.",
 "related_entity": "The user / content / campaign / date the alert is about.", "notified": "True if a notification was sent.",
 "notify_channel": "Channel used (slack) if notified.", "audit_log": "JSON array of every state change (created, notified, acknowledged...).",
 "triggered_at": "When the alert was raised.", "resolved_at": "When the alert was resolved.",
 "dag_id": "Airflow DAG id.", "task_id": "Airflow task id.", "run_id": "Airflow run id.", "message": "Error / info message.", "logged_at": "When the row was logged.",
 "genre_": "", "view_count_": "", "total_watch_seconds_": "",
 "avg_watch_seconds": "Average watch seconds per view.", "change_vs_prev_day": "Value minus previous day's value.",
 "avg_completion_rate": "Average completion rate.", "sample_size": "Rows behind the average.",
 "active_users": "Distinct users active that day.", "retained_from_prev_day": "Users active both yesterday and today.",
 "day_over_day_retention_rate": "retained / yesterday's active users.",
 "new_subscriptions": "New subscription records that day.", "new_active_subscriptions": "...of which status = active.",
 "new_cancelled_subscriptions": "...of which status = cancelled.", "cumulative_subscriptions": "Running total.",
 "ad_opportunities": "Number of ad events.", "impressions": "Sum of impression = 1.", "clicks": "Sum of click = 1.",
 "ad_fill_rate": "impressions / ad_opportunities.", "ctr": "clicks / impressions (NULL if no impressions).",
 "estimated_revenue_usd": "impressions / 1000 x assumed CPM (assumption - no revenue in source data).", "assumed_cpm_usd": "The CPM assumption used ($2.00).",
 "total_views": "Number of views.", "unique_viewers": "Distinct viewers.", "completions": "Views with event_type = complete.",
 "support_key": "Surrogate key.", "ticket_id": "Business key of the support ticket (UNIQUE).", "issue_type": "Ticket category (e.g. Billing, Playback).", "created_at": "When the ticket was opened.",
 "ingested_at": "Audit: ingestion timestamp (Silver CSV column).",
}

out = ["# Data Dictionary", "", "_Generated from the live database by `scripts/gen_data_dictionary.py`._", "",
       "Conventions: `*_key` = surrogate integer key (primary key), `*_id` = business key from the source system, "
       "`loaded_at` / `source_file` = audit columns. Referential integrity is enforced with foreign keys.", ""]
missing = []
with engine.connect() as c:
    for full, desc in TABLES.items():
        schema, table = full.split(".")
        cols = c.execute(text("""SELECT c.column_name, c.data_type, c.is_nullable, c.character_maximum_length,
                (SELECT string_agg(tc.constraint_type, ',') FROM information_schema.table_constraints tc
                   JOIN information_schema.key_column_usage k ON k.constraint_name = tc.constraint_name AND k.table_schema = tc.table_schema
                  WHERE k.table_name = c.table_name AND k.column_name = c.column_name AND k.table_schema = c.table_schema) AS cons
                FROM information_schema.columns c WHERE c.table_schema = :s AND c.table_name = :t ORDER BY c.ordinal_position"""),
                {"s": schema, "t": table}).all()
        if not cols:
            continue
        out += [f"## `{full}`", "", desc, "", "| Column | Type | Null? | Key | Description |", "|---|---|---|---|---|"]
        for name, dtype, nullable, maxlen, cons in cols:
            t = dtype + (f"({maxlen})" if maxlen else "")
            key = ("PK " if cons and "PRIMARY" in cons else "") + ("FK " if cons and "FOREIGN" in cons else "") + ("UK" if cons and "UNIQUE" in cons else "")
            d = COLS.get(name, "")
            if not d:
                missing.append(f"{full}.{name}")
            out.append(f"| `{name}` | {t} | {'yes' if nullable == 'YES' else 'no'} | {key.strip()} | {d} |")
        out.append("")
open("docs/data_dictionary.md", "w", encoding="utf-8").write("\n".join(out))
print("wrote docs/data_dictionary.md;", len(TABLES), "tables; undocumented columns:", missing or "none")
