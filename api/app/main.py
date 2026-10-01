"""MediaPulse API - FastAPI backend (section 12 of the brief).

Run locally:   cd api && uvicorn app.main:app --reload --port 8000
Docs (OpenAPI): http://localhost:8000/docs
Login:          POST /api/auth/login  (demo users: admin / analyst / viewer)
"""
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, Optional

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Path as PathParam, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from .core import (USERS, authenticate, client_ip, create_token, current_user, engine,
                   limiter, log, login_limiter, require_role)

app = FastAPI(
    title="MediaPulse API",
    version="1.0.0",
    description="Audience, content, monetization, churn-risk and alert data for the "
                "MediaPulse streaming-analytics platform. Authenticate via /api/auth/login, "
                "then click 'Authorize' in this page.",
)

# ------------------------------------------------------------------ helpers
def rows(sql: str, **params) -> list[dict]:
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(text(sql), params).mappings().all()]


def one(sql: str, **params) -> Optional[dict]:
    r = rows(sql, **params)
    return r[0] if r else None


ID_PATTERN = r"^[A-Za-z0-9_-]{1,30}$"

# ------------------------------------------------------------------ middleware / errors
@app.middleware("http")
async def guard_and_log(request: Request, call_next):
    ip = client_ip(request)
    if request.url.path.startswith("/api") and not limiter.allow(ip):
        log.warning("rate limit hit from %s on %s", ip, request.url.path)
        return JSONResponse({"error": "rate_limited", "detail": "Too many requests"}, status_code=429)
    start = time.time()
    response = await call_next(request)
    if request.url.path.startswith("/api") or request.url.path == "/health":
        log.info("%s %s -> %s (%.0f ms) ip=%s", request.method, request.url.path,
                 response.status_code, (time.time() - start) * 1000, ip)
    return response


@app.exception_handler(SQLAlchemyError)
async def db_error_handler(request: Request, exc: SQLAlchemyError):
    log.error("database error on %s: %s", request.url.path, exc)
    return JSONResponse({"error": "database_error", "detail": "Data store unavailable or query failed"},
                        status_code=503)


@app.exception_handler(RequestValidationError)
async def validation_handler(request: Request, exc: RequestValidationError):
    return JSONResponse({"error": "validation_error", "detail": jsonable_encoder(exc.errors())},
                        status_code=422)


@app.exception_handler(Exception)
async def unhandled_handler(request: Request, exc: Exception):
    log.exception("unhandled error on %s", request.url.path)
    return JSONResponse({"error": "internal_error", "detail": "Unexpected server error"}, status_code=500)


# ------------------------------------------------------------------ public routes
@app.get("/health", tags=["ops"])
def health():
    """Liveness + DB connectivity (used by Docker/cloud health checks)."""
    try:
        rows("SELECT 1 AS ok")
        return {"status": "ok", "database": "up", "time": datetime.now(timezone.utc).isoformat()}
    except Exception:
        return JSONResponse({"status": "degraded", "database": "down"}, status_code=503)


auth_router = APIRouter(prefix="/api/auth", tags=["auth"])


@auth_router.post("/login")
def login(request: Request, form: OAuth2PasswordRequestForm = Depends()):
    if not login_limiter.allow(client_ip(request)):
        raise HTTPException(429, "Too many login attempts, wait a minute")
    user = authenticate(form.username, form.password)
    if not user:
        log.warning("failed login for '%s' from %s", form.username, client_ip(request))
        raise HTTPException(401, "Incorrect username or password")
    log.info("login ok: %s (%s)", user["username"], user["role"])
    return {"access_token": create_token(user["username"], user["role"]), "token_type": "bearer",
            "username": user["username"], "role": user["role"]}


@auth_router.get("/me")
def me(user: dict = Depends(current_user)):
    return user


app.include_router(auth_router)

api = APIRouter(prefix="/api", dependencies=[Depends(current_user)])

# ------------------------------------------------------------------ 1. audience live
@api.get("/audience/live", tags=["audience"])
def audience_live(minutes: int = Query(60, ge=1, le=1440, description="look-back window")):
    """Concurrent users / events in the last N minutes (real-time when Kafka streaming is running).
    Also returns the same window measured back from the newest event, so the dashboard is
    meaningful even when only historical data is loaded."""
    now_win = one("""SELECT count(DISTINCT user_key) AS users, count(*) AS events
                     FROM fact_view
                     WHERE event_timestamp >= (now() AT TIME ZONE 'utc') - make_interval(mins => :m)""", m=minutes)
    latest = one("SELECT max(event_timestamp) AS latest FROM fact_view")["latest"]
    hist = one("""SELECT count(DISTINCT user_key) AS users, count(*) AS events
                  FROM fact_view
                  WHERE event_timestamp >= (SELECT max(event_timestamp) FROM fact_view) - make_interval(mins => :m)""",
               m=minutes)
    by_device = rows("""SELECT dv.device_name, count(DISTINCT fv.user_key) AS users
                        FROM fact_view fv JOIN dim_device dv USING (device_key)
                        WHERE fv.event_timestamp >= (SELECT max(event_timestamp) FROM fact_view) - make_interval(mins => :m)
                        GROUP BY 1 ORDER BY 2 DESC""", m=minutes)
    stale_min = (datetime.now(timezone.utc).replace(tzinfo=None) - latest).total_seconds() / 60 if latest else None
    return {"window_minutes": minutes,
            "concurrent_users_now": now_win["users"], "events_now": now_win["events"],
            "latest_event_at": latest, "minutes_since_latest_event": stale_min,
            "is_live": bool(stale_min is not None and stale_min <= minutes),
            "users_in_window_before_latest_event": hist["users"],
            "events_in_window_before_latest_event": hist["events"],
            "by_device": by_device}


# ------------------------------------------------------------------ 2. content performance
@api.get("/content/{content_id}/performance", tags=["content"])
def content_performance(content_id: str = PathParam(..., pattern=ID_PATTERN, examples=["C00001"])):
    perf = one("""SELECT content_id, genre, language, total_views, total_watch_seconds, avg_watch_seconds,
                         unique_viewers, completions
                  FROM public_marts.mart_content_performance WHERE content_id = :c""", c=content_id)
    if not perf:
        raise HTTPException(404, f"Content '{content_id}' not found")
    rank = one("""SELECT count(*) + 1 AS r FROM public_marts.mart_content_performance
                  WHERE total_watch_seconds > :t""", t=perf["total_watch_seconds"] or 0)["r"]
    total = one("SELECT count(*) AS n FROM public_marts.mart_content_performance")["n"]
    comp = one("""SELECT avg(least(fv.watch_seconds / (c.duration_min * 60.0), 1.0)) AS completion_rate
                  FROM fact_view fv JOIN dim_content c USING (content_key)
                  WHERE c.content_id = :c AND fv.watch_seconds IS NOT NULL AND c.duration_min > 0""", c=content_id)
    trend = rows("""SELECT d.full_date AS date, count(*) AS views, coalesce(sum(fv.watch_seconds),0) AS watch_seconds
                    FROM fact_view fv JOIN dim_content c USING (content_key) JOIN dim_date d USING (date_key)
                    WHERE c.content_id = :c GROUP BY 1 ORDER BY 1 DESC LIMIT 30""", c=content_id)
    return {**perf, "completion_rate": comp["completion_rate"], "rank_by_watch_time": rank,
            "total_titles": total, "daily_trend": list(reversed(trend))}


@api.get("/content", tags=["content"])
def content_list(limit: int = Query(20, ge=1, le=200), genre: Optional[str] = Query(None, max_length=40)):
    """Top content by watch time (feeds the Content Analytics page)."""
    return rows("""SELECT content_id, genre, language, total_views, total_watch_seconds, unique_viewers, completions
                   FROM public_marts.mart_content_performance
                   WHERE (CAST(:g AS text) IS NULL OR genre = :g)
                   ORDER BY total_watch_seconds DESC NULLS LAST LIMIT :l""", g=genre, l=limit)


# ------------------------------------------------------------------ 3. subscriber health
@api.get("/subscriber/{user_id}/health", tags=["subscriber"])
def subscriber_health(user_id: str = PathParam(..., pattern=ID_PATTERN, examples=["U00001"])):
    user = one("SELECT user_key, user_id, subscription_type, region, device FROM dim_user WHERE user_id = :u", u=user_id)
    if not user:
        raise HTTPException(404, f"Subscriber '{user_id}' not found")
    k = user["user_key"]
    sub = one("""SELECT fs.subscription_id, fs.plan, fs.status, d.full_date AS start_date
                 FROM fact_subscription fs LEFT JOIN dim_date d ON d.date_key = fs.start_date_key
                 WHERE fs.user_key = :k ORDER BY fs.start_date_key DESC NULLS LAST LIMIT 1""", k=k)
    churn = one("""SELECT churn_probability, risk_tier, model_version, computed_at
                   FROM churn_risk_scores WHERE user_key = :k ORDER BY computed_at DESC LIMIT 1""", k=k)
    eng = one("""SELECT coalesce(sum(total_watch_seconds),0) AS total_watch_seconds,
                        coalesce(sum(session_count),0) AS sessions,
                        avg(completion_rate) AS avg_completion_rate, max(full_date) AS last_active_date
                 FROM public_marts.mart_engagement_daily WHERE user_key = :k""", k=k)
    latest_day = one("SELECT max(full_date) AS d FROM public_marts.mart_engagement_daily")["d"]
    days_inactive = (latest_day - eng["last_active_date"]).days if latest_day and eng["last_active_date"] else None
    recent = rows("""SELECT full_date AS date, total_watch_seconds, session_count
                     FROM public_marts.mart_engagement_daily WHERE user_key = :k ORDER BY full_date DESC LIMIT 30""", k=k)
    score = label = None
    if churn:
        score = round(100 * (1 - float(churn["churn_probability"])))
        label = "healthy" if score >= 67 else "at_risk" if score >= 34 else "critical"
    alerts = rows("""SELECT alert_id, alert_type, severity, status, trigger_reason
                     FROM alerts WHERE related_entity IN (:uk, :uid) ORDER BY alert_id DESC LIMIT 5""",
                  uk=str(k), uid=user_id)
    support = one("SELECT count(*) AS total, max(created_at) AS latest FROM fact_support WHERE user_key = :k", k=k)
    tickets = rows("""SELECT ticket_id, issue_type, created_at FROM fact_support
                      WHERE user_key = :k ORDER BY created_at DESC LIMIT 5""", k=k)
    return {"profile": {x: user[x] for x in ("user_id", "subscription_type", "region", "device")},
            "support": {"total_tickets": support["total"], "latest_ticket_at": support["latest"], "recent": tickets},
            "subscription": sub, "churn_risk": churn, "engagement": {**eng, "days_since_last_active": days_inactive},
            "health_score": score, "health_label": label, "recent_activity": list(reversed(recent)),
            "related_alerts": alerts}


# ------------------------------------------------------------------ 4. churn risk
@api.get("/churn-risk", tags=["subscriber"])
def churn_risk(tier: Optional[Literal["low", "medium", "high"]] = None,
               limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0)):
    total = one("SELECT count(*) AS n FROM churn_risk_scores WHERE (CAST(:t AS text) IS NULL OR risk_tier = :t)", t=tier)["n"]
    items = rows("""SELECT u.user_id, u.subscription_type, u.region, u.device, c.churn_probability,
                           c.risk_tier, c.features_used, c.model_version, c.computed_at
                    FROM churn_risk_scores c JOIN dim_user u USING (user_key)
                    WHERE (CAST(:t AS text) IS NULL OR c.risk_tier = :t)
                    ORDER BY c.churn_probability DESC, u.user_id LIMIT :l OFFSET :o""", t=tier, l=limit, o=offset)
    return {"total": total, "limit": limit, "offset": offset, "items": items}


# ------------------------------------------------------------------ 5. campaigns
@api.get("/campaigns", tags=["monetization"])
def campaigns(limit: int = Query(100, ge=1, le=500)):
    # Revenue is computed from the SUMMED impressions, not by adding up each
    # day's already-rounded cents - summing pre-rounded tiny amounts loses
    # almost all the money (many rows round to $0.00 individually).
    return rows("""SELECT campaign_id, sum(ad_opportunities) AS ad_opportunities, sum(impressions) AS impressions,
                          sum(clicks) AS clicks,
                          round(sum(impressions)::numeric / nullif(sum(ad_opportunities),0), 4) AS ad_fill_rate,
                          round(sum(clicks)::numeric / nullif(sum(impressions),0), 4) AS ctr,
                          round(sum(impressions)::numeric / 1000.0 * max(assumed_cpm_usd), 2) AS estimated_revenue_usd,
                          max(assumed_cpm_usd) AS assumed_cpm_usd
                   FROM public_marts.mart_ad_performance GROUP BY campaign_id
                   ORDER BY sum(impressions) DESC NULLS LAST LIMIT :l""", l=limit)


# ------------------------------------------------------------------ 6. dashboard
@api.get("/dashboard", tags=["dashboard"])
def dashboard():
    watch = one("SELECT coalesce(sum(watch_seconds),0) AS s, count(*) AS views, count(DISTINCT user_key) AS users FROM fact_view")
    comp = one("SELECT sum(avg_completion_rate * sample_size) / nullif(sum(sample_size),0) AS c FROM public_marts.mart_completion_rate")
    ads = one("""SELECT sum(ad_opportunities) AS o, sum(impressions) AS i, sum(clicks) AS c,
                        max(assumed_cpm_usd) AS cpm
                 FROM public_marts.mart_ad_performance""")
    ads_rev = round(float(ads["i"]) / 1000.0 * float(ads["cpm"]), 2) if ads["i"] and ads["cpm"] else 0.0
    subs = one("""SELECT count(*) AS total, count(*) FILTER (WHERE status='active') AS active,
                         count(*) FILTER (WHERE status IN ('cancelled','expired')) AS lost FROM fact_subscription""")
    ret = one("SELECT avg(day_over_day_retention_rate) AS r FROM public_marts.mart_retention")
    growth = rows("""SELECT full_date AS date, new_subscriptions, cumulative_subscriptions
                     FROM public_marts.mart_subscriber_growth ORDER BY full_date DESC LIMIT 30""")
    trend = rows("""SELECT full_date AS date, sum(total_watch_seconds) AS watch_seconds, sum(view_count) AS views
                    FROM public_marts.mart_watch_time GROUP BY 1 ORDER BY 1 DESC LIMIT 30""")
    alerts = rows("SELECT severity, count(*) AS n FROM alerts WHERE status = 'open' GROUP BY 1")
    genres = rows("""SELECT genre, sum(total_watch_seconds) AS watch_seconds FROM public_marts.mart_watch_time
                     GROUP BY 1 ORDER BY 2 DESC NULLS LAST LIMIT 8""")
    return {"kpis": {
                "total_watch_hours": round(float(watch["s"]) / 3600, 1), "total_views": watch["views"],
                "active_users": watch["users"],
                "completion_rate": comp["c"], "retention_day_over_day": ret["r"],
                "subscribers_total": subs["total"], "subscribers_active": subs["active"], "subscribers_lost": subs["lost"],
                "ad_fill_rate": (float(ads["i"]) / float(ads["o"])) if ads["o"] else None,
                "ctr": (float(ads["c"]) / float(ads["i"])) if ads["i"] else None,
                "estimated_revenue_usd": ads_rev,
                "open_alerts": {a["severity"]: a["n"] for a in alerts}},
            "watch_time_trend": list(reversed(trend)), "subscriber_growth": list(reversed(growth)),
            "watch_time_by_genre": genres}


# ------------------------------------------------------------------ 7. live events feed
@api.get("/events/live", tags=["audience"])
def events_live(limit: int = Query(30, ge=1, le=200)):
    items = rows("""SELECT e.kind, e.id, e.ts, u.user_id, c.content_id, e.detail FROM (
                      (SELECT 'view' AS kind, event_id AS id, event_timestamp AS ts, user_key, content_key,
                              coalesce(event_type,'unknown') AS detail FROM fact_view ORDER BY event_timestamp DESC LIMIT :l)
                      UNION ALL
                      (SELECT 'ad', ad_event_id, event_timestamp, user_key, content_key,
                              CASE WHEN click = 1 THEN 'click' WHEN impression = 1 THEN 'impression' ELSE 'no_fill' END
                       FROM fact_ad ORDER BY event_timestamp DESC LIMIT :l)) e
                    JOIN dim_user u USING (user_key) JOIN dim_content c USING (content_key)
                    ORDER BY e.ts DESC LIMIT :l""", l=limit)
    return {"count": len(items), "server_time": datetime.now(timezone.utc), "events": items}


# ------------------------------------------------------------------ 8/11. alerts
AlertStatus = Literal["open", "acknowledged", "resolved"]
Severity = Literal["info", "warning", "critical"]
AlertType = Literal["content_alert", "monetization_alert", "retention_campaign", "editorial_notification"]


@api.get("/alerts", tags=["alerts"])
def list_alerts(status: Optional[AlertStatus] = None, alert_type: Optional[AlertType] = None,
                severity: Optional[Severity] = None, limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0)):
    where = """(CAST(:s AS text) IS NULL OR status = :s) AND (CAST(:t AS text) IS NULL OR alert_type = :t)
               AND (CAST(:v AS text) IS NULL OR severity = :v)"""
    total = one(f"SELECT count(*) AS n FROM alerts WHERE {where}", s=status, t=alert_type, v=severity)["n"]
    items = rows(f"""SELECT alert_id, alert_type, severity, status, owner, trigger_reason, related_entity, notified,
                            notify_channel, audit_log, triggered_at, resolved_at
                     FROM alerts WHERE {where} ORDER BY alert_id DESC LIMIT :l OFFSET :o""",
                 s=status, t=alert_type, v=severity, l=limit, o=offset)
    return {"total": total, "limit": limit, "offset": offset, "items": items}


class AckBody(BaseModel):
    note: Optional[str] = Field(None, max_length=500)


@api.post("/alerts/{alert_id}/acknowledge", tags=["alerts"])
def acknowledge_alert(alert_id: int = PathParam(..., ge=1), body: AckBody = AckBody(),
                      user: dict = Depends(require_role("admin", "analyst"))):
    """Analyst/admin only. Sets status=acknowledged, owner=caller, appends to the audit trail."""
    current = one("SELECT status FROM alerts WHERE alert_id = :a", a=alert_id)
    if not current:
        raise HTTPException(404, f"Alert {alert_id} not found")
    if current["status"] != "open":
        raise HTTPException(409, f"Alert already {current['status']}")
    entry = [{"event": "acknowledged", "by": user["username"], "role": user["role"],
              "at": datetime.now(timezone.utc).isoformat(), "note": body.note}]
    with engine.begin() as conn:
        conn.execute(text("""UPDATE alerts SET status = 'acknowledged', owner = :o,
                             audit_log = audit_log || CAST(:e AS jsonb) WHERE alert_id = :a"""),
                     {"o": user["username"], "e": json.dumps(entry), "a": alert_id})
    log.info("alert %s acknowledged by %s", alert_id, user["username"])
    return one("SELECT alert_id, alert_type, severity, status, owner, audit_log FROM alerts WHERE alert_id = :a", a=alert_id)


# ------------------------------------------------------------------ 9. forecasts
@api.get("/forecasts", tags=["intelligence"])
def forecasts(history_days: int = Query(14, ge=0, le=90)):
    fc = rows("""SELECT forecast_date AS date, predicted_watch_seconds, lower_bound, upper_bound, model_version, generated_at
                 FROM watch_time_forecast ORDER BY forecast_date""")
    hist = rows("""SELECT full_date AS date, sum(total_watch_seconds) AS watch_seconds FROM public_marts.mart_watch_time
                   GROUP BY 1 ORDER BY 1 DESC LIMIT :h""", h=history_days)
    return {"forecast": fc, "history": list(reversed(hist)),
            "note": "Linear-trend baseline; see docs/ml_model_cards.md for limitations."}


# ------------------------------------------------------------------ 10. risk summary
@api.get("/risk", tags=["intelligence"])
def risk():
    tiers = rows("SELECT risk_tier, count(*) AS users FROM churn_risk_scores GROUP BY 1")
    open_alerts = rows("SELECT alert_type, severity, count(*) AS n FROM alerts WHERE status='open' GROUP BY 1,2 ORDER BY 3 DESC")
    anomalies = {
        "engagement_drops": one("SELECT count(*) AS n FROM engagement_anomalies")["n"],
        "ad_anomalies": one("SELECT count(*) AS n FROM ad_anomalies")["n"],
        "content_surges": one("SELECT count(*) AS n FROM content_trend_anomalies")["n"]}
    critical = sum(a["n"] for a in open_alerts if a["severity"] == "critical")
    warning = sum(a["n"] for a in open_alerts if a["severity"] == "warning")
    level = "high" if critical else "medium" if warning else "low"
    top = rows("""SELECT u.user_id, c.churn_probability FROM churn_risk_scores c JOIN dim_user u USING (user_key)
                  ORDER BY c.churn_probability DESC LIMIT 5""")
    return {"overall_risk_level": level, "churn_tiers": {t["risk_tier"]: t["users"] for t in tiers},
            "open_alerts": open_alerts, "anomaly_counts": anomalies, "top_churn_risk_users": top}


# ------------------------------------------------------------------ 12. data quality
QUALITY_TABLES = ["dim_user", "dim_content", "dim_device", "dim_geography", "dim_campaign", "dim_date",
                  "fact_view", "fact_ad", "fact_subscription", "fact_support", "alerts", "churn_risk_scores"]
MIN_VIEW_ROWS = int(os.environ.get("MIN_VIEW_ROWS", "1000"))
FRESHNESS_HOURS = float(os.environ.get("FRESHNESS_HOURS", "24"))
DEAD_LETTER = Path(os.environ.get("DEAD_LETTER_FILE", "kafka/dead_letter_events.jsonl"))


@api.get("/data-quality", tags=["ops"])
def data_quality():
    checks = []

    def add(name, category, value, ok, threshold, warn=False):
        checks.append({"check": name, "category": category, "value": value, "threshold": threshold,
                       "status": "pass" if ok else ("warn" if warn else "fail")})

    counts = {t: one(f"SELECT count(*) AS n FROM {t}")["n"] for t in QUALITY_TABLES}
    add("fact_view volume", "volume", counts["fact_view"], counts["fact_view"] >= MIN_VIEW_ROWS, f">= {MIN_VIEW_ROWS}")
    nulls = one("SELECT 100.0 * count(*) FILTER (WHERE watch_seconds IS NULL) / nullif(count(*),0) AS p FROM fact_view")["p"]
    add("watch_seconds null %", "null", round(float(nulls or 0), 2), (nulls or 0) < 10, "< 10 %", warn=True)
    dup = one("SELECT count(*) - count(DISTINCT event_id) AS d FROM fact_view")["d"]
    add("duplicate event_id in fact_view", "duplicate", dup, dup == 0, "= 0")
    dup_u = one("SELECT count(*) - count(DISTINCT user_id) AS d FROM dim_user")["d"]
    add("uniqueness dim_user.user_id", "uniqueness", dup_u, dup_u == 0, "= 0")
    orphan = one("""SELECT count(*) AS n FROM fact_view f LEFT JOIN dim_user u USING (user_key) WHERE u.user_key IS NULL""")["n"]
    add("orphan fact_view -> dim_user", "referential", orphan, orphan == 0, "= 0")
    orphan_c = one("""SELECT count(*) AS n FROM fact_ad f LEFT JOIN dim_campaign c USING (campaign_key) WHERE c.campaign_key IS NULL""")["n"]
    add("orphan fact_ad -> dim_campaign", "referential", orphan_c, orphan_c == 0, "= 0")
    neg = one("SELECT count(*) AS n FROM fact_view WHERE watch_seconds < 0 OR watch_seconds > 21600")["n"]
    add("watch_seconds in [0, 21600]", "range", neg, neg == 0, "= 0 violations")
    bad_click = one("SELECT count(*) AS n FROM fact_ad WHERE click = 1 AND coalesce(impression,0) <> 1")["n"]
    add("business rule: click implies impression", "business_rule", bad_click, bad_click == 0, "= 0 violations")
    latest = one("SELECT max(event_timestamp) AS t FROM fact_view")["t"]
    age_h = (datetime.now(timezone.utc).replace(tzinfo=None) - latest).total_seconds() / 3600 if latest else None
    add("freshness (hours since newest view)", "freshness", round(age_h, 1) if age_h is not None else None,
        age_h is not None and age_h <= FRESHNESS_HOURS, f"<= {FRESHNESS_HOURS} h", warn=True)

    runs, failed_24h = [], None
    try:
        runs = rows("SELECT dag_id, task_id, status, message, logged_at FROM pipeline_run_log ORDER BY id DESC LIMIT 10")
        failed_24h = one("""SELECT count(*) AS n FROM pipeline_run_log WHERE status='failed'
                            AND logged_at > now() - interval '24 hours'""")["n"]
        add("pipeline failures (24h)", "pipeline", failed_24h, failed_24h == 0, "= 0")
    except SQLAlchemyError:
        add("pipeline run log available", "pipeline", "table missing (run ops/ops_schema.sql)", False, "exists", warn=True)
    dead = sum(1 for _ in DEAD_LETTER.open()) if DEAD_LETTER.exists() else 0
    add("Kafka dead-letter events", "streaming", dead, dead == 0, "= 0", warn=True)

    failed = [c for c in checks if c["status"] == "fail"]
    warned = [c for c in checks if c["status"] == "warn"]
    return {"overall_status": "fail" if failed else "warn" if warned else "pass",
            "checked_at": datetime.now(timezone.utc), "summary": {"pass": len(checks) - len(failed) - len(warned),
                                                                   "warn": len(warned), "fail": len(failed)},
            "checks": checks, "row_counts": counts, "recent_pipeline_runs": runs}


app.include_router(api)

# ------------------------------------------------------------------ frontend (static SPA)
FRONTEND_DIR = Path(os.environ.get("FRONTEND_DIR", Path(__file__).resolve().parents[2] / "frontend"))
if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")
else:
    log.warning("frontend directory not found at %s - API only", FRONTEND_DIR)
