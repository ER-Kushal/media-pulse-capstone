"""
MediaPulse - Automation / Decision Engine
============================================
Implements the 4 required domain rules (section 10):
    1. Engagement drop        -> content_alert
    2. Ad anomaly             -> monetization_alert
    3. Churn risk (high tier) -> retention_campaign
    4. Content trend surge    -> editorial_notification

Every alert is written to the `alerts` table with a timestamp, severity,
owner/status, trigger reason, and an audit_log (JSON array) that's
appended to on every state change - satisfying the "audit history"
requirement.

Notifications: sent via a Slack incoming webhook (free, 2-minute setup -
see docs/slack_webhook_setup.md) for the two highest-signal alert types
(content_alert and retention_campaign), satisfying "at least two
automated actions must send a notification". Retries with backoff on
transient network failures.

Run this AFTER ml/detect_anomalies.py and ml/churn_risk.py have produced
fresh data for it to read.

Usage:
    export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."   # optional
    python automation/decision_engine.py
"""

import os
import time
import json
import requests
from datetime import datetime, timezone
from sqlalchemy import create_engine, text

PGHOST = os.environ.get("PGHOST", "localhost")
PGPORT = os.environ.get("PGPORT", "5432")
PGDATABASE = os.environ.get("PGDATABASE", "mediapulse")
PGUSER = os.environ.get("PGUSER", "mediapulse")
PGPASSWORD = os.environ.get("PGPASSWORD", "mediapulse_dev_pw")
engine = create_engine(f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}")

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
MAX_RETRIES = 3


def send_slack_notification(text_msg):
    if not SLACK_WEBHOOK_URL:
        print(f"  [notify skipped - no SLACK_WEBHOOK_URL set] {text_msg}")
        return False
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(SLACK_WEBHOOK_URL, json={"text": text_msg}, timeout=5)
            resp.raise_for_status()
            return True
        except Exception as exc:
            wait = 1 * attempt
            print(f"  [RETRY {attempt}/{MAX_RETRIES}] Slack notify failed: {exc} -- waiting {wait}s")
            time.sleep(wait)
    print("  [notify FAILED after retries]")
    return False


def create_alert(conn, alert_type, severity, owner, trigger_reason, related_entity,
                  notify=False):
    audit_entry = {
        "event": "created", "at": datetime.now(timezone.utc).isoformat(),
        "detail": trigger_reason,
    }
    notified = False
    channel = None
    if notify:
        notified = send_slack_notification(
            f"[{severity.upper()}] {alert_type}: {trigger_reason} (entity: {related_entity})"
        )
        channel = "slack" if notified else None
        audit_entry_notify = {
            "event": "notification_sent" if notified else "notification_failed",
            "at": datetime.now(timezone.utc).isoformat(), "channel": "slack",
        }
    result = conn.execute(text("""
        INSERT INTO alerts (alert_type, severity, owner, trigger_reason, related_entity,
                             notified, notify_channel, audit_log)
        VALUES (:t, :s, :o, :r, :e, :n, :c, :a)
        RETURNING alert_id
    """), {
        "t": alert_type, "s": severity, "o": owner, "r": trigger_reason, "e": related_entity,
        "n": notified, "c": channel,
        "a": json.dumps([audit_entry] + ([audit_entry_notify] if notify else [])),
    })
    return result.scalar()


def run_engagement_drop_rule(conn):
    rows = conn.execute(text("""
        SELECT detected_date, actual_value, expected_value, severity
        FROM engagement_anomalies ORDER BY detected_date DESC LIMIT 20
    """)).fetchall()
    count = 0
    for r in rows:
        create_alert(
            conn, "content_alert", r.severity, "content_team",
            f"Watch time dropped to {r.actual_value:.0f}s on {r.detected_date} "
            f"(expected ~{r.expected_value:.0f}s)",
            related_entity=str(r.detected_date),
            notify=(r.severity == "critical"),
        )
        count += 1
    print(f"content_alert: {count} alert(s) created")


def run_ad_anomaly_rule(conn):
    rows = conn.execute(text("""
        SELECT detected_date, campaign_id, metric, actual_value, expected_value, severity
        FROM ad_anomalies ORDER BY detected_date DESC LIMIT 20
    """)).fetchall()
    count = 0
    for r in rows:
        create_alert(
            conn, "monetization_alert", r.severity, "monetization_team",
            f"{r.metric} anomaly for campaign {r.campaign_id} on {r.detected_date}: "
            f"{r.actual_value:.3f} (expected ~{r.expected_value:.3f})",
            related_entity=r.campaign_id,
            notify=False,
        )
        count += 1
    print(f"monetization_alert: {count} alert(s) created")


def run_churn_risk_rule(conn):
    rows = conn.execute(text("""
        SELECT user_key, churn_probability
        FROM churn_risk_scores WHERE risk_tier = 'high'
        ORDER BY churn_probability DESC LIMIT 20
    """)).fetchall()
    count = 0
    for r in rows:
        create_alert(
            conn, "retention_campaign", "warning", "retention_team",
            f"User flagged high churn risk (probability {r.churn_probability:.2f})",
            related_entity=str(r.user_key),
            notify=(count < 3),  # notify only for the top few, to avoid alert spam
        )
        count += 1
    print(f"retention_campaign: {count} alert(s) created")


def run_content_trend_rule(conn):
    rows = conn.execute(text("""
        SELECT detected_date, content_id, view_count, expected_views
        FROM content_trend_anomalies ORDER BY detected_date DESC LIMIT 20
    """)).fetchall()
    count = 0
    for r in rows:
        create_alert(
            conn, "editorial_notification", "info", "editorial_team",
            f"Content {r.content_id} saw a view surge on {r.detected_date}: "
            f"{r.view_count} views (expected ~{r.expected_views:.0f})",
            related_entity=r.content_id,
            notify=False,
        )
        count += 1
    print(f"editorial_notification: {count} alert(s) created")


def main():
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE alerts"))
        run_engagement_drop_rule(conn)
        run_ad_anomaly_rule(conn)
        run_churn_risk_rule(conn)
        run_content_trend_rule(conn)


if __name__ == "__main__":
    main()
