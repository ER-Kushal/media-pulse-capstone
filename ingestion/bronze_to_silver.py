"""
MediaPulse - Bronze to Silver Cleaning Pipeline
=================================================
Reads raw ("Bronze") CSVs and produces cleaned, validated ("Silver") CSVs.

Usage:
    python bronze_to_silver.py

Expects raw CSVs in ./data/ (relative to where you run this script):
    users_raw.csv, content_raw.csv, views_raw.csv,
    ads_raw.csv, subscriptions_raw.csv, support_raw.csv

Writes cleaned CSVs to ./data_silver/, plus a data_quality_report.txt
summarizing every fix that was made (useful evidence for your assignment's
"Data Quality & Observability" deliverable).
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone
import os

RAW_DIR = "data"
SILVER_DIR = "data_silver"
REPORT_PATH = os.path.join(SILVER_DIR, "data_quality_report.txt")

report_lines = []


def log(msg):
    print(msg)
    report_lines.append(msg)


def section(title):
    log("\n" + "=" * 70)
    log(title)
    log("=" * 70)


def clean_text_column(series, mapping=None):
    """Trim whitespace, collapse case, optionally map known variants."""
    cleaned = series.astype("string").str.strip()
    cleaned_upper = cleaned.str.upper()
    if mapping:
        cleaned = cleaned_upper.map(mapping).fillna(cleaned)
    return cleaned


def dedupe(df, key, name):
    before = len(df)
    df = df.drop_duplicates(subset=[key], keep="first")
    removed = before - len(df)
    log(f"[{name}] removed {removed} duplicate rows (by {key})")
    return df


def add_audit_columns(df, source_file):
    df["source_file"] = source_file
    df["ingested_at"] = datetime.now(timezone.utc).isoformat()
    return df


def main():
    os.makedirs(SILVER_DIR, exist_ok=True)
    section("MEDIAPULSE BRONZE -> SILVER CLEANING RUN")
    log(f"Run time: {datetime.now(timezone.utc).isoformat()} UTC")

    # ---------------- USERS ----------------
    section("Cleaning users_raw.csv")
    users = pd.read_csv(os.path.join(RAW_DIR, "users_raw.csv"))
    before = len(users)

    users = dedupe(users, "user_id", "users")

    # Drop rows with no user_id at all - can't use them for anything
    users = users.dropna(subset=["user_id"])

    region_map = {
        "NORTH": "North", "SOUTH": "South", "EAST": "East",
        "WEST": "West", "CENTRAL": "Central",
    }
    users["region"] = clean_text_column(users["region"], region_map)
    users["region"] = users["region"].replace({"Unknown": pd.NA})
    n_missing_region = users["region"].isna().sum()
    users["region"] = users["region"].fillna("Unknown")
    log(f"[users] normalized region casing, {n_missing_region} missing/'Unknown' -> 'Unknown'")

    device_map = {
        "MOBILE": "Mobile", "TV": "TV", "SMART TV": "Smart TV",
        "TABLET": "Tablet", "WEB": "Web",
    }
    users["device"] = clean_text_column(users["device"], device_map)
    n_missing_device = users["device"].isna().sum()
    users["device"] = users["device"].fillna("Unknown")
    log(f"[users] normalized device casing, {n_missing_device} missing -> 'Unknown'")

    sub_type_map = {
        "FREE": "Free", "BASIC": "Basic", "PREMIUM": "Premium", "FAMILY": "Family",
    }
    users["subscription_type"] = clean_text_column(users["subscription_type"], sub_type_map)
    n_missing_sub = users["subscription_type"].isna().sum()
    users["subscription_type"] = users["subscription_type"].fillna("Unknown")
    log(f"[users] normalized subscription_type casing, {n_missing_sub} missing -> 'Unknown'")

    users = add_audit_columns(users, "users_raw.csv")
    log(f"[users] {before} raw rows -> {len(users)} clean rows")

    # ---------------- CONTENT ----------------
    section("Cleaning content_raw.csv")
    content = pd.read_csv(os.path.join(RAW_DIR, "content_raw.csv"))
    before = len(content)

    content = dedupe(content, "content_id", "content")
    content = content.dropna(subset=["content_id"])

    content["genre"] = clean_text_column(content["genre"]).fillna("Unknown")
    content["language"] = clean_text_column(content["language"]).fillna("Unknown")

    content["duration"] = pd.to_numeric(content["duration"], errors="coerce")
    n_bad_duration = (content["duration"] < 0).sum()
    content.loc[content["duration"] < 0, "duration"] = np.nan
    median_duration = content["duration"].median()
    n_missing_duration = content["duration"].isna().sum()
    content["duration"] = content["duration"].fillna(median_duration)
    log(f"[content] {n_bad_duration} negative durations nulled, "
        f"{n_missing_duration} missing durations filled with median ({median_duration:.0f} min)")

    content["release_date"] = pd.to_datetime(content["release_date"], errors="coerce")
    n_bad_dates = content["release_date"].isna().sum()
    log(f"[content] {n_bad_dates} unparsable release_date values set to null")

    content = add_audit_columns(content, "content_raw.csv")
    log(f"[content] {before} raw rows -> {len(content)} clean rows")

    valid_user_ids = set(users["user_id"])
    valid_content_ids = set(content["content_id"])

    # ---------------- VIEWS ----------------
    section("Cleaning views_raw.csv")
    views = pd.read_csv(os.path.join(RAW_DIR, "views_raw.csv"))
    before = len(views)

    views = dedupe(views, "event_id", "views")
    views = views.dropna(subset=["event_id"])

    n_orphan_user = (~views["user_id"].isin(valid_user_ids)).sum()
    n_orphan_content = (~views["content_id"].isin(valid_content_ids)).sum()
    views = views[views["user_id"].isin(valid_user_ids)]
    views = views[views["content_id"].isin(valid_content_ids)]
    log(f"[views] dropped {n_orphan_user} rows with unknown user_id, "
        f"{n_orphan_content} rows with unknown content_id")

    event_type_map = {"PLAY": "play", "PAUSE": "pause", "SKIP": "skip",
                       "COMPLETE": "complete", "BUFFER": "buffer", "RESUME": "resume"}
    views["event_type"] = views["event_type"].astype("string").str.strip().str.lower()
    n_missing_event = views["event_type"].isna().sum()
    views["event_type"] = views["event_type"].fillna("unknown")
    log(f"[views] lowercased event_type, {n_missing_event} missing -> 'unknown'")

    views["watch_seconds"] = pd.to_numeric(views["watch_seconds"], errors="coerce")
    n_negative_ws = (views["watch_seconds"] < 0).sum()
    n_extreme_ws = (views["watch_seconds"] > 21600).sum()  # >6 hours in one event = implausible
    views.loc[views["watch_seconds"] < 0, "watch_seconds"] = np.nan
    views.loc[views["watch_seconds"] > 21600, "watch_seconds"] = np.nan
    n_missing_ws = views["watch_seconds"].isna().sum()
    log(f"[views] {n_negative_ws} negative watch_seconds nulled, "
        f"{n_extreme_ws} implausibly large (>6h) nulled, "
        f"{n_missing_ws} total now null (kept as null, not imputed, to avoid biasing watch-time KPIs)")

    views["timestamp"] = pd.to_datetime(views["timestamp"], errors="coerce")
    n_bad_ts = views["timestamp"].isna().sum()
    views = views.dropna(subset=["timestamp"])
    log(f"[views] dropped {n_bad_ts} rows with unparsable timestamp")

    views = add_audit_columns(views, "views_raw.csv")
    log(f"[views] {before} raw rows -> {len(views)} clean rows")

    # ---------------- ADS ----------------
    section("Cleaning ads_raw.csv")
    ads = pd.read_csv(os.path.join(RAW_DIR, "ads_raw.csv"))
    before = len(ads)

    ads = dedupe(ads, "ad_event_id", "ads")
    ads = ads.dropna(subset=["ad_event_id"])

    n_orphan_user = (~ads["user_id"].isin(valid_user_ids)).sum()
    n_orphan_content = (~ads["content_id"].isin(valid_content_ids)).sum()
    ads = ads[ads["user_id"].isin(valid_user_ids)]
    ads = ads[ads["content_id"].isin(valid_content_ids)]
    log(f"[ads] dropped {n_orphan_user} rows with unknown user_id, "
        f"{n_orphan_content} rows with unknown content_id")

    def normalize_binary(series, true_vals, false_vals, label):
        s = series.astype("string").str.strip().str.lower()
        out = pd.Series(pd.NA, index=s.index, dtype="Int64")
        out[s.isin(true_vals)] = 1
        out[s.isin(false_vals)] = 0
        n_invalid = s.notna().sum() - out.notna().sum()
        log(f"[ads] {label}: {n_invalid} invalid values (e.g. -1, 2) set to null")
        return out

    ads["impression"] = normalize_binary(
        ads["impression"], true_vals={"1", "yes"}, false_vals={"0", "no"}, label="impression")
    ads["click"] = normalize_binary(
        ads["click"], true_vals={"1", "yes", "clicked"}, false_vals={"0", "no"}, label="click")

    # Business rule: a click cannot happen without an impression.
    # NOTE: impression can be null (unknown); pandas treats null comparisons as "unknown"
    # (not True), so we fill nulls first or those rows would silently escape the rule.
    invalid_click = ((ads["click"] == 1) & (ads["impression"].fillna(0) != 1)).fillna(False)
    n_invalid_click = invalid_click.sum()
    ads.loc[invalid_click, "impression"] = 1
    log(f"[ads] business rule 'click implies impression': fixed {n_invalid_click} rows "
        f"where click=1 but impression was 0/unknown (set impression=1)")

    ads["timestamp"] = pd.to_datetime(ads["timestamp"], errors="coerce")
    n_bad_ts = ads["timestamp"].isna().sum()
    ads = ads.dropna(subset=["timestamp"])
    log(f"[ads] dropped {n_bad_ts} rows with unparsable/missing timestamp")

    n_missing_campaign = ads["campaign_id"].isna().sum()
    ads["campaign_id"] = ads["campaign_id"].fillna("UNKNOWN")
    log(f"[ads] {n_missing_campaign} missing campaign_id -> 'UNKNOWN'")

    ads = add_audit_columns(ads, "ads_raw.csv")
    log(f"[ads] {before} raw rows -> {len(ads)} clean rows")

    # ---------------- SUBSCRIPTIONS ----------------
    section("Cleaning subscriptions_raw.csv")
    subs = pd.read_csv(os.path.join(RAW_DIR, "subscriptions_raw.csv"))
    before = len(subs)

    subs = dedupe(subs, "subscription_id", "subscriptions")
    subs = subs.dropna(subset=["subscription_id"])

    n_orphan_user = (~subs["user_id"].isin(valid_user_ids)).sum()
    subs = subs[subs["user_id"].isin(valid_user_ids)]
    log(f"[subscriptions] dropped {n_orphan_user} rows with unknown user_id")

    plan_map = {"BASIC": "Basic", "STANDARD": "Standard", "PREMIUM": "Premium",
                "PREM": "Premium", "FAMILY": "Family", "GOLD": "Premium"}
    subs["plan"] = clean_text_column(subs["plan"], plan_map)
    n_missing_plan = subs["plan"].isna().sum()
    subs["plan"] = subs["plan"].fillna("Unknown")
    log(f"[subscriptions] normalized plan values (incl. 'PREM'->Premium, 'Gold'->Premium), "
        f"{n_missing_plan} missing -> 'Unknown'")

    status_map = {"ACTIVE": "active", "CANCELLED": "cancelled", "EXPIRED": "expired",
                  "PAUSED": "paused", "UNKNOWN": "unknown"}
    subs["status"] = subs["status"].astype("string").str.strip().str.upper().map(status_map)
    n_missing_status = subs["status"].isna().sum()
    subs["status"] = subs["status"].fillna("unknown")
    log(f"[subscriptions] normalized status casing, {n_missing_status} missing -> 'unknown'")

    subs["start_date"] = pd.to_datetime(subs["start_date"], errors="coerce")
    n_bad_dates = subs["start_date"].isna().sum()
    log(f"[subscriptions] {n_bad_dates} unparsable start_date values set to null (rows kept)")

    subs = add_audit_columns(subs, "subscriptions_raw.csv")
    log(f"[subscriptions] {before} raw rows -> {len(subs)} clean rows")

    # ---------------- SUPPORT ----------------
    section("Cleaning support_raw.csv")
    support = pd.read_csv(os.path.join(RAW_DIR, "support_raw.csv"))
    before = len(support)

    support = dedupe(support, "ticket_id", "support")
    support = support.dropna(subset=["ticket_id"])

    n_orphan_user = (~support["user_id"].isin(valid_user_ids)).sum()
    support = support[support["user_id"].isin(valid_user_ids)]
    log(f"[support] dropped {n_orphan_user} rows with unknown user_id")

    support["issue_type"] = clean_text_column(support["issue_type"]).fillna("Unknown")

    support["created_at"] = pd.to_datetime(support["created_at"], errors="coerce")
    n_bad_ts = support["created_at"].isna().sum()
    support = support.dropna(subset=["created_at"])
    log(f"[support] dropped {n_bad_ts} rows with unparsable created_at")

    support = add_audit_columns(support, "support_raw.csv")
    log(f"[support] {before} raw rows -> {len(support)} clean rows")

    # ---------------- WRITE OUTPUT ----------------
    section("Writing Silver CSVs")
    outputs = {
        "users_silver.csv": users,
        "content_silver.csv": content,
        "views_silver.csv": views,
        "ads_silver.csv": ads,
        "subscriptions_silver.csv": subs,
        "support_silver.csv": support,
    }
    for filename, df in outputs.items():
        path = os.path.join(SILVER_DIR, filename)
        df.to_csv(path, index=False)
        log(f"wrote {path} ({len(df)} rows)")

    with open(REPORT_PATH, "w") as f:
        f.write("\n".join(report_lines))
    print(f"\nFull report saved to {REPORT_PATH}")


if __name__ == "__main__":
    main()
