"""
MediaPulse - Load Silver CSVs into the Postgres warehouse (star schema)
========================================================================
Run this AFTER bronze_to_silver.py has produced data_silver/*.csv,
and AFTER schema.sql has been applied to your Postgres database.

INCREMENTAL + IDEMPOTENT: every table is loaded with INSERT ... ON CONFLICT
DO NOTHING on its business key, so running this twice never duplicates rows
and never wipes rows that arrived via Kafka.

Usage:
    pip install psycopg2-binary pandas
    python load_warehouse.py

Reads connection settings from environment variables (with defaults
matching docker-compose.yml), so you never hardcode credentials:
    PGHOST=localhost PGPORT=5432 PGDATABASE=mediapulse
    PGUSER=mediapulse PGPASSWORD=mediapulse_dev_pw
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text

SILVER_DIR = "data_silver"

PGHOST = os.environ.get("PGHOST", "localhost")
PGPORT = os.environ.get("PGPORT", "5432")
PGDATABASE = os.environ.get("PGDATABASE", "mediapulse")
PGUSER = os.environ.get("PGUSER", "mediapulse")
PGPASSWORD = os.environ.get("PGPASSWORD", "mediapulse_dev_pw")

engine = create_engine(
    f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"
)


def load_csv(name):
    return pd.read_csv(os.path.join(SILVER_DIR, name))


def insert_ignore(df, table, conflict_cols):
    """Incremental, idempotent load: rows whose business key already exists are skipped.
    Data goes into a temp staging table first, then INSERT ... ON CONFLICT DO NOTHING."""
    if df.empty:
        return 0
    tmp = f"_stg_{table}"
    df.to_sql(tmp, engine, if_exists="replace", index=False)
    cols = ", ".join(f'"{c}"' for c in df.columns)
    with engine.begin() as conn:
        res = conn.execute(text(
            f'INSERT INTO {table} ({cols}) SELECT {cols} FROM {tmp} '
            f'ON CONFLICT ({", ".join(conflict_cols)}) DO NOTHING'))
        inserted = res.rowcount
        conn.execute(text(f"DROP TABLE {tmp}"))
    return inserted


def build_date_dim(all_dates):
    """all_dates: a pandas Series of datetime64 values (may include NaT)."""
    dates = pd.to_datetime(pd.Series(all_dates)).dropna().dt.normalize().unique()
    df = pd.DataFrame({"full_date": pd.to_datetime(dates)})
    df["date_key"] = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["year"] = df["full_date"].dt.year
    df["quarter"] = df["full_date"].dt.quarter
    df["month"] = df["full_date"].dt.month
    df["month_name"] = df["full_date"].dt.strftime("%B")
    df["day"] = df["full_date"].dt.day
    df["day_of_week"] = df["full_date"].dt.dayofweek
    df["day_name"] = df["full_date"].dt.strftime("%A")
    df["is_weekend"] = df["day_of_week"].isin([5, 6])
    return df[["date_key", "full_date", "year", "quarter", "month", "month_name",
               "day", "day_of_week", "day_name", "is_weekend"]]


def main():
    print("Loading Silver CSVs...")
    users = load_csv("users_silver.csv")
    content = load_csv("content_silver.csv")
    views = load_csv("views_silver.csv")
    ads = load_csv("ads_silver.csv")
    subs = load_csv("subscriptions_silver.csv")
    support = load_csv("support_silver.csv")
    support["created_at"] = pd.to_datetime(support["created_at"])

    views["event_timestamp"] = pd.to_datetime(views["timestamp"])
    ads["event_timestamp"] = pd.to_datetime(ads["timestamp"])
    subs["start_date"] = pd.to_datetime(subs["start_date"])
    content["release_date"] = pd.to_datetime(content["release_date"])

    # -------- dim_date: built from every date appearing anywhere --------
    all_dates = pd.concat([
        views["event_timestamp"], ads["event_timestamp"], subs["start_date"], support["created_at"]
    ])
    dim_date = build_date_dim(all_dates)
    n = insert_ignore(dim_date, "dim_date", ["date_key"])
    print(f"  dim_date: {n} new rows")

    # -------- dim_user --------
    dim_user = users.rename(columns={
        "subscription_type": "subscription_type", "region": "region", "device": "device"
    })[["user_id", "subscription_type", "region", "device", "source_file"]]
    print(f"  dim_user: {insert_ignore(dim_user, 'dim_user', ['user_id'])} new rows")

    # -------- dim_content --------
    dim_content = content.rename(columns={"duration": "duration_min"})[
        ["content_id", "genre", "language", "release_date", "duration_min", "source_file"]
    ]
    print(f"  dim_content: {insert_ignore(dim_content, 'dim_content', ['content_id'])} new rows")

    # -------- dim_device / dim_geography / dim_campaign --------
    dim_device = pd.DataFrame({"device_name": users["device"].dropna().unique()})
    print(f"  dim_device: {insert_ignore(dim_device, 'dim_device', ['device_name'])} new rows")

    dim_geo = pd.DataFrame({"region": users["region"].dropna().unique()})
    print(f"  dim_geography: {insert_ignore(dim_geo, 'dim_geography', ['region'])} new rows")

    dim_campaign = pd.DataFrame({"campaign_id": ads["campaign_id"].dropna().unique()})
    print(f"  dim_campaign: {insert_ignore(dim_campaign, 'dim_campaign', ['campaign_id'])} new rows")

    # -------- pull back surrogate keys for joining --------
    user_keys = pd.read_sql("SELECT user_key, user_id FROM dim_user", engine)
    content_keys = pd.read_sql("SELECT content_key, content_id FROM dim_content", engine)
    device_keys = pd.read_sql("SELECT device_key, device_name FROM dim_device", engine)
    geo_keys = pd.read_sql("SELECT geography_key, region FROM dim_geography", engine)
    campaign_keys = pd.read_sql("SELECT campaign_key, campaign_id FROM dim_campaign", engine)
    date_keys = pd.read_sql("SELECT date_key, full_date FROM dim_date", engine)
    date_keys["full_date"] = pd.to_datetime(date_keys["full_date"])

    user_lookup = users[["user_id", "device", "region"]].merge(user_keys, on="user_id", how="left")
    user_lookup = user_lookup.merge(device_keys, left_on="device", right_on="device_name", how="left")
    user_lookup = user_lookup.merge(geo_keys, left_on="region", right_on="region", how="left")
    user_lookup = user_lookup[["user_id", "user_key", "device_key", "geography_key"]]

    # -------- fact_view --------
    fv = views.merge(user_lookup, on="user_id", how="inner")
    fv = fv.merge(content_keys, on="content_id", how="inner")
    fv["date_only"] = fv["event_timestamp"].dt.normalize()
    fv = fv.merge(date_keys, left_on="date_only", right_on="full_date", how="inner")
    fact_view = fv.rename(columns={"event_id": "event_id"})[[
        "event_id", "user_key", "content_key", "device_key", "geography_key",
        "date_key", "event_timestamp", "event_type", "watch_seconds", "source_file"
    ]]
    n = insert_ignore(fact_view, "fact_view", ["event_id"])
    print(f"  fact_view: {n} new rows (of {len(views)} silver rows; duplicates skipped)")

    # -------- fact_ad --------
    fa = ads.merge(user_lookup[["user_id", "user_key", "geography_key"]], on="user_id", how="inner")
    fa = fa.merge(content_keys, on="content_id", how="inner")
    fa = fa.merge(campaign_keys, on="campaign_id", how="inner")
    fa["date_only"] = fa["event_timestamp"].dt.normalize()
    fa = fa.merge(date_keys, left_on="date_only", right_on="full_date", how="inner")
    fact_ad = fa[[
        "ad_event_id", "user_key", "content_key", "campaign_key", "geography_key",
        "date_key", "event_timestamp", "impression", "click", "source_file"
    ]]
    n = insert_ignore(fact_ad, "fact_ad", ["ad_event_id"])
    print(f"  fact_ad: {n} new rows (of {len(ads)} silver rows; duplicates skipped)")

    # -------- fact_subscription --------
    fs = subs.merge(user_lookup[["user_id", "user_key"]], on="user_id", how="inner")
    fs["date_only"] = fs["start_date"].dt.normalize()
    fs = fs.merge(date_keys.rename(columns={"date_key": "start_date_key"}),
                  left_on="date_only", right_on="full_date", how="left")
    fact_subscription = fs[[
        "subscription_id", "user_key", "start_date_key", "plan", "status", "source_file"
    ]]
    n = insert_ignore(fact_subscription, "fact_subscription", ["subscription_id"])
    print(f"  fact_subscription: {n} new rows (of {len(subs)} silver rows; duplicates skipped)")

    # -------- fact_support --------
    fsu = support.merge(user_lookup[["user_id", "user_key"]], on="user_id", how="inner")
    fsu["date_only"] = fsu["created_at"].dt.normalize()
    fsu = fsu.merge(date_keys, left_on="date_only", right_on="full_date", how="inner")
    fact_support = fsu[["ticket_id", "user_key", "date_key", "created_at", "issue_type", "source_file"]]
    n = insert_ignore(fact_support, "fact_support", ["ticket_id"])
    print(f"  fact_support: {n} new rows (of {len(support)} silver rows; duplicates skipped)")

    print("\nDone. fact_engagement is left empty here - it's populated by the dbt model, "
          "since it's a daily aggregate derived FROM fact_view, not raw source data.")


if __name__ == "__main__":
    main()
