"""
MediaPulse - Load Silver CSVs into the Postgres warehouse (star schema)
========================================================================
Run this AFTER bronze_to_silver.py has produced data_silver/*.csv,
and AFTER schema.sql has been applied to your Postgres database.

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

    views["event_timestamp"] = pd.to_datetime(views["timestamp"])
    ads["event_timestamp"] = pd.to_datetime(ads["timestamp"])
    subs["start_date"] = pd.to_datetime(subs["start_date"])
    content["release_date"] = pd.to_datetime(content["release_date"])

    # -------- dim_date: built from every date appearing anywhere --------
    all_dates = pd.concat([
        views["event_timestamp"], ads["event_timestamp"], subs["start_date"]
    ])
    dim_date = build_date_dim(all_dates)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE dim_date CASCADE"))
    dim_date.to_sql("dim_date", engine, if_exists="append", index=False)
    print(f"  dim_date: {len(dim_date)} rows")

    # -------- dim_user --------
    dim_user = users.rename(columns={
        "subscription_type": "subscription_type", "region": "region", "device": "device"
    })[["user_id", "subscription_type", "region", "device", "source_file"]]
    dim_user.to_sql("dim_user", engine, if_exists="append", index=False)
    print(f"  dim_user: {len(dim_user)} rows")

    # -------- dim_content --------
    dim_content = content.rename(columns={"duration": "duration_min"})[
        ["content_id", "genre", "language", "release_date", "duration_min", "source_file"]
    ]
    dim_content.to_sql("dim_content", engine, if_exists="append", index=False)
    print(f"  dim_content: {len(dim_content)} rows")

    # -------- dim_device / dim_geography / dim_campaign --------
    dim_device = pd.DataFrame({"device_name": users["device"].dropna().unique()})
    dim_device.to_sql("dim_device", engine, if_exists="append", index=False)
    print(f"  dim_device: {len(dim_device)} rows")

    dim_geo = pd.DataFrame({"region": users["region"].dropna().unique()})
    dim_geo.to_sql("dim_geography", engine, if_exists="append", index=False)
    print(f"  dim_geography: {len(dim_geo)} rows")

    dim_campaign = pd.DataFrame({"campaign_id": ads["campaign_id"].dropna().unique()})
    dim_campaign.to_sql("dim_campaign", engine, if_exists="append", index=False)
    print(f"  dim_campaign: {len(dim_campaign)} rows")

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
    fact_view.to_sql("fact_view", engine, if_exists="append", index=False)
    print(f"  fact_view: {len(fact_view)} rows loaded (of {len(views)} silver rows)")

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
    fact_ad.to_sql("fact_ad", engine, if_exists="append", index=False)
    print(f"  fact_ad: {len(fact_ad)} rows loaded (of {len(ads)} silver rows)")

    # -------- fact_subscription --------
    fs = subs.merge(user_lookup[["user_id", "user_key"]], on="user_id", how="inner")
    fs["date_only"] = fs["start_date"].dt.normalize()
    fs = fs.merge(date_keys.rename(columns={"date_key": "start_date_key"}),
                  left_on="date_only", right_on="full_date", how="left")
    fact_subscription = fs[[
        "subscription_id", "user_key", "start_date_key", "plan", "status", "source_file"
    ]]
    fact_subscription.to_sql("fact_subscription", engine, if_exists="append", index=False)
    print(f"  fact_subscription: {len(fact_subscription)} rows loaded (of {len(subs)} silver rows)")

    print("\nDone. fact_engagement is left empty here - it's populated by the dbt model, "
          "since it's a daily aggregate derived FROM fact_view, not raw source data.")


if __name__ == "__main__":
    main()
