"""
MediaPulse - Anomaly Detection
================================
Method: rolling z-score. For each day, compare the actual metric value
to the mean/stddev of the preceding 7 days. Flag as:
    - 'warning'  if |z| > 2
    - 'critical' if |z| > 3
This is a standard, explainable anomaly-detection baseline - easy to
justify in an interview ("why 2/3 sigma?") versus a black-box model,
which matters more here than marginal accuracy gains.

Limitations: needs >= 7 prior days of history to establish a baseline
(the first week of any date range won't be checked); a rolling mean
is sensitive to genuine trend changes (e.g. real steady growth can look
like a string of "anomalies") - a production version would use
seasonal decomposition instead.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text

PGHOST = os.environ.get("PGHOST", "localhost")
PGPORT = os.environ.get("PGPORT", "5432")
PGDATABASE = os.environ.get("PGDATABASE", "mediapulse")
PGUSER = os.environ.get("PGUSER", "mediapulse")
PGPASSWORD = os.environ.get("PGPASSWORD", "mediapulse_dev_pw")
engine = create_engine(f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}")

WINDOW = 7


def rolling_zscore(series):
    roll_mean = series.rolling(WINDOW).mean().shift(1)
    roll_std = series.rolling(WINDOW).std().shift(1)
    z = (series - roll_mean) / roll_std.replace(0, pd.NA)
    return z, roll_mean


def severity(z):
    az = abs(z)
    if az > 3:
        return "critical"
    if az > 2:
        return "warning"
    return None


def detect_engagement_drops():
    df = pd.read_sql("""
        SELECT full_date, sum(total_watch_seconds) AS total_watch_seconds
        FROM public_marts.mart_watch_time
        GROUP BY full_date ORDER BY full_date
    """, engine)
    df["z"], df["expected"] = rolling_zscore(df["total_watch_seconds"])
    df["sev"] = df["z"].apply(lambda z: severity(z) if pd.notna(z) else None)
    # only interested in DROPS (negative z) for "engagement drop" alerts
    flagged = df[(df["sev"].notna()) & (df["z"] < 0)]

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE engagement_anomalies"))
        for _, r in flagged.iterrows():
            conn.execute(text("""
                INSERT INTO engagement_anomalies
                    (detected_date, metric, actual_value, expected_value, z_score, severity)
                VALUES (:d, 'total_watch_seconds', :a, :e, :z, :s)
            """), {"d": r["full_date"], "a": float(r["total_watch_seconds"]),
                    "e": float(r["expected"]) if pd.notna(r["expected"]) else None,
                    "z": float(r["z"]), "s": r["sev"]})
    print(f"engagement_anomalies: {len(flagged)} drop(s) flagged")
    return flagged


def detect_ad_anomalies():
    df = pd.read_sql("""
        SELECT full_date, campaign_id, ctr, ad_fill_rate
        FROM public_marts.mart_ad_performance
        ORDER BY campaign_id, full_date
    """, engine)
    all_flagged = []
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE ad_anomalies"))
        for campaign_id, grp in df.groupby("campaign_id"):
            grp = grp.sort_values("full_date")
            for metric in ["ctr", "ad_fill_rate"]:
                z, expected = rolling_zscore(grp[metric])
                sev = z.apply(lambda v: severity(v) if pd.notna(v) else None)
                flagged = grp.assign(z=z, expected=expected, sev=sev)
                flagged = flagged[flagged["sev"].notna()]
                for _, r in flagged.iterrows():
                    conn.execute(text("""
                        INSERT INTO ad_anomalies
                            (detected_date, campaign_id, metric, actual_value,
                             expected_value, z_score, severity)
                        VALUES (:d, :c, :m, :a, :e, :z, :s)
                    """), {"d": r["full_date"], "c": campaign_id, "m": metric,
                            "a": float(r[metric]) if pd.notna(r[metric]) else None,
                            "e": float(r["expected"]) if pd.notna(r["expected"]) else None,
                            "z": float(r["z"]), "s": r["sev"]})
                    all_flagged.append(r)
    print(f"ad_anomalies: {len(all_flagged)} flagged")
    return all_flagged


def detect_content_trend_surges():
    df = pd.read_sql("""
        SELECT d.full_date, fv.content_key, c.content_id, count(*) AS view_count
        FROM fact_view fv
        JOIN dim_date d ON fv.date_key = d.date_key
        JOIN dim_content c ON fv.content_key = c.content_key
        GROUP BY 1, 2, 3
        ORDER BY c.content_id, d.full_date
    """, engine)
    flagged_rows = []
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE content_trend_anomalies"))
        for content_id, grp in df.groupby("content_id"):
            grp = grp.sort_values("full_date")
            z, expected = rolling_zscore(grp["view_count"])
            sev = z.apply(lambda v: severity(v) if pd.notna(v) else None)
            flagged = grp.assign(z=z, expected=expected, sev=sev)
            flagged = flagged[(flagged["sev"].notna()) & (flagged["z"] > 0)]  # surges only
            for _, r in flagged.iterrows():
                conn.execute(text("""
                    INSERT INTO content_trend_anomalies
                        (detected_date, content_id, view_count, expected_views, z_score, trend_type)
                    VALUES (:d, :c, :v, :e, :z, 'surge')
                """), {"d": r["full_date"], "c": content_id, "v": int(r["view_count"]),
                        "e": float(r["expected"]) if pd.notna(r["expected"]) else None,
                        "z": float(r["z"])})
                flagged_rows.append(r)
    print(f"content_trend_anomalies: {len(flagged_rows)} surge(s) flagged")
    return flagged_rows


if __name__ == "__main__":
    detect_engagement_drops()
    detect_ad_anomalies()
    detect_content_trend_surges()
