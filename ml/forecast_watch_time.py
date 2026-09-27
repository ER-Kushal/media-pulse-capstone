"""
MediaPulse - Audience/Watch-Time Forecasting
==============================================
Business objective: forecast total daily watch time for the next 7 days
so the business can anticipate capacity needs and spot expected vs.
actual gaps early.

Method: linear regression on (day_index -> total_watch_seconds), i.e. a
simple trend line, plus a naive +/- 1 standard-deviation-of-residuals
band as a rough confidence interval.

This is intentionally simple (a straight trend line, not ARIMA/Prophet):
with well under a year of daily history and no obvious seasonality
established yet, a simple trend is both honest about what the data
supports and easy to explain. Documented explicitly as a limitation
below rather than overclaiming sophistication.

Limitations:
    - Assumes a linear trend continues; won't catch seasonality
      (e.g. weekday vs weekend effects) or sudden shifts.
    - Confidence band is a simple +/-1 std of in-sample residuals, not a
      proper prediction interval.
    - Should be refit regularly (e.g. daily/weekly) as more data arrives.
"""

import os
import numpy as np
import pandas as pd
from datetime import timedelta
from sqlalchemy import create_engine, text
from sklearn.linear_model import LinearRegression

MODEL_VERSION = "watch_time_linreg_v1"
FORECAST_DAYS = 7

PGHOST = os.environ.get("PGHOST", "localhost")
PGPORT = os.environ.get("PGPORT", "5432")
PGDATABASE = os.environ.get("PGDATABASE", "mediapulse")
PGUSER = os.environ.get("PGUSER", "mediapulse")
PGPASSWORD = os.environ.get("PGPASSWORD", "mediapulse_dev_pw")
engine = create_engine(f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}")


def main():
    df = pd.read_sql("""
        SELECT full_date, sum(total_watch_seconds) AS total_watch_seconds
        FROM public_marts.mart_watch_time
        GROUP BY full_date ORDER BY full_date
    """, engine)
    df["full_date"] = pd.to_datetime(df["full_date"])
    df["day_index"] = (df["full_date"] - df["full_date"].min()).dt.days

    X = df[["day_index"]].values
    y = df["total_watch_seconds"].values
    model = LinearRegression().fit(X, y)

    residuals = y - model.predict(X)
    resid_std = residuals.std()

    last_index = df["day_index"].max()
    last_date = df["full_date"].max()
    future_index = np.arange(last_index + 1, last_index + 1 + FORECAST_DAYS).reshape(-1, 1)
    future_dates = [last_date + timedelta(days=i) for i in range(1, FORECAST_DAYS + 1)]
    preds = model.predict(future_index)

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE watch_time_forecast"))
        for date, pred in zip(future_dates, preds):
            conn.execute(text("""
                INSERT INTO watch_time_forecast
                    (forecast_date, predicted_watch_seconds, lower_bound, upper_bound, model_version)
                VALUES (:d, :p, :lo, :hi, :v)
            """), {"d": date.date(), "p": float(pred),
                    "lo": float(pred - resid_std), "hi": float(pred + resid_std),
                    "v": MODEL_VERSION})

    print(f"Forecast written for {FORECAST_DAYS} days starting {future_dates[0].date()}")
    print(f"Trend slope: {model.coef_[0]:.1f} watch-seconds/day, "
          f"residual std: {resid_std:.1f}")


if __name__ == "__main__":
    main()
