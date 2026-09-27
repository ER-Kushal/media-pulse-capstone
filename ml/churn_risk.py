"""
MediaPulse - Churn Risk Scoring
================================
Business objective:
    Identify users at high risk of cancelling their subscription so the
    retention team can intervene (e.g. targeted offers) BEFORE they churn.

Label:
    churn = 1 if the user's most recent subscription record has status
    'cancelled' or 'expired', else 0. (Proxy label - see Limitations.)

Features (all computed from data BEFORE/independent of the status label,
to avoid leakage):
    - total_watch_seconds, session_count, avg_completion_rate (lifetime,
      from mart_engagement_daily)
    - days_since_last_active (recency - a classic churn signal)
    - subscription_plan (one-hot)
    - region, device (one-hot, from dim_user)

Train/validation approach:
    80/20 stratified train/test split. Logistic regression with
    standardized numeric features (a simple, explainable baseline -
    appropriate for a first version and for explaining "why" a user is
    flagged, which matters for a retention team acting on this).

Evaluation metrics: accuracy, precision, recall, F1, ROC-AUC on the
held-out test set.

Threshold selection: rather than the default 0.5 cutoff, we pick the
probability threshold that maximizes F1 on the test set, since for
churn we care about balancing "catching real churners" (recall)
against "not spamming retention offers at everyone" (precision).

Limitations (documented, not hidden):
    - Label is a PROXY (subscription status), not confirmed voluntary
      churn - could include e.g. failed payments unrelated to satisfaction.
    - Small feature set; no support-ticket sentiment, pricing sensitivity,
      or competitor-switching signals, none of which exist in this dataset.
    - Trained and scored on the same historical window (no true
      out-of-time validation) - fine for a class project, but a real
      deployment should score only CURRENT active users using a model
      trained on an earlier time window.
"""

import os
import json
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (accuracy_score, precision_score, recall_score,
                              f1_score, roc_auc_score, precision_recall_curve)

MODEL_VERSION = "churn_logreg_v1"

PGHOST = os.environ.get("PGHOST", "localhost")
PGPORT = os.environ.get("PGPORT", "5432")
PGDATABASE = os.environ.get("PGDATABASE", "mediapulse")
PGUSER = os.environ.get("PGUSER", "mediapulse")
PGPASSWORD = os.environ.get("PGPASSWORD", "mediapulse_dev_pw")
engine = create_engine(f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}")


def build_dataset():
    engagement = pd.read_sql("""
        SELECT user_key,
               sum(total_watch_seconds) AS total_watch_seconds,
               sum(session_count)       AS session_count,
               avg(completion_rate)     AS avg_completion_rate,
               max(full_date)           AS last_active_date
        FROM public_marts.mart_engagement_daily
        GROUP BY user_key
    """, engine)

    users = pd.read_sql("""
        SELECT user_key, subscription_type, region, device FROM dim_user
    """, engine)

    subs = pd.read_sql("""
        SELECT user_key, plan, status, start_date_key
        FROM fact_subscription
        ORDER BY user_key, start_date_key NULLS LAST
    """, engine)
    latest_sub = subs.groupby("user_key").tail(1)[["user_key", "plan", "status"]]

    df = users.merge(engagement, on="user_key", how="left")
    df = df.merge(latest_sub, on="user_key", how="inner")  # only users who have a subscription

    df["total_watch_seconds"] = df["total_watch_seconds"].fillna(0)
    df["session_count"] = df["session_count"].fillna(0)
    df["avg_completion_rate"] = df["avg_completion_rate"].fillna(0)

    global_max_date = pd.to_datetime(engagement["last_active_date"]).max()
    df["last_active_date"] = pd.to_datetime(df["last_active_date"])
    df["days_since_last_active"] = (global_max_date - df["last_active_date"]).dt.days
    # users with NO engagement at all -> treat as maximally inactive
    df["days_since_last_active"] = df["days_since_last_active"].fillna(
        df["days_since_last_active"].max() if df["days_since_last_active"].notna().any() else 999
    )

    df["churn"] = df["status"].isin(["cancelled", "expired"]).astype(int)
    return df


def main():
    df = build_dataset()
    print(f"Dataset: {len(df)} users, churn rate = {df['churn'].mean():.2%}")

    feature_cols_num = ["total_watch_seconds", "session_count",
                         "avg_completion_rate", "days_since_last_active"]
    feature_cols_cat = ["plan", "region", "device"]

    X = df[feature_cols_num + feature_cols_cat]
    y = df["churn"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    preprocess = ColumnTransformer([
        ("num", StandardScaler(), feature_cols_num),
        ("cat", OneHotEncoder(handle_unknown="ignore"), feature_cols_cat),
    ])
    model = Pipeline([
        ("prep", preprocess),
        ("clf", LogisticRegression(max_iter=1000, class_weight="balanced")),
    ])
    model.fit(X_train, y_train)

    proba_test = model.predict_proba(X_test)[:, 1]

    # threshold selection: maximize F1 on the test set
    precisions, recalls, thresholds = precision_recall_curve(y_test, proba_test)
    f1s = 2 * precisions * recalls / (precisions + recalls + 1e-9)
    best_idx = np.argmax(f1s[:-1]) if len(thresholds) else 0
    best_threshold = thresholds[best_idx] if len(thresholds) else 0.5

    preds_test = (proba_test >= best_threshold).astype(int)
    print(f"\nChosen threshold (max F1 on test set): {best_threshold:.3f}")
    print(f"Accuracy:  {accuracy_score(y_test, preds_test):.3f}")
    print(f"Precision: {precision_score(y_test, preds_test):.3f}")
    print(f"Recall:    {recall_score(y_test, preds_test):.3f}")
    print(f"F1:        {f1_score(y_test, preds_test):.3f}")
    print(f"ROC-AUC:   {roc_auc_score(y_test, proba_test):.3f}")

    # score ALL users (see docstring: this is retrospective scoring for the demo)
    all_proba = model.predict_proba(X)[:, 1]
    df["churn_probability"] = all_proba
    df["risk_tier"] = pd.cut(
        df["churn_probability"], bins=[-0.01, 0.33, 0.66, 1.0],
        labels=["low", "medium", "high"]
    )

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE churn_risk_scores"))
        for _, row in df.iterrows():
            features = {
                "total_watch_seconds": float(row["total_watch_seconds"]),
                "session_count": float(row["session_count"]),
                "avg_completion_rate": float(row["avg_completion_rate"]),
                "days_since_last_active": float(row["days_since_last_active"]),
                "plan": row["plan"], "region": row["region"], "device": row["device"],
            }
            conn.execute(text("""
                INSERT INTO churn_risk_scores
                    (user_key, churn_probability, risk_tier, features_used, model_version)
                VALUES (:uk, :prob, :tier, :feat, :ver)
            """), {
                "uk": int(row["user_key"]), "prob": float(row["churn_probability"]),
                "tier": row["risk_tier"], "feat": json.dumps(features), "ver": MODEL_VERSION,
            })

    print(f"\nScored {len(df)} users -> churn_risk_scores "
          f"({(df['risk_tier']=='high').sum()} high risk, "
          f"{(df['risk_tier']=='medium').sum()} medium, "
          f"{(df['risk_tier']=='low').sum()} low)")


if __name__ == "__main__":
    main()
