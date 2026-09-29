# Model cards (brief section 9)

All models read the dbt marts / warehouse and write their output to tables that the API and the decision engine use.
Code: `ml/`. Re-run: `bash scripts/run_all.sh refresh`.

## 1. Churn-risk scoring - `ml/churn_risk.py`
* **Business objective:** flag subscribers likely to cancel so retention can act (retention campaign alert).
* **Label:** `churn = 1` if the user's latest subscription status is `cancelled` or `expired` (proxy label).
* **Features:** lifetime watch seconds, session count, average completion rate, days since last active (recency), plan, region, device. Status itself is *not* a feature (no leakage).
* **Train/validation/test:** 80/20 stratified split (`random_state=42`); logistic regression with standardised numerics, one-hot categoricals, `class_weight=balanced`.
* **Threshold selection:** the probability threshold that maximises F1 on the held-out set (0.286).
* **Results (923 users, 45.4 % churn rate):** accuracy 0.454, precision 0.454, recall 1.000, F1 0.625, **ROC-AUC 0.530**.
* **Honest interpretation:** AUC ~0.5 means the model has essentially **no predictive signal**, and the F1-optimal threshold degenerates to "flag everybody" (precision = base rate). This is the correct finding for this dataset: the source data is synthetic and subscription status was generated independently of viewing behaviour. The pipeline (features -> model -> scores -> tiers -> alerts -> API -> UI) is fully functional and would improve with real data; with a real signal I would (a) use a precision-constrained or top-k threshold, (b) validate out-of-time, (c) calibrate probabilities.
* **Output:** `churn_risk_scores` (probability, tier low <0.33 / medium / high >=0.66, features JSON, model version).
* **Limitations:** proxy label (cancelled/expired may not equal voluntary churn); small feature set; trained and scored on the same window; ties/plan effects untested.

## 2. Engagement / ad / content anomaly detection - `ml/detect_anomalies.py`
* **Business objective:** catch sudden drops in watch time (content alert), abnormal CTR/fill (monetisation alert) and view surges (editorial notification).
* **Method:** rolling z-score against the mean/std of the previous 7 days; |z|>2 = warning, |z|>3 = critical. Drops only for engagement, surges only for content.
* **Why this method:** transparent, cheap, explainable to non-technical owners; no training data required.
* **Results on this data:** 32 engagement drops (10 critical), 438 ad anomalies, 295 content surges.
* **Evaluation:** no labelled anomalies exist, so evaluation is by inspection of flagged days and by the alert volume; thresholds (2/3 sigma) are the standard control-chart choice. A labelled set would allow precision/recall tuning.
* **Limitations:** needs 7 days of history; a genuine trend change looks like repeated anomalies; small per-campaign daily counts make CTR/fill noisy (many ad anomalies); no seasonality handling.

## 3. Watch-time forecast - `ml/forecast_watch_time.py`
* **Business objective:** expected total daily watch time for the next 7 days (capacity planning; compare actual vs expected).
* **Method:** linear regression of daily watch seconds on day index, band = +/- 1 std of in-sample residuals.
* **Result:** slope -1.1 s/day (essentially flat), residual std ~14.7 k s.
* **Evaluation:** in-sample residual spread only (no holdout with this short, flat, noisy series).
* **Limitations:** assumes linear trend, no weekday seasonality, band is not a true prediction interval. An honest baseline - next steps would be Holt-Winters/Prophet with a backtest.

## 4. Content recommendation signals
* **Signals produced:** per-title total views, watch seconds, unique viewers, completions (`mart_content_performance`), completion rate by genre (`mart_completion_rate`), and surge detection (`content_trend_anomalies`). These are the inputs a recommender or an editorial team would use ("what is trending, what holds attention").
* **Bias note (interview Q "avoid recommendation bias"):** popularity signals favour already-popular titles; mitigate with exposure caps, normalising by exposure (completion rate rather than raw views), an exploration share, and monitoring genre/language share of recommendations.
