# Test cases

## Automated - API (`cd api && pytest -v`, 20 tests, also run in CI)
| # | Test | Verifies |
|---|---|---|
| 1 | `test_health_is_public` | /health works without login and reports DB up |
| 2 | `test_login_wrong_password_rejected` | bad credentials -> 401 |
| 3 | `test_protected_endpoint_requires_token` | no token -> 401 |
| 4 | `test_bad_token_rejected` | tampered token -> 401 |
| 5-6 | `test_audience_live`, `..._validation` | live metrics; `minutes` outside 1-1440 -> 422 |
| 7-8 | `test_content_performance`, `..._not_found_and_bad_id` | drill-down data; unknown id -> 404; malformed id -> 422 |
| 9 | `test_subscriber_health` | Subscriber 360 payload, health label; unknown user -> 404 |
| 10 | `test_churn_risk_filter_and_pagination` | tier filter, limit; invalid tier -> 422 |
| 11 | `test_campaigns` | campaign KPIs |
| 12 | `test_dashboard` | executive KPIs and trends present |
| 13 | `test_events_live` | live feed limit honoured |
| 14 | `test_alerts_list_and_filters` | filters; invalid status -> 422 |
| 15 | `test_forecasts` | 7 forecast rows |
| 16 | `test_risk` | risk summary level |
| 17 | `test_data_quality` | >= 8 checks; duplicate + click/impression checks must pass |
| 18 | `test_acknowledge_requires_privileged_role` | viewer -> 403, anonymous -> 401 (RBAC) |
| 19 | `test_acknowledge_flow_and_audit_log` | status, owner, audit entry written; 2nd ack -> 409 |
| 20 | `test_acknowledge_unknown_alert` | unknown alert -> 404 |

## Automated - data (`dbt test`, 13 tests, part of `run_all.sh` and CI)
not_null on 9 mart columns, unique on `full_date` (x2) and `content_id`, plus singular test `assert_rates_within_0_and_1` (fill rate, CTR, completion rate within 0-1).

## Automated - end to end (CI)
`bash scripts/run_all.sh full` on an empty database: Silver cleaning -> warehouse load -> dbt run (16 models) -> dbt test -> churn/anomaly/forecast -> alert engine. Then `refresh` mode idempotency was verified manually: second run loads 0 new rows and creates 0 new alerts.

## Airflow (verified with `airflow dags test mediapulse_daily_pipeline`)
All 9 tasks succeed in dependency order; 9 rows appear in `pipeline_run_log`; failure/retry callbacks write `failed`/`retry` rows and do not crash without a Slack webhook.

## Frontend (headless render test against the live API - 18 checks)
Login screen; wrong password message; successful login; Command Center KPIs, feed and 4 charts; Content list + drill-down; Monetization; Subscriber 360 list + drill-down; Alerts list, filters, acknowledge (admin) and read-only behaviour (viewer); Data Quality; no console errors.

## Manual - streaming test (Kafka)
1. `docker compose up -d`; start `python kafka/consumer.py` in terminal 1, `python kafka/producer.py --rate 3` in terminal 2.
2. Kafka UI (localhost:8081) -> topics `mediapulse.views` / `mediapulse.ads` show growing message counts.
3. Consumer prints `[monitor] processed=N ...`.
4. pgAdmin/psql: `SELECT * FROM fact_view WHERE source_file='kafka_stream' ORDER BY view_key DESC LIMIT 5;` returns the new rows.
5. Duplicate test: stop the consumer, run it again - row counts do not double (`ON CONFLICT DO NOTHING`, offsets committed after DB write).
6. Dashboard *Command Center* -> Live audience shows a green "Kafka stream is feeding live data" and the live feed lists the new events.

## Acceptance (brief section 22) - demo script
| Step | Evidence |
|---|---|
| Event enters the system | producer console + Kafka UI |
| Observe it processed | consumer `[monitor]` lines |
| Warehouse representation | `fact_view` row with `source_file='kafka_stream'` in pgAdmin |
| Insight / model result | `bash scripts/run_all.sh refresh` -> marts, anomaly, churn tables update; dashboard KPIs |
| Automated business action | decision engine creates alert (+ Slack message if webhook set) |
| Visible in deployed app | Render URL -> Engagement Alerts -> acknowledge -> audit log entry |
