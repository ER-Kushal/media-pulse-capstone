# Data Quality, Observability & Recovery (brief section 14)

## Checks implemented (and where)

| Category | Check | Where it runs |
|---|---|---|
| Null | Key columns present; `watch_seconds` null % under 10 % | Silver cleaning (drops rows with no key), dbt `not_null` tests (9), `/api/data-quality` |
| Duplicate | Duplicate business keys removed; `event_id` duplicates = 0 | Silver cleaning (`drop_duplicates`), `UNIQUE` constraints, `ON CONFLICT DO NOTHING`, API check |
| Uniqueness | `dim_user.user_id`, `content_id`, `full_date` unique | dbt `unique` tests (3), UNIQUE constraints, API check |
| Referential integrity | Orphan `user_id`/`content_id` rows dropped in Silver; FKs enforced in the warehouse | Silver cleaning, FOREIGN KEY constraints, API orphan checks |
| Valid range | `watch_seconds` in 0-21600, `duration` > 0, binary flags 0/1, rates within 0-1 | Silver cleaning, dbt singular test `assert_rates_within_0_and_1`, API check |
| Business rule | A click implies an impression | Silver cleaning (1,297 rows repaired), API check (found a real bug during development - see below) |
| Freshness | Newest view event <= 24 h old | `/api/data-quality` (warn when stale; goes green while Kafka streams) |
| Volume | `fact_view` rows >= 1000 | `/api/data-quality` |
| Pipeline success/failure | Every Airflow task logs success/retry/failed; failures alert Slack | `pipeline_run_log`, Airflow callbacks, `/api/data-quality`, dashboard "Data Quality" page |
| Streaming | Events that still fail after 4 retries go to `kafka/dead_letter_events.jsonl`; consumer prints `[monitor]` lines | `kafka/consumer.py`, API check "dead-letter events" |
| Audit / app logging | Alerts have JSON audit trails; API logs each request (method, path, status, ms, IP) to `api/logs/api.log` | `alerts.audit_log`, `api/app/core.py` |

### A real defect the checks caught
While building the API, the data-quality endpoint reported 20 ad rows with `click = 1` but `impression` null. The Silver rule "click implies impression" had missed them because pandas treats comparisons with null as *unknown* rather than true. Fix: fill nulls before comparing (`bronze_to_silver.py`); the count repaired went from 1,277 to 1,297 and the check now passes. Good example of why validation belongs in more than one layer.

## Recovery procedure for a failed pipeline

Every step is **idempotent** (incremental loads with `ON CONFLICT DO NOTHING`, alert de-duplication, dbt rebuilds tables), so re-running is always safe.

1. **Detect** - Airflow UI (red task), Slack failure message, or dashboard *Data Quality* page / `GET /api/data-quality` (`recent_pipeline_runs`, `overall_status`).
2. **Diagnose** - open the failed task's log (Airflow UI -> task -> Logs) or run the step by hand: `bash scripts/run_all.sh refresh` prints which step fails.
3. **Fix the cause**

| Symptom | Likely cause | Fix |
|---|---|---|
| `connection refused` | Postgres container down | `docker compose up -d`, check `docker ps` |
| `password authentication failed` | wrong `PG*` variables | re-export the variables (see FINISH_GUIDE) |
| `dbt test` fails | new bad data reached the warehouse | read the failing test, fix the rule in `bronze_to_silver.py`, re-run |
| `foreign key violation` in load | dimension missing for a fact | re-run load (dimensions load first) or fix source |
| Consumer keeps retrying | DB/schema problem | fix DB; offsets were not committed so nothing is lost; inspect `dead_letter_events.jsonl` |

4. **Re-run** - Airflow: *Clear* the failed task (downstream tasks follow); or `bash scripts/run_all.sh refresh`.
5. **Last resort - full rebuild from Bronze** (Bronze is immutable and Silver is recomputable): `bash scripts/run_all.sh full`. Note: this drops Kafka-streamed rows and alert history; replay the producer if you need them.
6. **Verify** - `dbt test` all PASS, `GET /api/data-quality` shows `pass`, dashboard KPIs load.
7. **Record** - the failure and the fix are already in `pipeline_run_log`; add a note to the incident log if this were production.
