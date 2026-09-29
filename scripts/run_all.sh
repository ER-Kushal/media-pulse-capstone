#!/usr/bin/env bash
# One command to rebuild / refresh the WHOLE batch pipeline:
#   raw CSV -> Silver -> warehouse -> dbt marts + tests -> ML models -> alerts
#
#   ./scripts/run_all.sh full      first time, or to rebuild from scratch (drops + recreates tables)
#   ./scripts/run_all.sh refresh   incremental: keeps existing rows (Kafka data, alert history)
set -euo pipefail
cd "$(dirname "$0")/.."
MODE="${1:-full}"
PY="${PYTHON:-python}"
export PGHOST="${PGHOST:-localhost}" PGPORT="${PGPORT:-5432}" PGUSER="${PGUSER:-mediapulse}"
export PGPASSWORD="${PGPASSWORD:-mediapulse_dev_pw}" PGDATABASE="${PGDATABASE:-mediapulse}"
step() { echo; echo "=== $* ==="; }
dbt_cmd() { "$PY" -c "from dbt.cli.main import cli; cli()" "$@"; }   # avoids the 'dbt not on PATH' problem

step "1/8 Bronze -> Silver cleaning";        "$PY" ingestion/bronze_to_silver.py | tail -n 12
if [ "$MODE" = "full" ]; then
  step "2/8 Create warehouse tables (full)"; "$PY" scripts/apply_sql.py dbt/schema.sql
  "$PY" scripts/apply_sql.py ml/ml_schema.sql
else
  step "2/8 Skipping table reset (refresh mode)"
fi
"$PY" scripts/apply_sql.py ops/ops_schema.sql
step "3/8 Load warehouse (incremental)";      "$PY" ingestion/load_warehouse.py
step "4/8 dbt run (analytical marts)";        (cd dbt && dbt_cmd run --profiles-dir . )
step "5/8 dbt test (data-quality tests)";     (cd dbt && dbt_cmd test --profiles-dir . )
step "6/8 ML: churn risk";                    "$PY" ml/churn_risk.py
step "7/8 ML: anomalies + forecast";          "$PY" ml/detect_anomalies.py; "$PY" ml/forecast_watch_time.py
step "8/8 Automation: alerts";                "$PY" automation/decision_engine.py
echo; echo "PIPELINE COMPLETE ($MODE)"
