"""MediaPulse daily pipeline (section 13): batch ingestion -> transformations -> quality checks
-> model refresh -> alert workflow.

  clean_bronze_to_silver -> load_warehouse -> dbt_run -> dbt_test (quality gate)
        -> [churn_risk, detect_anomalies, forecast_watch_time] -> decision_engine

Reliability features required by the brief:
  * retries (2, with delay) on every task
  * failure notification (Slack webhook if SLACK_WEBHOOK_URL is set) + log line
  * operational run log: every success / retry / failure is written to the
    pipeline_run_log table, which GET /api/data-quality and the dashboard display
  * max_active_runs=1 so two runs never overwrite each other
  * every step is idempotent (incremental loads, alert de-duplication), so a retry is always safe

Env vars: PROJECT_DIR (repo root), PIPELINE_PYTHON (interpreter that has the project's
requirements installed), PG* connection variables, optional SLACK_WEBHOOK_URL.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/project")
PY = os.environ.get("PIPELINE_PYTHON", "python")


def _write_log(status: str, context: dict, message: str = "") -> None:
    """Append to the operational run log; never let logging break the pipeline."""
    try:
        from sqlalchemy import create_engine, text
        url = "postgresql+psycopg2://{u}:{p}@{h}:{port}/{d}".format(
            u=os.environ.get("PGUSER", "mediapulse"), p=os.environ.get("PGPASSWORD", "mediapulse_dev_pw"),
            h=os.environ.get("PGHOST", "postgres"), port=os.environ.get("PGPORT", "5432"),
            d=os.environ.get("PGDATABASE", "mediapulse"))
        ti = context["task_instance"]
        with create_engine(url).begin() as conn:
            conn.execute(text("""INSERT INTO pipeline_run_log (dag_id, task_id, run_id, status, message)
                                 VALUES (:d, :t, :r, :s, :m)"""),
                         {"d": ti.dag_id, "t": ti.task_id, "r": context.get("run_id"), "s": status, "m": message[:1000]})
    except Exception as exc:  # noqa: BLE001
        print(f"[run-log] could not write log row: {exc}")


def on_success(context):
    _write_log("success", context)


def on_retry(context):
    _write_log("retry", context, str(context.get("exception")))


def on_failure(context):
    msg = f"Task {context['task_instance'].task_id} FAILED: {context.get('exception')}"
    _write_log("failed", context, msg)
    hook = os.environ.get("SLACK_WEBHOOK_URL")
    if hook:
        try:
            import requests
            requests.post(hook, json={"text": f":rotating_light: MediaPulse pipeline - {msg}"}, timeout=5)
        except Exception as exc:  # noqa: BLE001
            print(f"[failure-notify] Slack call failed: {exc}")
    print(f"[failure-notify] {msg}")


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_success_callback": on_success,
    "on_retry_callback": on_retry,
    "on_failure_callback": on_failure,
}

with DAG(
    dag_id="mediapulse_daily_pipeline",
    description="Bronze->Silver->Warehouse->dbt->ML->Alerts",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",          # every day at 02:00
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["mediapulse", "batch"],
) as dag:

    def sh(task_id: str, command: str) -> BashOperator:
        return BashOperator(task_id=task_id, bash_command=f"cd {PROJECT_DIR} && {command}")

    clean = sh("clean_bronze_to_silver", f"{PY} ingestion/bronze_to_silver.py")
    ensure_ops = sh("ensure_ops_tables", f"{PY} scripts/apply_sql.py ops/ops_schema.sql")
    load = sh("load_warehouse", f"{PY} ingestion/load_warehouse.py")
    dbt_run = sh("dbt_run", f'cd dbt && {PY} -c "from dbt.cli.main import cli; cli()" run --profiles-dir .')
    dbt_test = sh("dbt_test_quality_gate", f'cd dbt && {PY} -c "from dbt.cli.main import cli; cli()" test --profiles-dir .')
    churn = sh("ml_churn_risk", f"{PY} ml/churn_risk.py")
    anomalies = sh("ml_detect_anomalies", f"{PY} ml/detect_anomalies.py")
    forecast = sh("ml_forecast_watch_time", f"{PY} ml/forecast_watch_time.py")
    alerts = sh("automation_decision_engine", f"{PY} automation/decision_engine.py")

    ensure_ops >> clean >> load >> dbt_run >> dbt_test
    dbt_test >> [churn, anomalies, forecast]
    [churn, anomalies] >> alerts
