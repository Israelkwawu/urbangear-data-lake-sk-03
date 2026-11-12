"""
UrbanGear Daily Sales Pipeline — Airflow DAG (local stack)
==========================================================
Orchestrates: Wait → Check → Spark ETL → Quality Gate
             → [Load warehouse | Alert failure] → dbt → Success alert

Design principles:
- Idempotent: safe to re-run for any date
- catchup=False: no accidental historical backfill
- max_active_runs=1: prevents overlapping daily runs
- Every task has retries and execution_timeout
"""

import json
import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

AWS_CONN_ID       = "aws_default"         # points at MinIO locally (Lab 06)
WAREHOUSE_CONN_ID = "warehouse_default"   # local Postgres warehouse (Lab 07)
S3_BUCKET         = "urbangear-data-lake"
MAX_REJECT_RATE   = 0.05

# ── Alerting ─────────────────────────────────────────────────────
def _send_alert(text: str) -> None:
    """Post to Slack if a webhook URL is configured; always log the alert."""
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if webhook:
        requests.post(webhook, json={"text": text}, timeout=10)
    print(f"[ALERT] {text}")

def _failure_alert(context):
    """Callback: fires when any task fails (after its last retry)."""
    ti = context["task_instance"]
    _send_alert(
        ":red_circle: *UrbanGear Pipeline FAILED*\n"
        f"*Task:* `{ti.task_id}`\n"
        f"*Date:* {context['ds']}\n"
        f"*Log:* {ti.log_url}"
    )

DEFAULT_ARGS = {
    "owner":               "data-engineering",
    "depends_on_past":     False,
    "email_on_failure":    False,    # needs SMTP configured; we alert via _failure_alert
    "email_on_retry":      False,
    "retries":             3,
    "retry_delay":         timedelta(minutes=10),
    "execution_timeout":   timedelta(hours=2),
    "on_failure_callback": _failure_alert,
}

# ── Python callables ─────────────────────────────────────────────
def _check_source_data(**context):
    """Count the day's source files and share the number via XCom."""
    process_date = context["ds"]
    hook   = S3Hook(aws_conn_id=AWS_CONN_ID)
    prefix = f"raw/orders/dt={process_date}/"
    keys   = hook.list_keys(bucket_name=S3_BUCKET, prefix=prefix)

    if not keys:
        raise FileNotFoundError(
            f"No source files at s3://{S3_BUCKET}/{prefix}. "
            "Source system export may not have completed."
        )

    context["ti"].xcom_push(key="source_file_count", value=len(keys))
    print(f"[CHECK] {len(keys)} source files found: {keys}")
    return len(keys)

# ── DAG ──────────────────────────────────────────────────────────
with DAG(
    dag_id          = "urbangear_daily_sales_pipeline",
    default_args    = DEFAULT_ARGS,
    description     = "Daily sales: raw JSON → PySpark → warehouse → dbt → alert",
    schedule        = "0 4 * * *",          # 4:00 AM UTC daily
    start_date      = datetime(2026, 7, 1),
    catchup         = False,                # Never backfill silently
    max_active_runs = 1,                    # Prevent day overlap
    tags            = ["production", "sales", "daily"],
) as dag:

    # ── 1. Sensor: wait up to 1 hour for the day's files ────────
    # mode=reschedule frees the worker slot while waiting.
    wait_for_files = S3KeySensor(
        task_id       = "wait_for_source_files",
        bucket_key    = "raw/orders/dt={{ ds }}/part-*.json",
        wildcard_match = True,                # bucket_key contains a * pattern
        bucket_name   = S3_BUCKET,
        aws_conn_id   = AWS_CONN_ID,
        poke_interval = 300,    # check every 5 minutes
        timeout       = 3600,   # fail if not found within 1 hour
        mode          = "reschedule",
        retries       = 0,      # a sensor timeout is real news — alert, don't retry
    )

    # ── 2. Check source data (count files → XCom) ────────────────
    check_source = PythonOperator(
        task_id         = "check_source_data",
        python_callable = _check_source_data,
    )

    # ── 3. Spark ETL (local mode inside the Airflow container) ───
    run_spark_etl = BashOperator(
        task_id      = "run_spark_etl",
        bash_command = (
            "spark-submit --master 'local[2]' --driver-memory 2g "
            "/opt/airflow/jobs/etl_job.py --process_date {{ ds }}"
        ),
        execution_timeout = timedelta(hours=3),
    )

    # ── Task dependency graph ────────────────────────────────────
    wait_for_files >> check_source >> run_spark_etl
