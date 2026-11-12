"""Lab 05: teaching DAG — operators, dependencies, {{ ds }}, retries, XCom, alert callback."""
import os
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def _alert(context):
    """on_failure_callback: runs once a task has failed its LAST retry."""
    ti = context["task_instance"]
    print(f"[ALERT] task={ti.task_id} date={context['ds']} FAILED. Log: {ti.log_url}")


default_args = {
    "owner":               "data-engineering",
    "retries":             2,                      # 3 attempts in total
    "retry_delay":         timedelta(minutes=1),
    "execution_timeout":   timedelta(minutes=10),  # kill a task that hangs
    "on_failure_callback": _alert,
}


def _count_source_files(**context):
    """List the day's raw files. The RETURN value is stored as an XCom."""
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )
    prefix = f"raw/orders/dt={context['ds']}/"
    n = len(s3.list_objects_v2(Bucket="urbangear-data-lake", Prefix=prefix).get("Contents", []))
    print(f"[COUNT] {n} file(s) under {prefix}")
    return n


def _report(**context):
    """Read the upstream task's XCom."""
    n = context["ti"].xcom_pull(task_ids="count_source_files")
    print(f"[REPORT] date={context['ds']} files={n}")


with DAG(
    dag_id="urbangear_hello",
    description="Lab 05 teaching DAG",
    start_date=datetime(2026, 7, 1),
    schedule="@daily",
    catchup=False,                     # never create runs for the past automatically
    default_args=default_args,
    tags=["lab05"],
) as dag:

    start = EmptyOperator(task_id="start")      # does nothing; a clear visual start node

    check_minio = BashOperator(
        task_id="check_minio",
        bash_command="curl -sf http://minio:9000/minio/health/live && echo MINIO_OK",
    )
    print_date = BashOperator(
        task_id="print_logical_date",
        bash_command="echo 'processing data interval starting {{ ds }}'",
    )
    count_files = PythonOperator(task_id="count_source_files", python_callable=_count_source_files)
    report = PythonOperator(task_id="report", python_callable=_report)

    start >> [check_minio, print_date] >> count_files >> report
