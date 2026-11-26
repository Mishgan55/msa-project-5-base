from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

DATA_FILE = Path(os.getenv("POC_SOURCE_FILE", "/opt/airflow/data/delivery_statuses.csv"))
OUTPUT_DIR = Path(os.getenv("POC_OUTPUT_DIR", "/opt/airflow/output"))
ROW_THRESHOLD = int(os.getenv("POC_ROW_THRESHOLD", "5"))
NOTIFY_TO = os.getenv("POC_NOTIFY_TO", "marketing-analytics@example.local")
REQUIRED_COLUMNS = {"order_id", "customer_id", "delivery_status", "updated_at"}


def read_source(**context):
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Source file does not exist: {DATA_FILE}")

    with DATA_FILE.open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        rows = list(reader)
        fieldnames = set(reader.fieldnames or [])

    missing_columns = sorted(REQUIRED_COLUMNS - fieldnames)
    invalid_rows = [
        index + 2
        for index, row in enumerate(rows)
        if any(not (row.get(column) or "").strip() for column in REQUIRED_COLUMNS)
    ]

    profile = {
        "input_file": str(DATA_FILE),
        "rows_total": len(rows),
        "missing_columns": missing_columns,
        "invalid_rows": invalid_rows,
        "read_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    context["ti"].xcom_push(key="source_profile", value=profile)
    print(json.dumps(profile, ensure_ascii=False, indent=2))


def choose_quality_path(**context):
    profile = context["ti"].xcom_pull(task_ids="read_source", key="source_profile")
    if profile["missing_columns"] or profile["invalid_rows"]:
        return "quarantine_source"
    return "analyze_data"


def analyze_data(**context):
    profile = context["ti"].xcom_pull(task_ids="read_source", key="source_profile")
    with DATA_FILE.open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source))

    by_status = {}
    for row in rows:
        status = row["delivery_status"].strip().lower()
        by_status[status] = by_status.get(status, 0) + 1

    analysis = {
        **profile,
        "row_threshold": ROW_THRESHOLD,
        "rows_by_status": dict(sorted(by_status.items())),
        "batch_size": "large" if len(rows) > ROW_THRESHOLD else "small",
    }
    context["ti"].xcom_push(key="analysis", value=analysis)
    print(json.dumps(analysis, ensure_ascii=False, indent=2))


def choose_volume_path(**context):
    analysis = context["ti"].xcom_pull(task_ids="analyze_data", key="analysis")
    if analysis["rows_total"] > analysis["row_threshold"]:
        return "process_large_batch"
    return "process_small_batch"


def process_batch(batch_mode, **context):
    analysis = context["ti"].xcom_pull(task_ids="analyze_data", key="analysis")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    result_file = OUTPUT_DIR / f"customer_delivery_report_{batch_mode}.json"
    result = {
        "mode": batch_mode,
        "source_rows": analysis["rows_total"],
        "rows_by_status": analysis["rows_by_status"],
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    result_file.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Report written to {result_file}")
    print(json.dumps(result, ensure_ascii=False, indent=2))


def quarantine_source(**context):
    profile = context["ti"].xcom_pull(task_ids="read_source", key="source_profile")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    quarantine_file = OUTPUT_DIR / "quarantine" / DATA_FILE.name
    quarantine_file.parent.mkdir(parents=True, exist_ok=True)
    quarantine_file.write_bytes(DATA_FILE.read_bytes())
    print(f"Invalid source moved to quarantine: {quarantine_file}")
    print(json.dumps(profile, ensure_ascii=False, indent=2))


default_args = {
    "owner": "marketing-data",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "email": [NOTIFY_TO],
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="customer_delivery_report_batch",
    description="POC batch pipeline: file read, validation, branching, retries and email notifications",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["poc", "batch", "task1"],
) as dag:
    start = EmptyOperator(task_id="start")

    read = PythonOperator(
        task_id="read_source",
        python_callable=read_source,
        retries=2,
        retry_delay=timedelta(seconds=20),
    )

    quality_branch = BranchPythonOperator(
        task_id="branch_on_data_quality",
        python_callable=choose_quality_path,
    )

    analyze = PythonOperator(
        task_id="analyze_data",
        python_callable=analyze_data,
        retries=2,
        retry_delay=timedelta(seconds=20),
    )

    volume_branch = BranchPythonOperator(
        task_id="branch_on_volume",
        python_callable=choose_volume_path,
    )

    large_batch = PythonOperator(
        task_id="process_large_batch",
        python_callable=process_batch,
        op_kwargs={"batch_mode": "large"},
    )

    small_batch = PythonOperator(
        task_id="process_small_batch",
        python_callable=process_batch,
        op_kwargs={"batch_mode": "small"},
    )

    quarantine = PythonOperator(
        task_id="quarantine_source",
        python_callable=quarantine_source,
    )

    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success",
    )

    notify_success = EmailOperator(
        task_id="notify_success",
        to=[NOTIFY_TO],
        subject="[Airflow POC] customer_delivery_report_batch succeeded",
        html_content="""
        <h3>Пайплайн успешно завершён</h3>
        <p>Отчёт по статусам доставки сформирован. Детали доступны в task instance logs и в каталоге output.</p>
        """,
    )

    notify_failure = EmailOperator(
        task_id="notify_failure",
        to=[NOTIFY_TO],
        subject="[Airflow POC] customer_delivery_report_batch requires attention",
        html_content="""
        <h3>Пайплайн завершился по fallback-ветке</h3>
        <p>Входной файл помещён в карантин. Проверьте структуру CSV, обязательные поля и пустые значения.</p>
        """,
    )

    start >> read >> quality_branch
    quality_branch >> analyze >> volume_branch
    quality_branch >> quarantine >> notify_failure
    volume_branch >> large_batch >> join
    volume_branch >> small_batch >> join
    join >> notify_success
