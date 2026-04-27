"""
pipeline_dag.py
---------------
Airflow DAG: Data Pipeline Observability

Stages (in order)
-----------------
1. ingest       — Read CSV → load into raw_transactions
2. validate     — Great Expectations checks on raw CSV
3. transform    — Clean + derive columns → transformed_transactions
4. detect       — Z-score & freshness anomaly detection

The DAG is idempotent: re-running it on the same data upserts cleanly.
Retries are configured at the task level; failures surface in the UI.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Make /opt/airflow/src importable
# ---------------------------------------------------------------------------
sys.path.insert(0, "/opt/airflow/src")

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pipeline configuration
# ---------------------------------------------------------------------------

CSV_PATH = os.getenv(
    "PIPELINE_CSV_PATH",
    "/opt/airflow/data/sample_transactions.csv",
)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,   # set to True + configure smtp in production
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}

# ---------------------------------------------------------------------------
# Task callables (thin wrappers — business logic lives in src/)
# ---------------------------------------------------------------------------

def task_ingest(**context) -> None:
    from ingestion import ingest
    summary = ingest(CSV_PATH)
    logger.info("Ingestion summary pushed to XCom: %s", summary)
    # Push summary to XCom so downstream tasks can read it
    context["ti"].xcom_push(key="ingestion_summary", value=summary)


def task_validate(**context) -> None:
    from validation import run_validation
    result = run_validation(CSV_PATH)
    logger.info("Validation passed. Statistics: %s", result.get("statistics"))
    context["ti"].xcom_push(key="validation_result", value=result)


def task_transform(**context) -> None:
    from transformation import run_transformation
    summary = run_transformation()
    logger.info("Transformation summary: %s", summary)
    context["ti"].xcom_push(key="transformation_summary", value=summary)


def task_detect_anomalies(**context) -> None:
    from anomaly_detection import run_anomaly_detection
    summary = run_anomaly_detection()
    logger.info("Anomaly detection summary: %s", summary)
    context["ti"].xcom_push(key="anomaly_summary", value=summary)

    # Surface important findings prominently in the logs
    if summary["total_anomalies"] > 0:
        logger.warning(
            "⚠️  %d anomaly/anomalies detected in this pipeline run. "
            "Check the anomaly_log table for details.",
            summary["total_anomalies"],
        )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="data_pipeline_observability",
    description="End-to-end data pipeline with validation and anomaly detection",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",          # Run once per day; trigger manually for demos
    start_date=datetime(2024, 1, 1),
    catchup=False,                        # Don't backfill historical runs
    max_active_runs=1,
    tags=["observability", "transactions", "great-expectations"],
    doc_md="""
## Data Pipeline Observability DAG

This DAG runs a four-stage data pipeline:

| Stage | Task | Description |
|-------|------|-------------|
| 1 | `ingest` | Load CSV into `raw_transactions` |
| 2 | `validate` | Run Great Expectations checks |
| 3 | `transform` | Clean data, derive `amount_usd` and `risk_band` |
| 4 | `detect_anomalies` | Z-score outlier + freshness detection |

Results are available via XCom and the `anomaly_log` table.
    """,
) as dag:

    ingest = PythonOperator(
        task_id="ingest",
        python_callable=task_ingest,
        doc_md="Read CSV data and load into raw_transactions table.",
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=task_validate,
        doc_md=(
            "Validate data against Great Expectations suite. "
            "Pipeline fails here if schema or quality checks fail."
        ),
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=task_transform,
        doc_md="Apply cleaning and derived-column transformations.",
    )

    detect_anomalies = PythonOperator(
        task_id="detect_anomalies",
        python_callable=task_detect_anomalies,
        doc_md="Run z-score and freshness anomaly detection.",
    )

    # Define execution order
    ingest >> validate >> transform >> detect_anomalies
