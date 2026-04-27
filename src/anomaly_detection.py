"""
anomaly_detection.py
--------------------
Statistical anomaly detection for the transactions pipeline.

Two detection strategies
------------------------
1. Z-score outlier detection on `amount_usd`
   Any transaction whose amount z-score exceeds Z_THRESHOLD is flagged.
   Default threshold = 3.0 (industry standard for Gaussian tails).

2. Data freshness check
   If the most recent transaction timestamp is older than
   FRESHNESS_THRESHOLD_HOURS, we flag a freshness anomaly.
   This catches stuck pipelines or upstream feed outages.

Anomalies are:
  - Logged to stderr/stdout (picked up by Airflow task logs).
  - Written to the anomaly_log table in PostgreSQL.
  - Used to update is_anomaly = TRUE on transformed_transactions.
"""

import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Any

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tunable thresholds
# ---------------------------------------------------------------------------

Z_THRESHOLD = float(os.getenv("ANOMALY_Z_THRESHOLD", "3.0"))
FRESHNESS_THRESHOLD_HOURS = float(os.getenv("FRESHNESS_THRESHOLD_HOURS", "24.0"))


# ---------------------------------------------------------------------------
# Z-score outlier detection
# ---------------------------------------------------------------------------

def detect_amount_outliers(df: pd.DataFrame, z_threshold: float = Z_THRESHOLD) -> pd.DataFrame:
    """
    Flag rows whose `amount_usd` z-score exceeds *z_threshold*.

    Returns a filtered DataFrame of anomalous rows with extra columns:
        z_score, anomaly_type, detail
    """
    if df.empty:
        return df.iloc[0:0]  # empty frame with same columns

    amounts = df["amount_usd"].astype(float)
    mean = amounts.mean()
    std = amounts.std()

    if std == 0:
        # All values identical — no outliers possible
        logger.info("Amount std=0, skipping z-score detection.")
        return df.iloc[0:0]

    z_scores = (amounts - mean) / std
    outlier_mask = z_scores.abs() > z_threshold

    anomalies = df[outlier_mask].copy()
    anomalies["z_score"] = z_scores[outlier_mask]
    anomalies["anomaly_type"] = "amount_outlier"
    anomalies["detail"] = anomalies.apply(
        lambda r: (
            f"amount_usd={r['amount_usd']:.2f} "
            f"z_score={r['z_score']:.3f} "
            f"(threshold={z_threshold})"
        ),
        axis=1,
    )

    if not anomalies.empty:
        logger.warning(
            "Amount outliers detected: %d transaction(s)\n%s",
            len(anomalies),
            anomalies[["transaction_id", "amount_usd", "z_score"]].to_string(index=False),
        )
    else:
        logger.info("No amount outliers detected (z_threshold=%.1f).", z_threshold)

    return anomalies


# ---------------------------------------------------------------------------
# Data freshness detection
# ---------------------------------------------------------------------------

def detect_freshness_issues(
    df: pd.DataFrame,
    threshold_hours: float = FRESHNESS_THRESHOLD_HOURS,
) -> list[dict[str, Any]]:
    """
    Check whether the latest timestamp in *df* is within the expected window.

    Returns a list of anomaly dicts (empty if data is fresh).
    """
    if df.empty:
        logger.warning("Empty DataFrame — cannot assess freshness.")
        return []

    # Make timestamps tz-aware if they aren't already
    timestamps = pd.to_datetime(df["timestamp"], utc=True)
    latest = timestamps.max()
    now = datetime.now(timezone.utc)
    lag_hours = (now - latest).total_seconds() / 3600

    logger.info(
        "Data freshness check: latest_ts=%s, lag=%.2f hours (threshold=%.1f h).",
        latest, lag_hours, threshold_hours,
    )

    if lag_hours > threshold_hours:
        issue = {
            "anomaly_type": "freshness_issue",
            "detail": (
                f"Latest record is {lag_hours:.1f}h old "
                f"(threshold={threshold_hours}h). "
                f"Latest timestamp: {latest.isoformat()}"
            ),
        }
        logger.warning("Freshness anomaly: %s", issue["detail"])
        return [issue]

    return []


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "pipeline_db"),
        user=os.getenv("POSTGRES_USER", "pipeline_user"),
        password=os.getenv("POSTGRES_PASSWORD", "pipeline_pass"),
    )


def _read_transformed(conn) -> pd.DataFrame:
    query = "SELECT * FROM transformed_transactions"
    df = pd.read_sql(query, conn)
    logger.info("Read %d rows from transformed_transactions.", len(df))
    return df


def _log_anomalies_to_db(anomaly_rows: list[dict], conn) -> None:
    """Write anomaly records to the anomaly_log table."""
    if not anomaly_rows:
        return

    records = [
        (
            row.get("transaction_id"),
            row["anomaly_type"],
            row["detail"],
        )
        for row in anomaly_rows
    ]

    sql = """
        INSERT INTO anomaly_log (transaction_id, anomaly_type, detail)
        VALUES %s
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, records)
    conn.commit()
    logger.info("Logged %d anomaly record(s) to anomaly_log.", len(records))


def _mark_anomalies(transaction_ids: list[str], conn) -> None:
    """Set is_anomaly = TRUE for the given transaction IDs."""
    if not transaction_ids:
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE transformed_transactions
            SET    is_anomaly = TRUE
            WHERE  transaction_id = ANY(%s)
            """,
            (transaction_ids,),
        )
    conn.commit()
    logger.info("Marked %d transaction(s) as anomalous.", len(transaction_ids))


# ---------------------------------------------------------------------------
# Airflow-callable entry point
# ---------------------------------------------------------------------------

def run_anomaly_detection() -> dict[str, Any]:
    """
    Full anomaly detection flow:
      read transformed → detect outliers → check freshness → persist results.
    Returns a summary dict.
    """
    conn = _get_conn()
    try:
        df = _read_transformed(conn)

        # --- Amount outliers ---
        outlier_df = detect_amount_outliers(df)
        outlier_anomalies = [
            {
                "transaction_id": row.transaction_id,
                "anomaly_type": row.anomaly_type,
                "detail": row.detail,
            }
            for row in outlier_df.itertuples()
        ] if not outlier_df.empty else []

        # --- Freshness ---
        freshness_anomalies = detect_freshness_issues(df)

        all_anomalies = outlier_anomalies + freshness_anomalies
        _log_anomalies_to_db(all_anomalies, conn)

        outlier_ids = [a["transaction_id"] for a in outlier_anomalies]
        _mark_anomalies(outlier_ids, conn)

    finally:
        conn.close()

    summary = {
        "amount_outliers": len(outlier_anomalies),
        "freshness_issues": len(freshness_anomalies),
        "total_anomalies": len(all_anomalies),
        "flagged_transactions": outlier_ids,
    }
    logger.info("Anomaly detection complete: %s", summary)
    return summary
