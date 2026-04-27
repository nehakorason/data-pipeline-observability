"""
ingestion.py
------------
Handles reading CSV data and loading it into PostgreSQL.
Keeps I/O concerns separate from transformation and validation.
"""

import logging
import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database connection helper
# ---------------------------------------------------------------------------

def get_db_connection():
    """
    Build a psycopg2 connection from environment variables.
    Defaults are set to match docker-compose service names.
    """
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "pipeline_db"),
        user=os.getenv("POSTGRES_USER", "pipeline_user"),
        password=os.getenv("POSTGRES_PASSWORD", "pipeline_pass"),
    )


# ---------------------------------------------------------------------------
# Schema setup
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS raw_transactions (
    transaction_id  TEXT PRIMARY KEY,
    customer_id     TEXT NOT NULL,
    amount          NUMERIC(15, 2) NOT NULL,
    currency        TEXT NOT NULL,
    merchant        TEXT,
    category        TEXT,
    timestamp       TIMESTAMPTZ NOT NULL,
    status          TEXT,
    ingested_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transformed_transactions (
    transaction_id  TEXT PRIMARY KEY,
    customer_id     TEXT NOT NULL,
    amount          NUMERIC(15, 2) NOT NULL,
    currency        TEXT NOT NULL,
    merchant        TEXT,
    category        TEXT,
    timestamp       TIMESTAMPTZ NOT NULL,
    status          TEXT,
    amount_usd      NUMERIC(15, 2),          -- derived: normalised amount
    risk_band       TEXT,                    -- derived: low / medium / high
    is_anomaly      BOOLEAN DEFAULT FALSE,
    processed_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS anomaly_log (
    id              SERIAL PRIMARY KEY,
    transaction_id  TEXT,
    anomaly_type    TEXT,
    detail          TEXT,
    detected_at     TIMESTAMPTZ DEFAULT NOW()
);
"""


def ensure_schema(conn) -> None:
    """Create tables if they don't already exist."""
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    logger.info("Schema ensured (tables created if absent).")


# ---------------------------------------------------------------------------
# Read CSV
# ---------------------------------------------------------------------------

def read_csv(path: str) -> pd.DataFrame:
    """
    Read the source CSV and parse timestamps.
    Returns a DataFrame; raises on missing file or parse errors.
    """
    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Source file not found: {csv_path}")

    df = pd.read_csv(csv_path, parse_dates=["timestamp"])
    logger.info("Read %d rows from %s", len(df), csv_path)
    return df


# ---------------------------------------------------------------------------
# Load into PostgreSQL
# ---------------------------------------------------------------------------

def load_raw(df: pd.DataFrame, conn) -> int:
    """
    Upsert rows into raw_transactions.
    Returns the number of rows inserted/updated.
    """
    records = [
        (
            row.transaction_id,
            row.customer_id,
            float(row.amount),
            row.currency,
            row.merchant,
            row.category,
            row.timestamp,
            row.status,
        )
        for row in df.itertuples(index=False)
    ]

    sql = """
        INSERT INTO raw_transactions
            (transaction_id, customer_id, amount, currency,
             merchant, category, timestamp, status)
        VALUES %s
        ON CONFLICT (transaction_id) DO UPDATE SET
            amount      = EXCLUDED.amount,
            status      = EXCLUDED.status,
            ingested_at = NOW()
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, records)
    conn.commit()

    logger.info("Loaded %d rows into raw_transactions.", len(records))
    return len(records)


# ---------------------------------------------------------------------------
# Convenience end-to-end function called by the Airflow task
# ---------------------------------------------------------------------------

def ingest(csv_path: str) -> dict:
    """
    Full ingestion flow: read CSV → ensure schema → load raw data.
    Returns a summary dict for XCom / logging.
    """
    logger.info("Starting ingestion from: %s", csv_path)
    df = read_csv(csv_path)

    conn = get_db_connection()
    try:
        ensure_schema(conn)
        row_count = load_raw(df, conn)
    finally:
        conn.close()

    summary = {"rows_ingested": row_count, "source": csv_path}
    logger.info("Ingestion complete: %s", summary)
    return summary
