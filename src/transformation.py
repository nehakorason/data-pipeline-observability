"""
transformation.py
-----------------
Reads validated raw data from PostgreSQL, applies cleaning and
derived-column logic, then writes to transformed_transactions.

Transformations applied
-----------------------
1. Strip leading/trailing whitespace from string columns.
2. Normalise currency to USD (stub — only USD in sample data, but
   the hook is here for extension).
3. Derive `risk_band` from amount:
       low    : amount <  500
       medium : 500 <= amount < 5 000
       high   : amount >= 5 000
4. Set `is_anomaly = False` as default (anomaly detection fills this later).
"""

import logging
import os

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# FX stub — extend this to hit a real FX API if needed
# ---------------------------------------------------------------------------

FX_RATES_TO_USD = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.27,
    "CAD": 0.74,
}


def _to_usd(amount: float, currency: str) -> float:
    rate = FX_RATES_TO_USD.get(currency.upper(), 1.0)
    return round(amount * rate, 2)


# ---------------------------------------------------------------------------
# Risk banding
# ---------------------------------------------------------------------------

def _assign_risk_band(amount_usd: float) -> str:
    if amount_usd < 500:
        return "low"
    elif amount_usd < 5_000:
        return "medium"
    return "high"


# ---------------------------------------------------------------------------
# Core transformation
# ---------------------------------------------------------------------------

def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all transformation logic to a raw DataFrame.
    Returns a new DataFrame ready for the warehouse.
    """
    df = df.copy()

    # 1. Strip whitespace from string columns
    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        df[col] = df[col].str.strip()

    # 2. Normalise currency → USD
    df["amount_usd"] = df.apply(
        lambda r: _to_usd(r["amount"], r["currency"]), axis=1
    )

    # 3. Risk band
    df["risk_band"] = df["amount_usd"].apply(_assign_risk_band)

    # 4. Default anomaly flag (anomaly detection updates this)
    df["is_anomaly"] = False

    logger.info(
        "Transformation complete. Risk band distribution:\n%s",
        df["risk_band"].value_counts().to_string(),
    )
    return df


# ---------------------------------------------------------------------------
# Database I/O helpers
# ---------------------------------------------------------------------------

def _get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "pipeline_db"),
        user=os.getenv("POSTGRES_USER", "pipeline_user"),
        password=os.getenv("POSTGRES_PASSWORD", "pipeline_pass"),
    )


def _read_raw(conn) -> pd.DataFrame:
    query = """
        SELECT transaction_id, customer_id, amount, currency,
               merchant, category, timestamp, status
        FROM   raw_transactions
    """
    df = pd.read_sql(query, conn)
    logger.info("Read %d rows from raw_transactions.", len(df))
    return df


def _write_transformed(df: pd.DataFrame, conn) -> int:
    records = [
        (
            row.transaction_id, row.customer_id,
            float(row.amount), row.currency,
            row.merchant, row.category,
            row.timestamp, row.status,
            float(row.amount_usd), row.risk_band,
            bool(row.is_anomaly),
        )
        for row in df.itertuples(index=False)
    ]

    sql = """
        INSERT INTO transformed_transactions
            (transaction_id, customer_id, amount, currency,
             merchant, category, timestamp, status,
             amount_usd, risk_band, is_anomaly)
        VALUES %s
        ON CONFLICT (transaction_id) DO UPDATE SET
            amount_usd   = EXCLUDED.amount_usd,
            risk_band    = EXCLUDED.risk_band,
            is_anomaly   = EXCLUDED.is_anomaly,
            processed_at = NOW()
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, records)
    conn.commit()
    logger.info("Wrote %d rows to transformed_transactions.", len(records))
    return len(records)


# ---------------------------------------------------------------------------
# Airflow-callable entry point
# ---------------------------------------------------------------------------

def run_transformation() -> dict:
    """
    Full transformation flow:
      read raw → transform → write transformed.
    Returns summary dict for XCom / logging.
    """
    conn = _get_conn()
    try:
        raw_df = _read_raw(conn)
        transformed_df = transform_dataframe(raw_df)
        rows = _write_transformed(transformed_df, conn)
    finally:
        conn.close()

    summary = {
        "rows_transformed": rows,
        "risk_bands": transformed_df["risk_band"].value_counts().to_dict(),
    }
    logger.info("Transformation summary: %s", summary)
    return summary
