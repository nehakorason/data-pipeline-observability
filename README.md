# Data Pipeline Observability Framework

A minimal but production-realistic framework for building **observable data pipelines** using Python, Apache Airflow, Great Expectations, PostgreSQL, and Docker.

> Built as a portfolio project demonstrating data engineering best practices: schema validation, anomaly detection, structured logging, and containerised orchestration — all runnable locally with a single command.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Running the Pipeline](#running-the-pipeline)
- [How Validation Works](#how-validation-works)
- [How Anomaly Detection Works](#how-anomaly-detection-works)
- [Running Tests](#running-tests)
- [Example Outputs](#example-outputs)
- [Environment Variables](#environment-variables)
- [Future Improvements](#future-improvements)

---

## Overview

This project demonstrates a **four-stage data pipeline** with built-in observability:

| Stage | Module | What it does |
|-------|--------|-------------|
| **Ingest** | `src/ingestion.py` | Reads a CSV of transactions → loads into PostgreSQL `raw_transactions` |
| **Validate** | `src/validation.py` | Runs Great Expectations checks — fails the pipeline on bad data |
| **Transform** | `src/transformation.py` | Cleans data, normalises currency, derives `risk_band` column |
| **Detect** | `src/anomaly_detection.py` | Z-score outlier detection + data freshness check |

Everything is orchestrated by an **Airflow DAG**, persisted in **PostgreSQL**, and runnable locally via **Docker Compose**.

---

## Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│                          Docker Compose Network                        │
│                                                                        │
│  ┌──────────────────┐         ┌──────────────────────────────────────┐ │
│  │   data/           │         │          Apache Airflow              │ │
│  │  sample_          │ ──CSV─▶ │  ┌──────────────────────────────┐   │ │
│  │  transactions.csv │         │  │  DAG: data_pipeline_         │   │ │
│  └──────────────────┘         │  │       observability           │   │ │
│                                │  │                              │   │ │
│                                │  │  [ingest] ──▶ [validate]    │   │ │
│                                │  │      ──▶ [transform]        │   │ │
│                                │  │      ──▶ [detect_anomalies] │   │ │
│                                │  └──────────────────────────────┘   │ │
│                                │  Webserver  :8080  Scheduler        │ │
│                                └──────────────┬───────────────────────┘ │
│                                               │ SQL                     │
│                                               ▼                         │
│                                ┌──────────────────────────┐            │
│                                │       PostgreSQL :5432   │            │
│                                │                          │            │
│                                │  pipeline_db             │            │
│                                │  ├─ raw_transactions     │            │
│                                │  ├─ transformed_         │            │
│                                │  │   transactions        │            │
│                                │  └─ anomaly_log          │            │
│                                │                          │            │
│                                │  airflow_db (metadata)   │            │
│                                └──────────────────────────┘            │
└────────────────────────────────────────────────────────────────────────┘
```

**Data flow:**

```
CSV file
  │
  ▼
[Ingest]  ──── raw_transactions (PostgreSQL)
                      │
                      ▼
            [Validate] (Great Expectations)
                      │  ← FAIL HERE if data quality check fails
                      ▼
            [Transform] ──── transformed_transactions
                      │
                      ▼
            [Detect Anomalies] ──── anomaly_log
                                     + marks is_anomaly=TRUE
```

---

## Project Structure

```
data-pipeline-observability/
│
├── dags/
│   └── pipeline_dag.py          # Airflow DAG definition
│
├── data/
│   └── sample_transactions.csv  # Source dataset (30 transactions)
│
├── src/
│   ├── ingestion.py             # CSV → PostgreSQL loader
│   ├── validation.py            # Great Expectations validation
│   ├── transformation.py        # Clean + derive columns
│   └── anomaly_detection.py     # Z-score + freshness detection
│
├── tests/
│   ├── test_validation.py       # Unit tests for validation logic
│   └── test_anomaly.py          # Unit + integration tests for detection
│
├── great_expectations/
│   ├── great_expectations.yml   # GE project config
│   └── expectations/
│       └── transactions_suite.json  # Expectation suite definition
│
├── scripts/
│   └── init_postgres.sh         # Creates both DBs on first start
│
├── docker-compose.yml           # Full stack definition
├── Dockerfile.airflow           # Extends apache/airflow image
├── requirements.txt             # Python dependencies
├── pytest.ini                   # Test configuration
├── Makefile                     # Convenience targets
└── README.md
```

---

## Quick Start

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (≥ 4.x) with Compose v2
- 4 GB RAM allocated to Docker (Airflow is memory-hungry)
- Ports `8080` and `5432` free on your host

### 1. Clone and start

```bash
git clone https://github.com/youruser/data-pipeline-observability.git
cd data-pipeline-observability

# Build images and start all services
docker compose up --build
```

Or using the Makefile:

```bash
make up
```

First start takes **2–3 minutes** while Docker pulls and builds images.

### 2. Wait for services to be healthy

Watch for this in the logs:

```
airflow-webserver-1  | [INFO] Listening at: http://0.0.0.0:8080
```

Or check:

```bash
docker compose ps
```

All services should show `healthy` or `running`.

### 3. Open Airflow UI

Navigate to **http://localhost:8080**

| Field | Value |
|-------|-------|
| Username | `admin` |
| Password | `admin` |

---

## Running the Pipeline

### Trigger via Airflow UI

1. Open **http://localhost:8080**
2. Find the DAG named **`data_pipeline_observability`**
3. Click the **▶ (Trigger DAG)** button
4. Watch tasks turn green: `ingest → validate → transform → detect_anomalies`

### Trigger via CLI

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger data_pipeline_observability
```

### Inspect results in PostgreSQL

```bash
# Connect to the database
docker compose exec postgres \
  psql -U pipeline_user -d pipeline_db

# Check ingested data
SELECT count(*), min(amount), max(amount) FROM raw_transactions;

# Check transformed data with risk bands
SELECT risk_band, count(*) FROM transformed_transactions GROUP BY 1;

# Check anomalies
SELECT * FROM anomaly_log ORDER BY detected_at DESC;

# See which transactions were flagged
SELECT transaction_id, amount_usd, risk_band, is_anomaly
FROM transformed_transactions
WHERE is_anomaly = TRUE;
```

---

## How Validation Works

Validation uses **Great Expectations** (OSS) with an in-memory DataContext — no filesystem GE project bootstrapping needed at runtime.

The expectation suite (`transactions_suite`) is defined in `src/validation.py` and checks:

| Expectation | What it validates |
|-------------|------------------|
| `expect_table_columns_to_match_set` | All 8 required columns exist |
| `expect_column_values_to_not_be_null` | `transaction_id`, `customer_id`, `amount`, `timestamp` are never null |
| `expect_column_values_to_be_between` | `amount > 0` (strict) |
| `expect_column_values_to_be_unique` | `transaction_id` is a true primary key |
| `expect_column_values_to_be_in_set` | `status` ∈ {completed, pending, failed, reversed} |
| `expect_column_values_to_match_regex` | `currency` matches `^[A-Z]{3}$` |

**What happens on failure:**

```
ValidationError: Data validation failed.
Failed expectations: [
  {
    "expectation": "expect_column_values_to_not_be_null",
    "kwargs": {"column": "amount"},
    ...
  }
]
```

The Airflow task is marked **Failed** and downstream tasks (`transform`, `detect_anomalies`) are skipped. The pipeline is safe — no bad data reaches the warehouse.

**To test a validation failure manually:**

Edit `data/sample_transactions.csv`, set one `amount` to blank, and re-trigger the DAG. The `validate` task will go red.

---

## How Anomaly Detection Works

Two independent detection strategies run in the `detect_anomalies` task:

### Strategy 1 — Z-score Outlier Detection

**Algorithm:**

```
z = (amount_usd - mean(amount_usd)) / std(amount_usd)
flag if |z| > threshold  (default: 3.0)
```

Z-score of 3.0 means the value is 3 standard deviations from the mean — roughly the top/bottom 0.3% of a normal distribution.

**On the sample dataset:**

Transaction `TXN019` (Real Estate, $95,000) and `TXN010` (Car Dealer, $18,500) will likely be flagged depending on the distribution of all 30 records.

```
⚠️  Amount outliers detected: 2 transaction(s)
 transaction_id  amount_usd  z_score
        TXN019    95000.00    4.821
        TXN010    18500.00    3.142
```

### Strategy 2 — Data Freshness Check

```
lag = now() - max(timestamp)
flag if lag > threshold  (default: 24 hours)
```

This catches:
- Upstream feed outages (no new data arriving)
- Stuck schedulers
- Timezone misconfigurations

**On the sample dataset:**

Since the sample data is from 2024-01-15, the freshness check will fire. This is expected — the sample data is historical. In production, point `PIPELINE_CSV_PATH` at a live feed.

```
⚠️  Freshness anomaly: Latest record is 8760.0h old (threshold=24h).
```

**Anomalies are written to the `anomaly_log` table:**

```sql
SELECT * FROM anomaly_log;

 id | transaction_id | anomaly_type    | detail                                          | detected_at
----+----------------+-----------------+-------------------------------------------------+-------------
  1 | TXN019         | amount_outlier  | amount_usd=95000.00 z_score=4.821 (threshold=3) | 2024-01-15...
  2 | TXN010         | amount_outlier  | amount_usd=18500.00 z_score=3.142 (threshold=3) | 2024-01-15...
  3 | NULL           | freshness_issue | Latest record is 8760.0h old (threshold=24h)    | 2024-01-15...
```

---

## Running Tests

Tests run against the source code directly — **no Docker required**.

### Setup (one-time)

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Run all tests

```bash
make test
# or
PYTHONPATH=src pytest tests/ -v
```

### Expected output

```
tests/test_validation.py::TestValidDataframe::test_valid_data_passes        PASSED
tests/test_validation.py::TestValidDataframe::test_statistics_present       PASSED
tests/test_validation.py::TestNullValues::test_null_in_critical_column_fails[transaction_id] PASSED
tests/test_validation.py::TestNullValues::test_null_in_critical_column_fails[customer_id]    PASSED
tests/test_validation.py::TestAmountRange::test_zero_amount_fails           PASSED
tests/test_validation.py::TestAmountRange::test_negative_amount_fails       PASSED
tests/test_validation.py::TestUniqueness::test_duplicate_transaction_id_fails PASSED
...
tests/test_anomaly.py::TestDetectAmountOutliers::test_extreme_outlier_is_detected PASSED
tests/test_anomaly.py::TestIntegrationBadData::test_pipeline_detects_injected_outlier PASSED
tests/test_anomaly.py::TestIntegrationBadData::test_pipeline_is_clean_on_good_data  PASSED

================================ 28 passed in 12.34s ================================
```

### Run with coverage

```bash
PYTHONPATH=src pytest tests/ --cov=src --cov-report=term-missing
```

---

## Example Outputs

### Airflow DAG view

```
data_pipeline_observability
│
├── [✅ ingest]          0:04   Loaded 30 rows into raw_transactions
├── [✅ validate]         0:08   Validation PASSED — 9/9 expectations met
├── [✅ transform]        0:03   Wrote 30 rows to transformed_transactions
└── [⚠️  detect_anomalies] 0:02   2 outliers + 1 freshness issue detected
```

### Transformation output (risk band breakdown)

```
risk_band    count
---------    -----
low          21      (amount < $500)
medium        6      ($500 – $5,000)
high          3      (> $5,000)
```

### Anomaly log

```
id | transaction_id | anomaly_type    | detail
---+----------------+-----------------+---------------------------------------------
 1 | TXN019         | amount_outlier  | amount_usd=95000.00 z_score=4.821
 2 | TXN010         | amount_outlier  | amount_usd=18500.00 z_score=3.142
 3 | <null>         | freshness_issue | Latest record is 8760.1h old (threshold=24h)
```

### Validation failure (bad data scenario)

```
[ERROR] Validation FAILED — 1/9 expectations failed.
[ERROR]   ✗ expect_column_values_to_not_be_null | kwargs={'column': 'amount'}
Traceback:
  ValueError: Data validation failed.
  Failed expectations: [{'expectation': 'expect_column_values_to_not_be_null', ...}]
```

---

## Environment Variables

All variables have sensible defaults matching the Docker Compose setup.

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | `postgres` | PostgreSQL hostname |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `pipeline_db` | Pipeline database name |
| `POSTGRES_USER` | `pipeline_user` | Database user |
| `POSTGRES_PASSWORD` | `pipeline_pass` | Database password |
| `PIPELINE_CSV_PATH` | `/opt/airflow/data/sample_transactions.csv` | Source CSV path |
| `ANOMALY_Z_THRESHOLD` | `3.0` | Z-score cutoff for outlier detection |
| `FRESHNESS_THRESHOLD_HOURS` | `24.0` | Max acceptable data age (hours) |

To override, edit the `environment` section in `docker-compose.yml` or export variables before running tests locally.

---

## Teardown

```bash
# Stop containers (keep volumes)
docker compose down

# Full teardown including PostgreSQL data
docker compose down -v

# Or via Makefile
make clean
```

---

## Future Improvements

| Area | Improvement |
|------|------------|
| **Alerting** | Integrate Slack/PagerDuty webhook on anomaly detection |
| **Metrics** | Expose pipeline metrics via Prometheus + Grafana dashboard |
| **Data quality** | Add row-count trend detection (sudden drops = upstream issue) |
| **FX rates** | Replace the stub FX lookup with a live API (e.g. exchangerate.host) |
| **Anomaly ML** | Replace z-score with Isolation Forest for multivariate anomaly detection |
| **Airflow** | Move to CeleryExecutor + Redis for distributed task execution |
| **CI/CD** | Add GitHub Actions workflow: lint → test → docker build on PR |
| **Data lineage** | Integrate OpenLineage/Marquez for full column-level lineage tracking |
| **Great Expectations** | Publish Data Docs to S3/GCS for shareable HTML validation reports |
| **Partitioning** | Partition `raw_transactions` by date for time-series query efficiency |

---

## Tech Stack

| Technology | Version | Role |
|-----------|---------|------|
| Python | 3.11 | Pipeline logic |
| Apache Airflow | 2.9.2 | Orchestration |
| Great Expectations | 0.18.15 | Data validation |
| PostgreSQL | 15 | Data persistence |
| Docker Compose | v2 | Local environment |
| pandas | 2.2.2 | DataFrame operations |
| NumPy | 1.26.4 | Statistical computing |
| pytest | 8.2.0 | Testing |

---

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/your-feature`
3. Run tests: `make test`
4. Open a pull request

---

*Built by [Neha Mary Korason](https://linkedin.com/in/neha-mary-korason-01693b217) · [github.com/nehakorason](https://github.com/nehakorason)*
