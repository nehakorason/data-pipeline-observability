"""
validation.py
-------------
Data validation using Great Expectations (OSS, in-memory approach).
Validates the raw DataFrame before it is written to the warehouse.

Design choice: we use GE's in-memory DataContext (Ephemeral) so no
filesystem-level GE project setup is required at runtime — the suite
is defined in code and the config folder is used only for optional
persistence of results.
"""

import logging
from typing import Any

import pandas as pd
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Expectation suite definition
# ---------------------------------------------------------------------------

SUITE_NAME = "transactions_suite"

# Each expectation is (method_name, kwargs).
# We keep them as data so tests can inspect them easily.
EXPECTATIONS = [
    # --- Schema: required columns must exist ---
    ("expect_table_columns_to_match_set", {
        "column_set": [
            "transaction_id", "customer_id", "amount", "currency",
            "merchant", "category", "timestamp", "status",
        ],
        "exact_match": False,   # extra columns are OK
    }),

    # --- No nulls in critical columns ---
    ("expect_column_values_to_not_be_null", {"column": "transaction_id"}),
    ("expect_column_values_to_not_be_null", {"column": "customer_id"}),
    ("expect_column_values_to_not_be_null", {"column": "amount"}),
    ("expect_column_values_to_not_be_null", {"column": "timestamp"}),

    # --- Value ranges ---
    ("expect_column_values_to_be_between", {
        "column": "amount",
        "min_value": 0,
        "max_value": None,      # no upper hard cap at validation stage
        "strict_min": True,     # amount must be > 0
    }),

    # --- Unique primary key ---
    ("expect_column_values_to_be_unique", {"column": "transaction_id"}),

    # --- Allowed status values ---
    ("expect_column_values_to_be_in_set", {
        "column": "status",
        "value_set": ["completed", "pending", "failed", "reversed"],
    }),

    # --- Currency is a 3-letter code ---
    ("expect_column_values_to_match_regex", {
        "column": "currency",
        "regex": r"^[A-Z]{3}$",
    }),
]


# ---------------------------------------------------------------------------
# Core validation logic
# ---------------------------------------------------------------------------

def _build_context():
    """
    Build a lightweight in-memory GE DataContext.
    No filesystem artefacts are written unless you configure a store backend.
    """
    config = DataContextConfig(
        store_backend_defaults=InMemoryStoreBackendDefaults(),
        anonymous_usage_statistics={"enabled": False},
    )
    return gx.get_context(project_config=config)


def validate_dataframe(df: pd.DataFrame) -> dict[str, Any]:
    """
    Run the full expectation suite against *df*.

    Returns a result dict:
        {
            "success": bool,
            "statistics": {...},
            "failed_expectations": [...],
        }

    Raises RuntimeError if validation itself errors (not just fails).
    """
    logger.info("Starting Great Expectations validation (%d rows).", len(df))

    context = _build_context()

    # Register an in-memory datasource backed by the DataFrame.
    # add_or_update_pandas avoids "already exists" errors on repeated calls
    # (GE 0.18 persists datasource names to its YAML store).
    datasource = context.sources.add_or_update_pandas("transactions_datasource")
    asset = datasource.add_dataframe_asset("transactions_asset")
    batch_request = asset.build_batch_request(dataframe=df)

    # Create (or overwrite) the expectation suite
    suite = context.add_or_update_expectation_suite(SUITE_NAME)

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite,
    )

    # Add expectations to the validator
    for method_name, kwargs in EXPECTATIONS:
        getattr(validator, method_name)(**kwargs)

    # Run validation
    results = validator.validate()

    failed = [
        {
            "expectation": r.expectation_config.expectation_type,
            "kwargs": r.expectation_config.kwargs,
            "result": r.result,
        }
        for r in results.results
        if not r.success
    ]

    summary = {
        "success": results.success,
        "statistics": results.statistics,
        "failed_expectations": failed,
    }

    if results.success:
        logger.info(
            "Validation PASSED — %d/%d expectations met.",
            results.statistics["successful_expectations"],
            results.statistics["evaluated_expectations"],
        )
    else:
        logger.error(
            "Validation FAILED — %d/%d expectations failed.",
            results.statistics["unsuccessful_expectations"],
            results.statistics["evaluated_expectations"],
        )
        for f in failed:
            logger.error("  ✗ %s | kwargs=%s", f["expectation"], f["kwargs"])

    return summary


# ---------------------------------------------------------------------------
# Airflow-callable entry point
# ---------------------------------------------------------------------------

def run_validation(csv_path: str) -> dict[str, Any]:
    """
    Load the CSV and validate it.
    Raises ValueError (fails the Airflow task) if validation does not pass.
    """
    import pandas as pd
    df = pd.read_csv(csv_path, parse_dates=["timestamp"])

    result = validate_dataframe(df)

    if not result["success"]:
        raise ValueError(
            f"Data validation failed. "
            f"Failed expectations: {result['failed_expectations']}"
        )

    return result
