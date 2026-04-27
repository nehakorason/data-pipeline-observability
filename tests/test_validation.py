"""
test_validation.py
------------------
Unit tests for src/validation.py

Tests cover:
  - Valid data passes all expectations
  - Null values in critical columns are caught
  - Amount <= 0 is caught
  - Duplicate transaction_id is caught
  - Invalid status value is caught
  - Invalid currency format is caught
"""

import sys
import os

import pandas as pd
import pytest

# Ensure src/ is on the path regardless of how pytest is invoked
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from validation import validate_dataframe, EXPECTATIONS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_valid_df() -> pd.DataFrame:
    """Minimal valid DataFrame that should pass all expectations."""
    return pd.DataFrame(
        {
            "transaction_id": ["TXN001", "TXN002", "TXN003"],
            "customer_id": ["CUST1", "CUST2", "CUST3"],
            "amount": [100.0, 250.0, 50.0],
            "currency": ["USD", "USD", "EUR"],
            "merchant": ["Shop A", "Shop B", "Shop C"],
            "category": ["retail", "food", "travel"],
            "timestamp": pd.to_datetime(
                ["2024-01-15 10:00:00", "2024-01-15 11:00:00", "2024-01-15 12:00:00"]
            ),
            "status": ["completed", "pending", "completed"],
        }
    )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

class TestValidDataframe:
    def test_valid_data_passes(self):
        df = _make_valid_df()
        result = validate_dataframe(df)
        assert result["success"] is True, (
            f"Expected validation to pass, got failures: {result['failed_expectations']}"
        )

    def test_statistics_present(self):
        df = _make_valid_df()
        result = validate_dataframe(df)
        assert "statistics" in result
        assert result["statistics"]["successful_expectations"] > 0

    def test_no_failed_expectations_on_valid_data(self):
        df = _make_valid_df()
        result = validate_dataframe(df)
        assert result["failed_expectations"] == []


# ---------------------------------------------------------------------------
# Null value checks
# ---------------------------------------------------------------------------

class TestNullValues:
    @pytest.mark.parametrize("col", ["transaction_id", "customer_id", "amount", "timestamp"])
    def test_null_in_critical_column_fails(self, col):
        df = _make_valid_df()
        df.loc[0, col] = None
        result = validate_dataframe(df)
        assert result["success"] is False
        failed_types = [f["expectation"] for f in result["failed_expectations"]]
        assert "expect_column_values_to_not_be_null" in failed_types


# ---------------------------------------------------------------------------
# Amount range checks
# ---------------------------------------------------------------------------

class TestAmountRange:
    def test_zero_amount_fails(self):
        df = _make_valid_df()
        df.loc[0, "amount"] = 0.0
        result = validate_dataframe(df)
        assert result["success"] is False
        failed_types = [f["expectation"] for f in result["failed_expectations"]]
        assert "expect_column_values_to_be_between" in failed_types

    def test_negative_amount_fails(self):
        df = _make_valid_df()
        df.loc[0, "amount"] = -50.0
        result = validate_dataframe(df)
        assert result["success"] is False

    def test_positive_amount_passes(self):
        df = _make_valid_df()
        df.loc[0, "amount"] = 0.01
        result = validate_dataframe(df)
        assert result["success"] is True


# ---------------------------------------------------------------------------
# Uniqueness checks
# ---------------------------------------------------------------------------

class TestUniqueness:
    def test_duplicate_transaction_id_fails(self):
        df = _make_valid_df()
        df.loc[1, "transaction_id"] = "TXN001"   # duplicate
        result = validate_dataframe(df)
        assert result["success"] is False
        failed_types = [f["expectation"] for f in result["failed_expectations"]]
        assert "expect_column_values_to_be_unique" in failed_types


# ---------------------------------------------------------------------------
# Status set checks
# ---------------------------------------------------------------------------

class TestStatusValues:
    def test_invalid_status_fails(self):
        df = _make_valid_df()
        df.loc[0, "status"] = "UNKNOWN_STATUS"
        result = validate_dataframe(df)
        assert result["success"] is False
        failed_types = [f["expectation"] for f in result["failed_expectations"]]
        assert "expect_column_values_to_be_in_set" in failed_types

    @pytest.mark.parametrize("status", ["completed", "pending", "failed", "reversed"])
    def test_valid_statuses_pass(self, status):
        df = _make_valid_df()
        df["status"] = status
        result = validate_dataframe(df)
        # Only test status expectation — other columns remain valid
        assert result["success"] is True


# ---------------------------------------------------------------------------
# Currency format checks
# ---------------------------------------------------------------------------

class TestCurrencyFormat:
    def test_lowercase_currency_fails(self):
        df = _make_valid_df()
        df.loc[0, "currency"] = "usd"
        result = validate_dataframe(df)
        assert result["success"] is False

    def test_long_currency_fails(self):
        df = _make_valid_df()
        df.loc[0, "currency"] = "USDT"
        result = validate_dataframe(df)
        assert result["success"] is False

    def test_valid_currency_passes(self):
        df = _make_valid_df()
        df.loc[0, "currency"] = "GBP"
        result = validate_dataframe(df)
        assert result["success"] is True


# ---------------------------------------------------------------------------
# Missing column checks
# ---------------------------------------------------------------------------

class TestSchemaColumns:
    def test_missing_required_column_fails(self):
        """
        GE may raise MetricResolutionError when a required column is entirely
        absent (it cannot compute metrics on a missing column). Either a failed
        result OR an exception counts as the pipeline correctly rejecting bad data.
        """
        import great_expectations.exceptions as gx_exc
        df = _make_valid_df().drop(columns=["amount"])
        try:
            result = validate_dataframe(df)
            assert result["success"] is False
        except (gx_exc.MetricResolutionError, ValueError):
            pass  # GE raises before returning — pipeline correctly fails

    def test_extra_column_is_ok(self):
        df = _make_valid_df()
        df["extra_column"] = "extra"
        result = validate_dataframe(df)
        assert result["success"] is True  # exact_match=False allows extras


# ---------------------------------------------------------------------------
# Expectation suite structure
# ---------------------------------------------------------------------------

class TestExpectationSuite:
    def test_expectations_list_is_not_empty(self):
        assert len(EXPECTATIONS) > 0

    def test_all_critical_columns_have_not_null_check(self):
        null_checked = [
            kw["column"]
            for method, kw in EXPECTATIONS
            if method == "expect_column_values_to_not_be_null"
        ]
        for col in ["transaction_id", "customer_id", "amount", "timestamp"]:
            assert col in null_checked, f"Missing not-null check for column: {col}"
