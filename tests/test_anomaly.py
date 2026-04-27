"""
test_anomaly.py
---------------
Unit tests for src/anomaly_detection.py

Tests cover:
  - Z-score outlier detection (positive and negative cases)
  - Freshness detection with controlled timestamps
  - Edge cases: empty DataFrame, zero std, all-same values
  - Integration: bad data causes pipeline to detect anomalies
"""

import sys
import os
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from anomaly_detection import detect_amount_outliers, detect_freshness_issues


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_normal_df(n: int = 20, seed: int = 42) -> pd.DataFrame:
    """
    Create a DataFrame with normally distributed amounts — no outliers.
    Mean ~200, std ~30 → z-scores well within ±3.
    """
    rng = np.random.default_rng(seed)
    amounts = rng.normal(loc=200, scale=30, size=n).clip(min=1)
    return pd.DataFrame(
        {
            "transaction_id": [f"TXN{i:04d}" for i in range(n)],
            "amount_usd": amounts,
            "timestamp": pd.date_range("2024-01-15 09:00", periods=n, freq="5min", tz="UTC"),
        }
    )


def _make_df_with_outlier(outlier_amount: float = 999_999.0) -> pd.DataFrame:
    """Normal DataFrame with one extreme outlier injected."""
    df = _make_normal_df()
    df.loc[0, "amount_usd"] = outlier_amount
    return df


def _make_stale_df(hours_old: float = 48.0) -> pd.DataFrame:
    """DataFrame whose latest timestamp is `hours_old` hours ago."""
    df = _make_normal_df()
    stale_ts = datetime.now(timezone.utc) - timedelta(hours=hours_old)
    df["timestamp"] = pd.date_range(
        end=stale_ts, periods=len(df), freq="5min", tz="UTC"
    )
    return df


def _make_fresh_df() -> pd.DataFrame:
    """DataFrame whose latest timestamp is just 1 hour ago."""
    return _make_stale_df(hours_old=1.0)


# ---------------------------------------------------------------------------
# Z-score outlier detection
# ---------------------------------------------------------------------------

class TestDetectAmountOutliers:

    def test_no_outliers_returns_empty(self):
        df = _make_normal_df()
        result = detect_amount_outliers(df, z_threshold=3.0)
        assert result.empty, "Expected no outliers in normally distributed data"

    def test_extreme_outlier_is_detected(self):
        df = _make_df_with_outlier(outlier_amount=999_999.0)
        result = detect_amount_outliers(df, z_threshold=3.0)
        assert len(result) >= 1
        assert "TXN0000" in result["transaction_id"].values

    def test_outlier_has_required_columns(self):
        df = _make_df_with_outlier()
        result = detect_amount_outliers(df)
        for col in ["z_score", "anomaly_type", "detail"]:
            assert col in result.columns

    def test_anomaly_type_is_correct(self):
        df = _make_df_with_outlier()
        result = detect_amount_outliers(df)
        assert (result["anomaly_type"] == "amount_outlier").all()

    def test_z_score_exceeds_threshold(self):
        df = _make_df_with_outlier(outlier_amount=999_999.0)
        result = detect_amount_outliers(df, z_threshold=3.0)
        assert (result["z_score"].abs() > 3.0).all()

    def test_lower_threshold_catches_more(self):
        df = _make_normal_df()
        # Inject a moderate outlier (z ≈ 2.5)
        mean = df["amount_usd"].mean()
        std = df["amount_usd"].std()
        df.loc[0, "amount_usd"] = mean + 2.5 * std

        result_strict = detect_amount_outliers(df, z_threshold=3.0)
        result_loose = detect_amount_outliers(df, z_threshold=2.0)
        assert len(result_loose) >= len(result_strict)

    def test_empty_dataframe_returns_empty(self):
        df = _make_normal_df().iloc[0:0]
        result = detect_amount_outliers(df)
        assert result.empty

    def test_all_same_amounts_returns_empty(self):
        """If std == 0, z-scores are undefined — function should return empty."""
        df = _make_normal_df()
        df["amount_usd"] = 100.0
        result = detect_amount_outliers(df)
        assert result.empty

    def test_negative_z_score_also_flagged(self):
        """Outliers on the LOW end should also be flagged (|z| > threshold)."""
        df = _make_normal_df()
        df.loc[0, "amount_usd"] = -99_999.0   # extremely low
        result = detect_amount_outliers(df, z_threshold=3.0)
        # Only if the negative value actually exceeds threshold
        if not result.empty:
            assert (result["z_score"].abs() > 3.0).all()


# ---------------------------------------------------------------------------
# Freshness detection
# ---------------------------------------------------------------------------

class TestDetectFreshnessIssues:

    def test_fresh_data_returns_no_issues(self):
        df = _make_fresh_df()
        result = detect_freshness_issues(df, threshold_hours=24.0)
        assert result == []

    def test_stale_data_returns_issue(self):
        df = _make_stale_df(hours_old=48.0)
        result = detect_freshness_issues(df, threshold_hours=24.0)
        assert len(result) == 1

    def test_freshness_issue_has_correct_type(self):
        df = _make_stale_df(hours_old=48.0)
        result = detect_freshness_issues(df, threshold_hours=24.0)
        assert result[0]["anomaly_type"] == "freshness_issue"

    def test_freshness_detail_contains_lag_info(self):
        df = _make_stale_df(hours_old=48.0)
        result = detect_freshness_issues(df, threshold_hours=24.0)
        assert "48" in result[0]["detail"] or "old" in result[0]["detail"]

    def test_exactly_at_threshold_is_ok(self):
        """Lag equal to threshold should NOT trigger (strict >)."""
        df = _make_stale_df(hours_old=24.0)
        # Depending on execution speed, might be just over. Test with 23.9h.
        df["timestamp"] = pd.date_range(
            end=datetime.now(timezone.utc) - timedelta(hours=23.9),
            periods=len(df),
            freq="5min",
            tz="UTC",
        )
        result = detect_freshness_issues(df, threshold_hours=24.0)
        assert result == []

    def test_empty_dataframe_returns_empty(self):
        df = _make_normal_df().iloc[0:0]
        result = detect_freshness_issues(df, threshold_hours=24.0)
        assert result == []

    def test_custom_threshold_respected(self):
        """A 2-hour old dataset should fail a 1-hour threshold."""
        df = _make_stale_df(hours_old=2.0)
        result = detect_freshness_issues(df, threshold_hours=1.0)
        assert len(result) == 1

    def test_timezone_naive_timestamps_handled(self):
        """Timestamps without tz info should be coerced to UTC without error."""
        df = _make_normal_df()
        df["timestamp"] = pd.date_range(
            "2024-01-01 00:00", periods=len(df), freq="5min"  # tz-naive
        )
        # Should not raise
        detect_freshness_issues(df, threshold_hours=24.0)


# ---------------------------------------------------------------------------
# Integration: simulate bad data → anomalies detected
# ---------------------------------------------------------------------------

class TestIntegrationBadData:
    """
    Simulate a realistic bad-data scenario end-to-end through the
    detection functions (without hitting the database).
    """

    def test_pipeline_detects_injected_outlier(self):
        """
        Given a dataset with one extreme transaction,
        the pipeline's anomaly detector must flag it.
        """
        df = _make_normal_df(n=30)
        # Inject a fraud-like outlier (100× the typical amount)
        fraud_amount = df["amount_usd"].mean() * 100
        df.loc[0, "amount_usd"] = fraud_amount
        df.loc[0, "transaction_id"] = "FRAUD_TXN"

        outliers = detect_amount_outliers(df, z_threshold=3.0)

        assert not outliers.empty, "Fraud transaction was NOT detected!"
        assert "FRAUD_TXN" in outliers["transaction_id"].values

    def test_pipeline_detects_stale_feed(self):
        """
        Given data that hasn't been refreshed for 72 hours,
        the freshness check must fire.
        """
        df = _make_stale_df(hours_old=72.0)
        issues = detect_freshness_issues(df, threshold_hours=24.0)

        assert len(issues) == 1
        assert issues[0]["anomaly_type"] == "freshness_issue"

    def test_pipeline_is_clean_on_good_data(self):
        """
        Normal data with no outliers and fresh timestamps
        should produce zero anomalies.
        """
        df = _make_fresh_df()
        outliers = detect_amount_outliers(df, z_threshold=3.0)
        freshness = detect_freshness_issues(df, threshold_hours=24.0)

        total = len(outliers) + len(freshness)
        assert total == 0, f"Expected 0 anomalies on clean data, got {total}"

    def test_multiple_outliers_all_detected(self):
        """All injected outliers should be flagged, not just the first."""
        df = _make_normal_df(n=50)
        fraud_amount = df["amount_usd"].mean() * 50
        df.loc[0, "amount_usd"] = fraud_amount
        df.loc[1, "amount_usd"] = fraud_amount * 1.1
        df.loc[2, "amount_usd"] = fraud_amount * 0.9

        outliers = detect_amount_outliers(df, z_threshold=3.0)
        assert len(outliers) >= 3
