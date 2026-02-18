"""Data quality tests validating 6Cs dimensions on generated output."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest
from scipy import stats

from src.generate_transactions import main


@pytest.fixture
def generated_df(tmp_output_dir: Path) -> pl.DataFrame:
    """Generate a dataset and return it as a DataFrame for quality testing."""
    main([
        "--count", "10000",
        "--seed", "42",
        "--output-dir", str(tmp_output_dir),
    ])
    files = list(tmp_output_dir.glob("transactions_*.parquet"))
    return pl.read_parquet(files[0])


class TestCurrencyDistribution:
    """Validate currency weight distribution per FR-001."""

    def test_currency_weights_within_tolerance(self, generated_df: pl.DataFrame) -> None:
        """Currency distribution must approximate USD 70%, EUR 15%, GBP 10%, JPY 5%."""
        total = len(generated_df)
        counts = generated_df.group_by("currency").len().sort("currency")

        expected = {"EUR": 0.15, "GBP": 0.10, "JPY": 0.05, "USD": 0.70}
        tolerance = 0.03  # 3% tolerance for 10k records

        for row in counts.iter_rows(named=True):
            currency = row["currency"]
            actual_pct = row["len"] / total
            exp_pct = expected[currency]
            assert abs(actual_pct - exp_pct) < tolerance, (
                f"{currency}: {actual_pct:.3f} vs expected {exp_pct:.3f}"
            )

    def test_only_valid_currencies(self, generated_df: pl.DataFrame) -> None:
        """Only USD, EUR, GBP, JPY currencies must appear."""
        valid = {"USD", "EUR", "GBP", "JPY"}
        actual = set(generated_df["currency"].unique().to_list())
        assert actual == valid


class TestTransactionTypeDistribution:
    """Validate transaction type weights per data-model."""

    def test_debit_credit_weights(self, generated_df: pl.DataFrame) -> None:
        """Transaction types must approximate debit 85%, credit 15%."""
        total = len(generated_df)
        counts = generated_df.group_by("transaction_type").len()

        debit_count = counts.filter(pl.col("transaction_type") == "debit")["len"][0]
        debit_pct = debit_count / total
        assert 0.82 < debit_pct < 0.88, f"Debit at {debit_pct:.3f}, expected ~0.85"

    def test_only_valid_types(self, generated_df: pl.DataFrame) -> None:
        """Only debit and credit types must appear."""
        valid = {"debit", "credit"}
        actual = set(generated_df["transaction_type"].unique().to_list())
        assert actual == valid


class TestStatusDistribution:
    """Validate status weights per data-model."""

    def test_status_weights(self, generated_df: pl.DataFrame) -> None:
        """Status distribution must approximate completed 92%, pending 5%, failed 3%."""
        total = len(generated_df)
        counts = generated_df.group_by("status").len()

        completed = counts.filter(pl.col("status") == "completed")["len"][0]
        completed_pct = completed / total
        assert 0.89 < completed_pct < 0.95, f"Completed at {completed_pct:.3f}, expected ~0.92"

    def test_only_valid_statuses(self, generated_df: pl.DataFrame) -> None:
        """Only completed, pending, failed statuses must appear."""
        valid = {"completed", "pending", "failed"}
        actual = set(generated_df["status"].unique().to_list())
        assert actual == valid


class TestAmountDistribution:
    """Validate amount distribution quality per SC-004."""

    def test_skewness_greater_than_one(self, generated_df: pl.DataFrame) -> None:
        """Amount distribution must have skewness > 1.0 per SC-004."""
        amounts = generated_df["amount"].to_numpy()
        skewness = stats.skew(amounts)
        assert skewness > 1.0, f"Skewness {skewness:.3f} is not > 1.0"

    def test_median_less_than_mean(self, generated_df: pl.DataFrame) -> None:
        """Median must be less than mean (right-skewed) per SC-004."""
        amounts = generated_df["amount"].to_numpy()
        assert np.median(amounts) < np.mean(amounts)

    def test_all_amounts_positive(self, generated_df: pl.DataFrame) -> None:
        """All amounts must be strictly positive."""
        assert (generated_df["amount"] > 0).all()


class TestAccountDistribution:
    """Validate account coverage in generated data."""

    def test_account_id_format(self, generated_df: pl.DataFrame) -> None:
        """All account IDs must follow ACC-XXXXX format."""
        for acc_id in generated_df["account_id"].unique().to_list():
            assert acc_id.startswith("ACC-"), f"Invalid account ID: {acc_id}"
            assert len(acc_id) == 9, f"Account ID wrong length: {acc_id}"

    def test_default_account_count(self, generated_df: pl.DataFrame) -> None:
        """Default generation should use 100 unique accounts."""
        unique_accounts = generated_df["account_id"].n_unique()
        assert unique_accounts == 100


class TestMerchantCategoryConsistency:
    """Validate merchant-category pairing consistency."""

    def test_merchant_category_mapping_stable(self, generated_df: pl.DataFrame) -> None:
        """Each merchant must always map to the same category."""
        pairs = generated_df.select("merchant_name", "category").unique()
        merchant_counts = pairs.group_by("merchant_name").len()
        violations = merchant_counts.filter(pl.col("len") > 1)
        assert len(violations) == 0, f"Merchants with multiple categories: {violations}"
