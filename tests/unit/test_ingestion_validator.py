"""Unit tests for ingestion schema and value validation (T010)."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl

from src.ingestion.validator import (
    EXPECTED_COLUMNS,
    REQUIRED_STRING_COLUMNS,
    VALID_CURRENCIES,
    VALID_STATUSES,
    VALID_TRANSACTION_TYPES,
    _types_compatible,
    validate_file_schema,
    validate_records,
)

# ---------------------------------------------------------------------------
# Tests for constants / schema definition
# ---------------------------------------------------------------------------


class TestSchemaConstants:
    """Tests for the expected schema constants."""

    def test_expected_columns_has_nine_entries(self) -> None:
        assert len(EXPECTED_COLUMNS) == 9

    def test_expected_column_names(self) -> None:
        expected_names = {
            "transaction_id", "timestamp", "amount", "currency",
            "merchant_name", "category", "account_id",
            "transaction_type", "status",
        }
        assert set(EXPECTED_COLUMNS.keys()) == expected_names

    def test_transaction_id_type(self) -> None:
        assert EXPECTED_COLUMNS["transaction_id"] == pl.Utf8

    def test_timestamp_type(self) -> None:
        assert isinstance(EXPECTED_COLUMNS["timestamp"], pl.Datetime)

    def test_amount_type(self) -> None:
        assert EXPECTED_COLUMNS["amount"] == pl.Float64

    def test_valid_currencies(self) -> None:
        assert VALID_CURRENCIES == {"USD", "EUR", "GBP", "JPY"}  # noqa: SIM300

    def test_valid_transaction_types(self) -> None:
        assert VALID_TRANSACTION_TYPES == {"debit", "credit"}  # noqa: SIM300

    def test_valid_statuses(self) -> None:
        assert VALID_STATUSES == {"completed", "pending", "failed"}  # noqa: SIM300

    def test_required_string_columns(self) -> None:
        assert "transaction_id" in REQUIRED_STRING_COLUMNS
        assert "currency" in REQUIRED_STRING_COLUMNS
        assert "merchant_name" in REQUIRED_STRING_COLUMNS
        assert "timestamp" not in REQUIRED_STRING_COLUMNS  # not a string col
        assert "amount" not in REQUIRED_STRING_COLUMNS  # not a string col


# ---------------------------------------------------------------------------
# Tests for _types_compatible
# ---------------------------------------------------------------------------


class TestTypesCompatible:
    """Tests for the _types_compatible helper."""

    def test_exact_match(self) -> None:
        assert _types_compatible(pl.Utf8, pl.Utf8) is True

    def test_float64_exact(self) -> None:
        assert _types_compatible(pl.Float64, pl.Float64) is True

    def test_datetime_different_precision(self) -> None:
        actual = pl.Datetime("ns", time_zone="UTC")
        expected = pl.Datetime("us", time_zone="UTC")
        assert _types_compatible(actual, expected) is True

    def test_datetime_different_timezone(self) -> None:
        actual = pl.Datetime("us", time_zone=None)
        expected = pl.Datetime("us", time_zone="UTC")
        assert _types_compatible(actual, expected) is True

    def test_int64_compatible_with_float64(self) -> None:
        assert _types_compatible(pl.Int64, pl.Float64) is True

    def test_int32_compatible_with_float64(self) -> None:
        assert _types_compatible(pl.Int32, pl.Float64) is True

    def test_float32_compatible_with_float64(self) -> None:
        assert _types_compatible(pl.Float32, pl.Float64) is True

    def test_utf8_not_compatible_with_float64(self) -> None:
        assert _types_compatible(pl.Utf8, pl.Float64) is False

    def test_int64_not_compatible_with_utf8(self) -> None:
        assert _types_compatible(pl.Int64, pl.Utf8) is False


# ---------------------------------------------------------------------------
# Tests for validate_file_schema
# ---------------------------------------------------------------------------


class TestValidateFileSchema:
    """Tests for file-level schema validation."""

    def test_valid_schema_returns_empty(self) -> None:
        df = _make_valid_df()
        errors = validate_file_schema(df)
        assert errors == []

    def test_missing_column_detected(self) -> None:
        df = _make_valid_df().drop("amount")
        errors = validate_file_schema(df)
        assert len(errors) >= 1
        assert any("amount" in e for e in errors)

    def test_extra_column_detected(self) -> None:
        df = _make_valid_df().with_columns(pl.lit("x").alias("extra_col"))
        errors = validate_file_schema(df)
        assert len(errors) >= 1
        assert any("extra_col" in e for e in errors)

    def test_wrong_type_detected(self) -> None:
        df = _make_valid_df().with_columns(
            pl.col("amount").cast(pl.Utf8).alias("amount")
        )
        errors = validate_file_schema(df)
        assert len(errors) >= 1
        assert any("amount" in e for e in errors)

    def test_multiple_missing_columns(self) -> None:
        df = _make_valid_df().drop("amount", "currency")
        errors = validate_file_schema(df)
        assert any("amount" in e for e in errors)
        assert any("currency" in e for e in errors)

    def test_compatible_int_type_passes(self) -> None:
        """Int64 amount column should pass (safe upcast to Float64)."""
        df = _make_valid_df().with_columns(
            pl.col("amount").cast(pl.Int64).alias("amount")
        )
        errors = validate_file_schema(df)
        assert errors == []


# ---------------------------------------------------------------------------
# Tests for validate_records
# ---------------------------------------------------------------------------


class TestValidateRecords:
    """Tests for record-level value validation."""

    def test_all_valid_records(self) -> None:
        df = _make_valid_df()
        result = validate_records(df)

        assert "_is_valid" in result.columns
        assert "_rejection_reason" in result.columns
        assert result["_is_valid"].all()

    def test_negative_amount_flagged(self) -> None:
        df = _make_valid_df()
        df = df.with_columns(pl.lit(-10.0).alias("amount"))
        result = validate_records(df)

        assert not result["_is_valid"][0]
        assert "amount" in result["_rejection_reason"][0]

    def test_zero_amount_flagged(self) -> None:
        df = _make_valid_df()
        df = df.with_columns(pl.lit(0.0).alias("amount"))
        result = validate_records(df)

        assert not result["_is_valid"][0]

    def test_null_amount_flagged(self) -> None:
        df = _make_valid_df().with_columns(
            pl.lit(None, dtype=pl.Float64).alias("amount")
        )
        result = validate_records(df)

        assert not result["_is_valid"][0]
        assert "amount" in result["_rejection_reason"][0]

    def test_invalid_currency_flagged(self) -> None:
        df = _make_valid_df().with_columns(pl.lit("XYZ").alias("currency"))
        result = validate_records(df)

        assert not result["_is_valid"][0]
        assert "currency" in result["_rejection_reason"][0]

    def test_invalid_transaction_type_flagged(self) -> None:
        df = _make_valid_df().with_columns(
            pl.lit("refund").alias("transaction_type")
        )
        result = validate_records(df)

        assert not result["_is_valid"][0]
        assert "transaction_type" in result["_rejection_reason"][0]

    def test_invalid_status_flagged(self) -> None:
        df = _make_valid_df().with_columns(pl.lit("cancelled").alias("status"))
        result = validate_records(df)

        assert not result["_is_valid"][0]
        assert "status" in result["_rejection_reason"][0]

    def test_invalid_account_id_flagged(self) -> None:
        df = _make_valid_df().with_columns(
            pl.lit("INVALID-123").alias("account_id")
        )
        result = validate_records(df)

        assert not result["_is_valid"][0]
        assert "account_id" in result["_rejection_reason"][0]

    def test_null_transaction_id_flagged(self) -> None:
        df = _make_valid_df().with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("transaction_id")
        )
        result = validate_records(df)

        assert not result["_is_valid"][0]
        assert "transaction_id" in result["_rejection_reason"][0]

    def test_empty_merchant_name_flagged(self) -> None:
        df = _make_valid_df().with_columns(pl.lit("").alias("merchant_name"))
        result = validate_records(df)

        assert not result["_is_valid"][0]
        assert "merchant_name" in result["_rejection_reason"][0]

    def test_null_timestamp_flagged(self) -> None:
        df = _make_valid_df().with_columns(
            pl.lit(None, dtype=pl.Datetime("us", time_zone="UTC")).alias("timestamp")
        )
        result = validate_records(df)

        assert not result["_is_valid"][0]
        assert "timestamp" in result["_rejection_reason"][0]

    def test_mixed_valid_and_invalid(self) -> None:
        """Verify that valid and invalid rows in the same DataFrame are handled."""
        df = pl.DataFrame({
            "transaction_id": ["txn-001", "txn-002"],
            "timestamp": [
                datetime(2026, 1, 15, 10, 0, 0, tzinfo=UTC),
                datetime(2026, 1, 15, 11, 0, 0, tzinfo=UTC),
            ],
            "amount": [50.0, -10.0],
            "currency": ["USD", "USD"],
            "merchant_name": ["Store A", "Store B"],
            "category": ["Shopping", "Shopping"],
            "account_id": ["ACC-00001", "ACC-00002"],
            "transaction_type": ["debit", "debit"],
            "status": ["completed", "completed"],
        })
        result = validate_records(df)

        assert result["_is_valid"][0] is True
        assert result["_is_valid"][1] is False

    def test_multiple_errors_concatenated(self) -> None:
        """Record with multiple failures should list all reasons."""
        df = _make_valid_df().with_columns(
            pl.lit(-5.0).alias("amount"),
            pl.lit("XYZ").alias("currency"),
        )
        result = validate_records(df)

        reason = result["_rejection_reason"][0]
        assert "amount" in reason
        assert "currency" in reason


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _make_valid_df() -> pl.DataFrame:
    """Create a single-row valid transaction DataFrame."""
    return pl.DataFrame({
        "transaction_id": ["txn-test-001"],
        "timestamp": [datetime(2026, 1, 15, 10, 30, 0, tzinfo=UTC)],
        "amount": [42.50],
        "currency": ["USD"],
        "merchant_name": ["Test Store"],
        "category": ["Shopping"],
        "account_id": ["ACC-00001"],
        "transaction_type": ["debit"],
        "status": ["completed"],
    })
