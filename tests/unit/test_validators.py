"""Unit tests for input parameter validation."""

from __future__ import annotations

from datetime import date, timedelta

from src.lib.validators import ValidatedParams, validate_params


class TestValidateParams:
    """Tests for the validate_params function."""

    def test_valid_defaults(self) -> None:
        """Default parameters should validate successfully."""
        result = validate_params(
            count=10_000, start_date=None, end_date=None,
            accounts=100, output_format="parquet", seed=42,
        )
        assert isinstance(result, ValidatedParams)
        assert result.count == 10_000
        assert result.seed == 42

    def test_negative_count_rejected(self) -> None:
        """Negative count must return a validation error."""
        result = validate_params(
            count=-1, start_date=None, end_date=None,
            accounts=100, output_format="parquet", seed=42,
        )
        assert isinstance(result, list)
        assert any(e.field == "count" for e in result)

    def test_zero_count_accepted(self) -> None:
        """Zero count must be accepted (produces empty file with schema)."""
        result = validate_params(
            count=0, start_date=None, end_date=None,
            accounts=100, output_format="parquet", seed=42,
        )
        assert isinstance(result, ValidatedParams)
        assert result.count == 0

    def test_invalid_start_date_format(self) -> None:
        """Invalid date format must return a validation error."""
        result = validate_params(
            count=100, start_date="not-a-date", end_date=None,
            accounts=10, output_format="parquet", seed=42,
        )
        assert isinstance(result, list)
        assert any(e.field == "start_date" for e in result)

    def test_invalid_end_date_format(self) -> None:
        """Invalid end date format must return a validation error."""
        result = validate_params(
            count=100, start_date=None, end_date="2025/01/01",
            accounts=10, output_format="parquet", seed=42,
        )
        assert isinstance(result, list)
        assert any(e.field == "end_date" for e in result)

    def test_end_before_start_rejected(self) -> None:
        """End date before start date must return a validation error."""
        result = validate_params(
            count=100, start_date="2025-12-31", end_date="2025-01-01",
            accounts=10, output_format="parquet", seed=42,
        )
        assert isinstance(result, list)
        assert any(e.field == "date_range" for e in result)

    def test_same_start_end_accepted(self) -> None:
        """Same start and end date (single day) must be accepted."""
        result = validate_params(
            count=100, start_date="2025-06-15", end_date="2025-06-15",
            accounts=10, output_format="parquet", seed=42,
        )
        assert isinstance(result, ValidatedParams)
        assert result.start_date == result.end_date

    def test_invalid_format_rejected(self) -> None:
        """Invalid output format must return a validation error."""
        result = validate_params(
            count=100, start_date=None, end_date=None,
            accounts=10, output_format="json", seed=42,
        )
        assert isinstance(result, list)
        assert any(e.field == "format" for e in result)

    def test_zero_accounts_rejected(self) -> None:
        """Zero accounts must return a validation error."""
        result = validate_params(
            count=100, start_date=None, end_date=None,
            accounts=0, output_format="parquet", seed=42,
        )
        assert isinstance(result, list)
        assert any(e.field == "accounts" for e in result)

    def test_accounts_capped_at_count(self) -> None:
        """Accounts exceeding count must be capped to count."""
        result = validate_params(
            count=10, start_date=None, end_date=None,
            accounts=100, output_format="parquet", seed=42,
        )
        assert isinstance(result, ValidatedParams)
        assert result.accounts == 10

    def test_default_date_range_is_90_days(self) -> None:
        """Default date range must be last 90 days from today."""
        result = validate_params(
            count=100, start_date=None, end_date=None,
            accounts=10, output_format="parquet", seed=42,
        )
        assert isinstance(result, ValidatedParams)
        today = date.today()
        assert result.end_date == today
        assert result.start_date == today - timedelta(days=90)

    def test_auto_generated_seed_when_none(self) -> None:
        """Seed must be auto-generated when not provided."""
        result = validate_params(
            count=100, start_date=None, end_date=None,
            accounts=10, output_format="parquet", seed=None,
        )
        assert isinstance(result, ValidatedParams)
        assert isinstance(result.seed, int)
        assert result.seed >= 0

    def test_multiple_errors_returned(self) -> None:
        """Multiple invalid params should return multiple errors."""
        result = validate_params(
            count=-1, start_date="bad", end_date="bad",
            accounts=0, output_format="xml", seed=42,
        )
        assert isinstance(result, list)
        assert len(result) >= 3

    def test_clear_error_messages(self) -> None:
        """Error messages must be human-readable and descriptive."""
        result = validate_params(
            count=-5, start_date=None, end_date=None,
            accounts=10, output_format="parquet", seed=42,
        )
        assert isinstance(result, list)
        error = next(e for e in result if e.field == "count")
        assert "-5" in error.message
        assert "0" in error.message
