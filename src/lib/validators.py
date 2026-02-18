"""Input parameter validation for the transaction generator."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta


@dataclass
class ValidationError:
    """Represents a single validation failure.

    Attributes:
        field: Name of the invalid parameter.
        message: Human-readable error description.
    """

    field: str
    message: str


@dataclass
class ValidatedParams:
    """Validated and normalized generation parameters.

    Attributes:
        count: Number of transactions to generate.
        start_date: Start of the date range (inclusive).
        end_date: End of the date range (inclusive).
        accounts: Number of unique accounts.
        format: Output format ('parquet' or 'csv').
        seed: Random seed (may be auto-generated).
    """

    count: int
    start_date: date
    end_date: date
    accounts: int
    format: str
    seed: int


def validate_params(
    *,
    count: int,
    start_date: str | None,
    end_date: str | None,
    accounts: int,
    output_format: str,
    seed: int | None,
) -> ValidatedParams | list[ValidationError]:
    """Validate and normalize all generation parameters per FR-010.

    Args:
        count: Requested number of transactions.
        start_date: Start date string (YYYY-MM-DD) or None for default.
        end_date: End date string (YYYY-MM-DD) or None for default.
        accounts: Requested number of unique accounts.
        output_format: Output format string ('parquet' or 'csv').
        seed: Random seed or None for auto-generated.

    Returns:
        ValidatedParams on success, or a list of ValidationError on failure.
    """
    errors: list[ValidationError] = []

    # Validate count
    if count < 0:
        errors.append(ValidationError("count", f"Count must be >= 0, got {count}"))

    # Parse and validate dates
    parsed_start: date | None = None
    parsed_end: date | None = None

    if start_date is not None:
        try:
            parsed_start = datetime.strptime(start_date, "%Y-%m-%d").date()
        except ValueError:
            errors.append(
                ValidationError(
                    "start_date",
                    f"Invalid date format '{start_date}', expected YYYY-MM-DD",
                )
            )

    if end_date is not None:
        try:
            parsed_end = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError:
            errors.append(
                ValidationError(
                    "end_date",
                    f"Invalid date format '{end_date}', expected YYYY-MM-DD",
                )
            )

    # Apply defaults
    today = date.today()
    if parsed_start is None and start_date is None:
        parsed_start = today - timedelta(days=90)
    if parsed_end is None and end_date is None:
        parsed_end = today

    # Validate date range (only if both parsed successfully)
    if parsed_start is not None and parsed_end is not None and parsed_start > parsed_end:
            errors.append(
                ValidationError(
                    "date_range",
                    f"Start date ({parsed_start}) must be before or equal to "
                    f"end date ({parsed_end})",
                )
            )

    # Validate accounts
    if accounts < 1:
        errors.append(
            ValidationError("accounts", f"Account count must be >= 1, got {accounts}")
        )

    # Validate format
    valid_formats = {"parquet", "csv"}
    if output_format not in valid_formats:
        errors.append(
            ValidationError(
                "format",
                f"Invalid format '{output_format}', "
                f"must be one of: {', '.join(sorted(valid_formats))}",
            )
        )

    if errors:
        return errors

    # Cap accounts at count (with warning handled by caller) per edge case spec
    effective_accounts = min(accounts, count) if count > 0 else accounts

    # Generate seed if not provided
    import secrets

    effective_seed = seed if seed is not None else secrets.randbelow(2**31)

    return ValidatedParams(
        count=count,
        start_date=parsed_start,  # type: ignore[arg-type]
        end_date=parsed_end,  # type: ignore[arg-type]
        accounts=effective_accounts,
        format=output_format,
        seed=effective_seed,
    )
