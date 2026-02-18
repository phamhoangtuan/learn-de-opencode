"""Schema and value validation for transaction Parquet files."""

from __future__ import annotations

import polars as pl

# Expected transaction schema: 9 columns per FR-004
EXPECTED_COLUMNS: dict[str, pl.DataType] = {
    "transaction_id": pl.Utf8,
    "timestamp": pl.Datetime("us", time_zone="UTC"),
    "amount": pl.Float64,
    "currency": pl.Utf8,
    "merchant_name": pl.Utf8,
    "category": pl.Utf8,
    "account_id": pl.Utf8,
    "transaction_type": pl.Utf8,
    "status": pl.Utf8,
}

# Valid enum values per data-model.md
VALID_CURRENCIES: set[str] = {"USD", "EUR", "GBP", "JPY"}
VALID_TRANSACTION_TYPES: set[str] = {"debit", "credit"}
VALID_STATUSES: set[str] = {"completed", "pending", "failed"}

# Required non-null string columns per FR-005
REQUIRED_STRING_COLUMNS: list[str] = [
    "transaction_id",
    "currency",
    "merchant_name",
    "category",
    "account_id",
    "transaction_type",
    "status",
]


def validate_file_schema(df: pl.DataFrame) -> list[str]:
    """Validate that a DataFrame has the expected columns and compatible types.

    File-level validation per FR-006: checks column names and data type
    compatibility. If this fails, the entire file should be quarantined.

    Args:
        df: The DataFrame to validate.

    Returns:
        List of error messages. Empty list means the schema is valid.
    """
    errors: list[str] = []

    actual_columns = set(df.columns)
    expected_column_names = set(EXPECTED_COLUMNS.keys())

    missing = expected_column_names - actual_columns
    if missing:
        errors.append(f"Missing columns: {sorted(missing)}")

    extra = actual_columns - expected_column_names
    if extra:
        errors.append(f"Unexpected columns: {sorted(extra)}")

    # Check data types for columns that exist
    if not missing:
        for col_name, expected_type in EXPECTED_COLUMNS.items():
            actual_type = df.schema[col_name]
            if not _types_compatible(actual_type, expected_type):
                errors.append(
                    f"Column '{col_name}' has type {actual_type}, "
                    f"expected {expected_type}"
                )

    return errors


def validate_records(df: pl.DataFrame) -> pl.DataFrame:
    """Validate individual records and return a DataFrame with a validity flag.

    Record-level validation per FR-005 and FR-007. Checks for:
    - Null values in required fields
    - Amount > 0
    - Currency in allowed set
    - Transaction type in allowed set
    - Status in allowed set
    - Account ID matches ACC-XXXXX pattern

    Args:
        df: A DataFrame that has passed file-level schema validation.

    Returns:
        The input DataFrame with an additional '_is_valid' boolean column
        and a '_rejection_reason' string column.
    """
    # Build validation expressions
    checks: list[tuple[pl.Expr, str]] = []

    # Null checks for required string columns
    for col in REQUIRED_STRING_COLUMNS:
        checks.append((
            pl.col(col).is_not_null() & (pl.col(col).str.len_bytes() > 0),
            f"{col} is null or empty",
        ))

    # Null check for timestamp
    checks.append((
        pl.col("timestamp").is_not_null(),
        "timestamp is null",
    ))

    # Null check and range check for amount
    checks.append((
        pl.col("amount").is_not_null() & (pl.col("amount") > 0),
        "amount is null or not positive",
    ))

    # Enum checks
    checks.append((
        pl.col("currency").is_in(list(VALID_CURRENCIES)),
        "currency not in allowed values",
    ))
    checks.append((
        pl.col("transaction_type").is_in(list(VALID_TRANSACTION_TYPES)),
        "transaction_type not in allowed values",
    ))
    checks.append((
        pl.col("status").is_in(list(VALID_STATUSES)),
        "status not in allowed values",
    ))

    # Account ID pattern check: ACC-XXXXX (5 digits)
    checks.append((
        pl.col("account_id").str.contains(r"^ACC-\d{5}$"),
        "account_id does not match ACC-XXXXX pattern",
    ))

    # Combine all checks into a single validity expression
    is_valid_expr = checks[0][0]
    for check_expr, _ in checks[1:]:
        is_valid_expr = is_valid_expr & check_expr

    # Build rejection reason: concatenate all failing check messages
    rejection_parts: list[pl.Expr] = []
    for check_expr, message in checks:
        rejection_parts.append(
            pl.when(~check_expr).then(pl.lit(message)).otherwise(pl.lit(""))
        )

    # Join non-empty rejection reasons with "; "
    rejection_expr = pl.concat_str(rejection_parts, separator="; ")

    return df.with_columns(
        is_valid_expr.alias("_is_valid"),
        rejection_expr.alias("_rejection_reason"),
    )


def _types_compatible(actual: pl.DataType, expected: pl.DataType) -> bool:
    """Check if an actual Polars type is compatible with the expected type.

    Allows some flexibility (e.g., Datetime with different precision is ok).

    Args:
        actual: The actual column data type.
        expected: The expected column data type.

    Returns:
        True if the types are compatible.
    """
    if actual == expected:
        return True

    # Allow Datetime with any precision/timezone to match
    if isinstance(actual, pl.Datetime) and isinstance(expected, pl.Datetime):
        return True

    # Allow Int types where Float64 is expected (safe upcast)
    return expected == pl.Float64 and actual in (pl.Int32, pl.Int64, pl.Float32)
