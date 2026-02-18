"""Shared test fixtures for the synthetic financial data generator and ingestion pipeline."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl
import pytest

# ---------------------------------------------------------------------------
# Feature 001: Generator fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def default_seed() -> int:
    """Provide a deterministic seed for reproducible tests."""
    return 42


@pytest.fixture
def small_count() -> int:
    """Provide a small record count for fast test execution."""
    return 100


@pytest.fixture
def tmp_output_dir(tmp_path: Path) -> Path:
    """Provide a temporary output directory for generated files.

    Args:
        tmp_path: Pytest built-in temporary directory fixture.

    Returns:
        Path to a temporary 'raw' output directory.
    """
    output_dir = tmp_path / "data" / "raw"
    output_dir.mkdir(parents=True)
    return output_dir


# ---------------------------------------------------------------------------
# Feature 002: Ingestion pipeline fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_db_path(tmp_path: Path) -> Path:
    """Provide a temporary DuckDB database file path.

    Args:
        tmp_path: Pytest built-in temporary directory fixture.

    Returns:
        Path to a temporary DuckDB database file.
    """
    db_dir = tmp_path / "warehouse"
    db_dir.mkdir(parents=True)
    return db_dir / "test.duckdb"


@pytest.fixture
def tmp_source_dir(tmp_path: Path) -> Path:
    """Provide a temporary source directory for Parquet files.

    Args:
        tmp_path: Pytest built-in temporary directory fixture.

    Returns:
        Path to a temporary source directory.
    """
    source_dir = tmp_path / "data" / "raw"
    source_dir.mkdir(parents=True)
    return source_dir


@pytest.fixture
def sample_transactions_df() -> pl.DataFrame:
    """Provide a small valid transactions DataFrame matching the expected schema.

    Returns:
        A Polars DataFrame with 5 valid transaction records.
    """
    return pl.DataFrame({
        "transaction_id": [
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "b2c3d4e5-f6a7-8901-bcde-f12345678901",
            "c3d4e5f6-a7b8-9012-cdef-012345678902",
            "d4e5f6a7-b8c9-0123-defa-123456789013",
            "e5f6a7b8-c9d0-1234-efab-234567890124",
        ],
        "timestamp": [
            datetime(2026, 1, 15, 10, 30, 0, tzinfo=UTC),
            datetime(2026, 1, 15, 11, 45, 0, tzinfo=UTC),
            datetime(2026, 1, 16, 9, 0, 0, tzinfo=UTC),
            datetime(2026, 1, 16, 14, 20, 0, tzinfo=UTC),
            datetime(2026, 1, 17, 8, 15, 0, tzinfo=UTC),
        ],
        "amount": [42.50, 125.00, 8.99, 1500.00, 33.75],
        "currency": ["USD", "EUR", "GBP", "JPY", "USD"],
        "merchant_name": [
            "Whole Foods Market",
            "Amazon",
            "Starbucks",
            "Best Buy",
            "Target",
        ],
        "category": [
            "Groceries",
            "Shopping",
            "Dining",
            "Electronics",
            "Shopping",
        ],
        "account_id": ["ACC-00001", "ACC-00002", "ACC-00001", "ACC-00003", "ACC-00002"],
        "transaction_type": ["debit", "debit", "debit", "debit", "credit"],
        "status": ["completed", "completed", "pending", "completed", "completed"],
    })


@pytest.fixture
def sample_parquet_file(
    tmp_source_dir: Path,
    sample_transactions_df: pl.DataFrame,
) -> Path:
    """Write a sample Parquet file and return its path.

    Args:
        tmp_source_dir: Temporary source directory.
        sample_transactions_df: Valid transactions DataFrame.

    Returns:
        Path to the written Parquet file.
    """
    parquet_path = tmp_source_dir / "transactions_20260115_103000.parquet"
    sample_transactions_df.write_parquet(parquet_path)
    return parquet_path


@pytest.fixture
def duckdb_conn(tmp_db_path: Path) -> duckdb.DuckDBPyConnection:
    """Provide a temporary DuckDB connection that auto-closes after test.

    Args:
        tmp_db_path: Path to the temporary DuckDB database.

    Yields:
        A DuckDB connection.
    """
    conn = duckdb.connect(str(tmp_db_path))
    yield conn
    conn.close()
