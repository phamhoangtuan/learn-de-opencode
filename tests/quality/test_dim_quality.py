"""Data quality checks for the SCD Type 2 dim_accounts dimension (T029).

Tests verify that the SQL check files in src/checks/ correctly:
  - pass on clean, properly built dim_accounts data
  - detect intentionally injected violations
"""

from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path

import duckdb
import pytest

from src.dimensions.dim_accounts import build_dim_accounts, create_dim_tables

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def conn():
    """Provide an in-memory DuckDB connection with transactions table and dim tables ready."""
    c = duckdb.connect(":memory:")
    c.execute("""CREATE TABLE transactions (
        transaction_id VARCHAR PRIMARY KEY,
        "timestamp" TIMESTAMPTZ NOT NULL,
        amount DOUBLE NOT NULL,
        currency VARCHAR NOT NULL,
        merchant_name VARCHAR NOT NULL,
        category VARCHAR NOT NULL,
        account_id VARCHAR NOT NULL,
        transaction_type VARCHAR NOT NULL,
        status VARCHAR NOT NULL,
        transaction_date DATE NOT NULL,
        source_file VARCHAR NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL,
        run_id VARCHAR NOT NULL
    )""")
    c.execute("""CREATE OR REPLACE VIEW stg_transactions AS
        SELECT transaction_id, "timestamp" AS transaction_timestamp, transaction_date,
               amount, currency, merchant_name, category, account_id,
               transaction_type, status, source_file, ingested_at, run_id
        FROM transactions""")
    create_dim_tables(c)
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def insert_txn(
    conn: duckdb.DuckDBPyConnection,
    account_id: str,
    currency: str,
    category: str,
    amount: float = 50.0,
    txn_date: str = "2026-01-15",
) -> None:
    """Insert a single transaction row using a UUID primary key."""
    txn_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO transactions VALUES (?,CURRENT_TIMESTAMP,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,'run-setup')",
        [txn_id, amount, currency, "Merchant", category, account_id, "debit", "completed", txn_date, "file.parquet"],
    )


# ---------------------------------------------------------------------------
# T029 — quality check SQL files
# ---------------------------------------------------------------------------


class TestDimQualityChecks:
    """T029: Quality check SQL files return zero rows on clean data and detect violations."""

    def test_all_checks_pass_on_clean_data(self, conn: duckdb.DuckDBPyConnection) -> None:
        """All three dim_accounts check files must return zero rows on a clean dimension build."""
        insert_txn(conn, "ACC-Q1", "USD", "Food")
        insert_txn(conn, "ACC-Q2", "EUR", "Travel")
        insert_txn(conn, "ACC-Q3", "GBP", "Shopping")

        build_dim_accounts(conn, "run-q01", date(2026, 2, 26))

        check_dir = PROJECT_ROOT / "src" / "checks"
        check_files = [
            "check__dim_accounts_single_current.sql",
            "check__dim_accounts_no_overlapping_ranges.sql",
            "check__dim_accounts_no_null_sk.sql",
        ]

        for fname in check_files:
            sql = (check_dir / fname).read_text()
            result = conn.execute(sql).fetchall()
            assert result == [], f"Check {fname} failed with violations: {result}"

    def test_single_current_check_detects_violation(self, conn: duckdb.DuckDBPyConnection) -> None:
        """check__dim_accounts_single_current.sql must detect a duplicate current row."""
        insert_txn(conn, "ACC-DUP", "USD", "Food")
        build_dim_accounts(conn, "run-q02", date(2026, 2, 26))

        # Manually inject a second is_current=TRUE row for the same account_id
        conn.execute("""INSERT INTO dim_accounts
            (account_id, primary_currency, primary_category, transaction_count,
             total_spend, first_seen, last_seen, row_hash, valid_from, valid_to, is_current, run_id)
            VALUES ('ACC-DUP','EUR','Travel',5,200.0,'2026-01-01','2026-01-15','bad-hash',
            '2026-01-01T00:00:00+00:00',NULL,TRUE,'run-bad')""")

        check_sql = (
            PROJECT_ROOT / "src" / "checks" / "check__dim_accounts_single_current.sql"
        ).read_text()
        result = conn.execute(check_sql).fetchall()

        assert len(result) >= 1, "Expected violation but check returned no rows"

    def test_no_null_sk_check_passes_on_clean_data(self, conn: duckdb.DuckDBPyConnection) -> None:
        """check__dim_accounts_no_null_sk.sql must pass (return zero rows) after a clean build."""
        insert_txn(conn, "ACC-SKQ", "USD", "Food")
        build_dim_accounts(conn, "run-q03", date(2026, 2, 26))

        check_sql = (
            PROJECT_ROOT / "src" / "checks" / "check__dim_accounts_no_null_sk.sql"
        ).read_text()
        result = conn.execute(check_sql).fetchall()

        assert result == []

    def test_no_overlapping_ranges_check_passes_on_clean_data(
        self, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """check__dim_accounts_no_overlapping_ranges.sql must pass after two clean SCD runs."""
        insert_txn(conn, "ACC-OVL", "USD", "Food")
        insert_txn(conn, "ACC-OVL", "USD", "Food")
        build_dim_accounts(conn, "run-q04a", date(2026, 2, 26))

        # Add transactions that change the profile so a second version is created
        for _ in range(5):
            insert_txn(conn, "ACC-OVL", "EUR", "Food")
        build_dim_accounts(conn, "run-q04b", date(2026, 2, 27))

        check_sql = (
            PROJECT_ROOT / "src" / "checks" / "check__dim_accounts_no_overlapping_ranges.sql"
        ).read_text()
        result = conn.execute(check_sql).fetchall()

        assert result == [], f"Overlapping ranges check failed: {result}"
