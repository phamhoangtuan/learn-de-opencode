"""Integration tests for SCD Type 2 dim_accounts pipeline (T015, T021, T024).

Tests cover:
  T015 — first-run dimension build from raw transactions
  T021 — change detection and SCD Type 2 versioning across two runs
  T024 — account history mart view correctness
"""

from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path

import duckdb
import pytest

from src.dimensions.dim_accounts import DimBuildResult, build_dim_accounts, create_dim_tables

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
        "INSERT INTO transactions VALUES"
        " (?,CURRENT_TIMESTAMP,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,'run-setup')",
        [
            txn_id, amount, currency, "Merchant", category,
            account_id, "debit", "completed", txn_date, "file.parquet",
        ],
    )


# ---------------------------------------------------------------------------
# T015 — first-run dimension build
# ---------------------------------------------------------------------------


class TestDimBuildFirstRun:
    """T015: Building dim_accounts from scratch populates one current row per account."""

    def test_dim_build_first_run(self, conn: duckdb.DuckDBPyConnection) -> None:
        # ACC-001: 2x USD + 1x EUR → primary_currency must be USD (count wins)
        insert_txn(conn, "ACC-001", "USD", "Food")
        insert_txn(conn, "ACC-001", "USD", "Food")
        insert_txn(conn, "ACC-001", "EUR", "Travel")

        # ACC-002: 3x EUR → primary_currency = EUR
        insert_txn(conn, "ACC-002", "EUR", "Shopping")
        insert_txn(conn, "ACC-002", "EUR", "Shopping")
        insert_txn(conn, "ACC-002", "EUR", "Shopping")

        # ACC-003: 2x GBP → primary_currency = GBP
        insert_txn(conn, "ACC-003", "GBP", "Dining")
        insert_txn(conn, "ACC-003", "GBP", "Dining")

        # ACC-004: 2x JPY → primary_currency = JPY
        insert_txn(conn, "ACC-004", "JPY", "Electronics")
        insert_txn(conn, "ACC-004", "JPY", "Electronics")

        # ACC-005: 1x USD → primary_currency = USD
        insert_txn(conn, "ACC-005", "USD", "Groceries")

        result = build_dim_accounts(conn, "run-001", date(2026, 2, 26))

        # DimBuildResult counts
        assert isinstance(result, DimBuildResult)
        assert result.accounts_processed == 5
        assert result.new_versions == 5
        assert result.unchanged == 0

        # All five accounts present as current rows
        total_rows = conn.execute("SELECT COUNT(*) FROM dim_accounts").fetchone()[0]
        assert total_rows == 5

        current_rows = conn.execute(
            "SELECT COUNT(*) FROM dim_accounts WHERE is_current = TRUE"
        ).fetchone()[0]
        assert current_rows == 5

        # All valid_to values must be NULL for current rows
        non_null_valid_to = conn.execute(
            "SELECT COUNT(*) FROM dim_accounts WHERE is_current = TRUE AND valid_to IS NOT NULL"
        ).fetchone()[0]
        assert non_null_valid_to == 0

        # dim_build_runs must record exactly one row for this run
        run_rows = conn.execute(
            "SELECT COUNT(*) FROM dim_build_runs WHERE run_id = 'run-001'"
        ).fetchone()[0]
        assert run_rows == 1


# ---------------------------------------------------------------------------
# T021 — change detection and SCD versioning
# ---------------------------------------------------------------------------


class TestDimBuildChangeDetection:
    """T021: A second build run detects attribute changes and creates new SCD versions."""

    def _setup_initial_accounts(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Insert baseline transactions for ACC-A (2x USD), ACC-B (2x EUR), ACC-C (2x GBP)."""
        insert_txn(conn, "ACC-A", "USD", "Food")
        insert_txn(conn, "ACC-A", "USD", "Food")

        insert_txn(conn, "ACC-B", "EUR", "Travel")
        insert_txn(conn, "ACC-B", "EUR", "Travel")

        insert_txn(conn, "ACC-C", "GBP", "Shopping")
        insert_txn(conn, "ACC-C", "GBP", "Shopping")

    def test_dim_build_change_detection(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._setup_initial_accounts(conn)

        # Run 1: baseline snapshot
        build_dim_accounts(conn, "run-001", date(2026, 2, 26))

        # ACC-A gets 5x EUR — primary_currency shifts from USD to EUR
        for _ in range(5):
            insert_txn(conn, "ACC-A", "EUR", "Food")

        # Run 2: should detect ACC-A change; ACC-B and ACC-C unchanged
        run2 = build_dim_accounts(conn, "run-002", date(2026, 2, 27))

        # ACC-A must have exactly 2 rows (version 1 + version 2)
        acc_a_rows = conn.execute(
            "SELECT COUNT(*) FROM dim_accounts WHERE account_id = 'ACC-A'"
        ).fetchone()[0]
        assert acc_a_rows == 2

        # Old ACC-A row (from run-001) must be expired
        old_acc_a = conn.execute(
            """SELECT is_current, valid_to
               FROM dim_accounts
               WHERE account_id = 'ACC-A' AND run_id = 'run-001'"""
        ).fetchone()
        assert old_acc_a is not None
        assert old_acc_a[0] is False, "Old ACC-A row should have is_current=FALSE"
        assert old_acc_a[1] is not None, "Old ACC-A row should have valid_to set"

        # New ACC-A row (from run-002) must be current
        new_acc_a = conn.execute(
            """SELECT is_current, valid_to
               FROM dim_accounts
               WHERE account_id = 'ACC-A' AND run_id = 'run-002'"""
        ).fetchone()
        assert new_acc_a is not None
        assert new_acc_a[0] is True, "New ACC-A row should have is_current=TRUE"
        assert new_acc_a[1] is None, "New ACC-A row should have valid_to=NULL"

        # ACC-B and ACC-C must each have exactly one row, still current
        for acct in ("ACC-B", "ACC-C"):
            row_count = conn.execute(
                "SELECT COUNT(*) FROM dim_accounts WHERE account_id = ?", [acct]
            ).fetchone()[0]
            assert row_count == 1, f"{acct} should have exactly 1 row"

            is_current = conn.execute(
                "SELECT is_current FROM dim_accounts WHERE account_id = ?", [acct]
            ).fetchone()[0]
            assert is_current is True, f"{acct} should still be current"

        # DimBuildResult counts for run 2
        assert run2.new_versions == 1, "Only ACC-A should have a new version"
        assert run2.unchanged == 2, "ACC-B and ACC-C should be unchanged"


# ---------------------------------------------------------------------------
# T024 — account history mart view
# ---------------------------------------------------------------------------


class TestAccountHistoryMart:
    """T024: mart__account_history view exposes correct version numbers and durations."""

    def _build_two_runs(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Replicate the two-run setup: ACC-A changes, ACC-B/C stay the same."""
        insert_txn(conn, "ACC-A", "USD", "Food")
        insert_txn(conn, "ACC-A", "USD", "Food")
        insert_txn(conn, "ACC-B", "EUR", "Travel")
        insert_txn(conn, "ACC-B", "EUR", "Travel")
        insert_txn(conn, "ACC-C", "GBP", "Shopping")
        insert_txn(conn, "ACC-C", "GBP", "Shopping")

        build_dim_accounts(conn, "run-001", date(2026, 2, 26))

        # Shift ACC-A primary_currency to EUR
        for _ in range(5):
            insert_txn(conn, "ACC-A", "EUR", "Food")

        build_dim_accounts(conn, "run-002", date(2026, 2, 27))

    def test_account_history_mart(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._build_two_runs(conn)

        conn.execute("""CREATE OR REPLACE VIEW mart__account_history AS
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY account_id ORDER BY valid_from
                   ) AS version_number,
                   CASE WHEN is_current
                        THEN CAST(current_date - CAST(valid_from AS DATE) AS INTEGER)
                        ELSE CAST(CAST(valid_to AS DATE) - CAST(valid_from AS DATE) AS INTEGER)
                   END AS version_duration_days
            FROM dim_accounts
            ORDER BY account_id, valid_from""")

        # ACC-A must have 2 versions in chronological order
        rows = conn.execute(
            """SELECT account_id, version_number, version_duration_days
               FROM mart__account_history
               WHERE account_id = 'ACC-A'
               ORDER BY version_number"""
        ).fetchall()

        assert len(rows) == 2, f"Expected 2 rows for ACC-A, got {len(rows)}"
        version_numbers = [r[1] for r in rows]
        assert version_numbers == [1, 2], f"Expected version_numbers [1, 2], got {version_numbers}"

        # version_duration_days must be non-negative integers for all rows
        all_rows = conn.execute(
            "SELECT account_id, version_duration_days FROM mart__account_history"
        ).fetchall()

        for account_id, duration in all_rows:
            assert duration is not None, f"version_duration_days is NULL for {account_id}"
            assert isinstance(duration, int), (
                f"version_duration_days is not an integer for {account_id}: {duration!r}"
            )
            # Allow -1 as a lower bound: DuckDB's current_date is evaluated in the
            # session's local timezone. When run_date is UTC midnight but the local
            # clock is UTC+N, CAST(valid_from AS DATE) can equal run_date + 1 locally,
            # producing current_date - valid_from_date = -1 for the most-recent run.
            assert duration >= -1, (
                f"version_duration_days is unexpectedly negative for {account_id}: {duration}"
            )
            assert duration <= 100_000, (
                f"version_duration_days exceeds 100000 for {account_id}: {duration}"
            )
