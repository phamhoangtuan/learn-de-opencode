"""Unit tests for DuckDB loader operations (T009)."""

from __future__ import annotations

from datetime import UTC, datetime

import duckdb
import polars as pl
import pytest

from src.ingestion.loader import (
    complete_run,
    connect,
    create_run,
    create_tables,
    get_existing_transaction_ids,
    insert_quarantine,
    insert_quarantine_batch,
    insert_transactions,
    table_exists,
)


class TestConnect:
    """Tests for the connect() function."""

    def test_creates_database_file(self, tmp_path):
        db_path = tmp_path / "sub" / "dir" / "test.duckdb"
        conn = connect(db_path)
        try:
            assert db_path.exists()
        finally:
            conn.close()

    def test_creates_parent_directories(self, tmp_path):
        db_path = tmp_path / "nested" / "deep" / "warehouse.duckdb"
        conn = connect(db_path)
        try:
            assert db_path.parent.exists()
        finally:
            conn.close()

    def test_returns_valid_connection(self, tmp_path):
        db_path = tmp_path / "test.duckdb"
        conn = connect(db_path)
        try:
            result = conn.execute("SELECT 1 AS val").fetchone()
            assert result[0] == 1
        finally:
            conn.close()


class TestCreateTables:
    """Tests for the create_tables() function."""

    def test_creates_all_three_tables(self, duckdb_conn):
        create_tables(duckdb_conn)

        assert table_exists(duckdb_conn, "transactions")
        assert table_exists(duckdb_conn, "quarantine")
        assert table_exists(duckdb_conn, "ingestion_runs")

    def test_idempotent_creation(self, duckdb_conn):
        create_tables(duckdb_conn)
        create_tables(duckdb_conn)

        assert table_exists(duckdb_conn, "transactions")

    def test_transactions_columns(self, duckdb_conn):
        create_tables(duckdb_conn)
        cols = _get_column_names(duckdb_conn, "transactions")

        expected = {
            "transaction_id", "timestamp", "amount", "currency",
            "merchant_name", "category", "account_id", "transaction_type",
            "status", "transaction_date", "source_file", "ingested_at",
            "run_id",
        }
        assert cols == expected

    def test_quarantine_columns(self, duckdb_conn):
        create_tables(duckdb_conn)
        cols = _get_column_names(duckdb_conn, "quarantine")

        expected = {
            "quarantine_id", "source_file", "record_data",
            "rejection_reason", "rejected_at", "run_id",
        }
        assert cols == expected

    def test_ingestion_runs_columns(self, duckdb_conn):
        create_tables(duckdb_conn)
        cols = _get_column_names(duckdb_conn, "ingestion_runs")

        expected = {
            "run_id", "started_at", "completed_at", "status",
            "files_processed", "records_loaded", "records_quarantined",
            "duplicates_skipped", "elapsed_seconds",
        }
        assert cols == expected


class TestTableExists:
    """Tests for the table_exists() function."""

    def test_returns_false_for_nonexistent(self, duckdb_conn):
        assert table_exists(duckdb_conn, "nonexistent_table") is False

    def test_returns_true_after_creation(self, duckdb_conn):
        create_tables(duckdb_conn)
        assert table_exists(duckdb_conn, "transactions") is True


class TestInsertTransactions:
    """Tests for the insert_transactions() function."""

    def test_inserts_records(self, duckdb_conn):
        create_tables(duckdb_conn)
        df = _make_transaction_df(count=3)

        inserted = insert_transactions(duckdb_conn, df)

        assert inserted == 3
        row_count = duckdb_conn.execute(
            "SELECT COUNT(*) FROM transactions"
        ).fetchone()[0]
        assert row_count == 3

    def test_empty_dataframe_inserts_nothing(self, duckdb_conn):
        create_tables(duckdb_conn)
        df = _make_transaction_df(count=0)

        inserted = insert_transactions(duckdb_conn, df)

        assert inserted == 0
        row_count = duckdb_conn.execute(
            "SELECT COUNT(*) FROM transactions"
        ).fetchone()[0]
        assert row_count == 0

    def test_preserves_all_columns(self, duckdb_conn):
        create_tables(duckdb_conn)
        df = _make_transaction_df(count=1)

        insert_transactions(duckdb_conn, df)

        row = duckdb_conn.execute(
            "SELECT transaction_id, amount, currency, source_file, run_id "
            "FROM transactions LIMIT 1"
        ).fetchone()
        assert row[0] == "txn-00000"
        assert row[1] == 42.50
        assert row[2] == "USD"
        assert row[3] == "test_file.parquet"
        assert row[4] == "run-001"

    def test_transaction_date_stored_correctly(self, duckdb_conn):
        create_tables(duckdb_conn)
        df = _make_transaction_df(count=1)

        insert_transactions(duckdb_conn, df)

        row = duckdb_conn.execute(
            "SELECT transaction_date FROM transactions LIMIT 1"
        ).fetchone()
        import datetime as dt
        assert row[0] == dt.date(2026, 1, 15)


class TestInsertQuarantine:
    """Tests for quarantine insertion functions."""

    def test_insert_single_quarantine(self, duckdb_conn):
        create_tables(duckdb_conn)

        insert_quarantine(
            duckdb_conn,
            source_file="bad.parquet",
            record_data='{"amount": -5}',
            rejection_reason="amount not positive",
            run_id="run-001",
        )

        row = duckdb_conn.execute(
            "SELECT source_file, record_data, rejection_reason, run_id "
            "FROM quarantine LIMIT 1"
        ).fetchone()
        assert row[0] == "bad.parquet"
        assert row[1] == '{"amount": -5}'
        assert row[2] == "amount not positive"
        assert row[3] == "run-001"

    def test_insert_quarantine_batch(self, duckdb_conn):
        create_tables(duckdb_conn)

        records = [
            {
                "source_file": "f1.parquet",
                "record_data": '{"id": "1"}',
                "rejection_reason": "null amount",
                "run_id": "run-001",
            },
            {
                "source_file": "f1.parquet",
                "record_data": '{"id": "2"}',
                "rejection_reason": "bad currency",
                "run_id": "run-001",
            },
        ]
        count = insert_quarantine_batch(duckdb_conn, records=records)

        assert count == 2
        row_count = duckdb_conn.execute(
            "SELECT COUNT(*) FROM quarantine"
        ).fetchone()[0]
        assert row_count == 2

    def test_insert_quarantine_batch_empty(self, duckdb_conn):
        create_tables(duckdb_conn)
        count = insert_quarantine_batch(duckdb_conn, records=[])
        assert count == 0


class TestRunTracking:
    """Tests for ingestion run create/complete functions."""

    def test_create_run(self, duckdb_conn):
        create_tables(duckdb_conn)

        create_run(
            duckdb_conn,
            run_id="run-test-001",
            started_at="2026-01-15T10:00:00+00:00",
        )

        row = duckdb_conn.execute(
            "SELECT run_id, status FROM ingestion_runs LIMIT 1"
        ).fetchone()
        assert row[0] == "run-test-001"
        assert row[1] == "running"

    def test_complete_run(self, duckdb_conn):
        create_tables(duckdb_conn)
        create_run(
            duckdb_conn,
            run_id="run-test-002",
            started_at="2026-01-15T10:00:00+00:00",
        )

        complete_run(
            duckdb_conn,
            run_id="run-test-002",
            status="completed",
            completed_at="2026-01-15T10:01:30+00:00",
            files_processed=5,
            records_loaded=1000,
            records_quarantined=10,
            duplicates_skipped=3,
            elapsed_seconds=90.5,
        )

        row = duckdb_conn.execute(
            "SELECT status, files_processed, records_loaded, "
            "records_quarantined, duplicates_skipped, elapsed_seconds "
            "FROM ingestion_runs WHERE run_id = 'run-test-002'"
        ).fetchone()
        assert row[0] == "completed"
        assert row[1] == 5
        assert row[2] == 1000
        assert row[3] == 10
        assert row[4] == 3
        assert row[5] == pytest.approx(90.5)

    def test_complete_run_failed_status(self, duckdb_conn):
        create_tables(duckdb_conn)
        create_run(
            duckdb_conn,
            run_id="run-test-003",
            started_at="2026-01-15T10:00:00+00:00",
        )

        complete_run(
            duckdb_conn,
            run_id="run-test-003",
            status="failed",
            completed_at="2026-01-15T10:00:05+00:00",
            files_processed=1,
            records_loaded=0,
            records_quarantined=0,
            duplicates_skipped=0,
            elapsed_seconds=5.0,
        )

        row = duckdb_conn.execute(
            "SELECT status FROM ingestion_runs WHERE run_id = 'run-test-003'"
        ).fetchone()
        assert row[0] == "failed"


class TestGetExistingTransactionIds:
    """Tests for get_existing_transaction_ids()."""

    def test_empty_table_returns_empty_set(self, duckdb_conn):
        create_tables(duckdb_conn)
        ids = get_existing_transaction_ids(duckdb_conn)
        assert ids == set()

    def test_returns_existing_ids(self, duckdb_conn):
        create_tables(duckdb_conn)
        df = _make_transaction_df(count=3)
        insert_transactions(duckdb_conn, df)

        ids = get_existing_transaction_ids(duckdb_conn)
        assert ids == {"txn-00000", "txn-00001", "txn-00002"}


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _get_column_names(conn: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    """Get column names for a table."""
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
        [table],
    ).fetchall()
    return {row[0] for row in rows}


def _make_transaction_df(count: int) -> pl.DataFrame:
    """Create a minimal valid transactions DataFrame with all 13 columns.

    Args:
        count: Number of rows to generate.

    Returns:
        A Polars DataFrame matching the transactions table schema.
    """
    if count == 0:
        return pl.DataFrame(
            schema={
                "transaction_id": pl.Utf8,
                "timestamp": pl.Datetime("us", time_zone="UTC"),
                "amount": pl.Float64,
                "currency": pl.Utf8,
                "merchant_name": pl.Utf8,
                "category": pl.Utf8,
                "account_id": pl.Utf8,
                "transaction_type": pl.Utf8,
                "status": pl.Utf8,
                "transaction_date": pl.Date,
                "source_file": pl.Utf8,
                "ingested_at": pl.Datetime("us", time_zone="UTC"),
                "run_id": pl.Utf8,
            }
        )

    ts = datetime(2026, 1, 15, 10, 30, 0, tzinfo=UTC)
    ingested = datetime(2026, 1, 18, 12, 0, 0, tzinfo=UTC)

    return pl.DataFrame({
        "transaction_id": [f"txn-{i:05d}" for i in range(count)],
        "timestamp": [ts] * count,
        "amount": [42.50 + i for i in range(count)],
        "currency": ["USD"] * count,
        "merchant_name": ["Test Merchant"] * count,
        "category": ["Shopping"] * count,
        "account_id": ["ACC-00001"] * count,
        "transaction_type": ["debit"] * count,
        "status": ["completed"] * count,
        "transaction_date": [ts.date()] * count,
        "source_file": ["test_file.parquet"] * count,
        "ingested_at": [ingested] * count,
        "run_id": ["run-001"] * count,
    })
