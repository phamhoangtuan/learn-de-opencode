"""Data quality tests for the ingestion pipeline (6Cs validation).

Validates Completeness, Consistency, Conformity, Correctness, Currency,
and Coverage dimensions on warehouse data after ingestion.
"""

from __future__ import annotations

import datetime as dt
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl
import pytest

from src.ingestion.pipeline import run_pipeline


@pytest.fixture
def _multi_file_warehouse(
    tmp_source_dir: Path,
    tmp_db_path: Path,
) -> Path:
    """Ingest multiple files into a warehouse and return the DB path.

    Creates two Parquet files with distinct records spanning multiple dates,
    currencies, and statuses, then runs the pipeline.
    """
    df1 = pl.DataFrame({
        "transaction_id": [f"q-{i}" for i in range(1, 6)],
        "timestamp": [
            datetime(2026, 1, 15, 10, 0, 0, tzinfo=UTC),
            datetime(2026, 1, 15, 11, 0, 0, tzinfo=UTC),
            datetime(2026, 1, 16, 9, 0, 0, tzinfo=UTC),
            datetime(2026, 1, 16, 14, 0, 0, tzinfo=UTC),
            datetime(2026, 1, 17, 8, 0, 0, tzinfo=UTC),
        ],
        "amount": [42.50, 125.00, 8.99, 1500.00, 33.75],
        "currency": ["USD", "EUR", "GBP", "JPY", "USD"],
        "merchant_name": [
            "Whole Foods", "Amazon", "Starbucks", "Best Buy", "Target",
        ],
        "category": [
            "Groceries", "Shopping", "Dining", "Electronics", "Shopping",
        ],
        "account_id": [
            "ACC-00001", "ACC-00002", "ACC-00001", "ACC-00003", "ACC-00002",
        ],
        "transaction_type": ["debit", "debit", "debit", "debit", "credit"],
        "status": ["completed", "completed", "pending", "completed", "completed"],
    })

    df2 = pl.DataFrame({
        "transaction_id": [f"q-{i}" for i in range(6, 9)],
        "timestamp": [
            datetime(2026, 1, 17, 15, 0, 0, tzinfo=UTC),
            datetime(2026, 1, 18, 10, 0, 0, tzinfo=UTC),
            datetime(2026, 1, 18, 12, 0, 0, tzinfo=UTC),
        ],
        "amount": [55.00, 200.00, 15.50],
        "currency": ["EUR", "USD", "GBP"],
        "merchant_name": ["Lidl", "Costco", "Pret A Manger"],
        "category": ["Groceries", "Shopping", "Dining"],
        "account_id": ["ACC-00001", "ACC-00004", "ACC-00003"],
        "transaction_type": ["debit", "credit", "debit"],
        "status": ["completed", "completed", "completed"],
    })

    df1.write_parquet(tmp_source_dir / "batch_001.parquet")
    df2.write_parquet(tmp_source_dir / "batch_002.parquet")

    run_pipeline(source_dir=tmp_source_dir, db_path=tmp_db_path)
    return tmp_db_path


class TestCompleteness:
    """Verify no data loss during ingestion — all valid records are present."""

    def test_all_records_loaded(self, _multi_file_warehouse: Path) -> None:
        """Given 8 valid records across 2 files, all 8 are in the warehouse."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
            assert count == 8
        finally:
            conn.close()

    def test_no_null_primary_keys(self, _multi_file_warehouse: Path) -> None:
        """Every record must have a non-null transaction_id."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            nulls = conn.execute(
                "SELECT COUNT(*) FROM transactions WHERE transaction_id IS NULL"
            ).fetchone()[0]
            assert nulls == 0
        finally:
            conn.close()

    def test_no_null_required_fields(self, _multi_file_warehouse: Path) -> None:
        """All required columns must be non-null for every record."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            required = [
                "transaction_id", "timestamp", "amount", "currency",
                "merchant_name", "category", "account_id",
                "transaction_type", "status",
                "transaction_date", "source_file", "ingested_at", "run_id",
            ]
            for col in required:
                nulls = conn.execute(
                    f"SELECT COUNT(*) FROM transactions WHERE {col} IS NULL"
                ).fetchone()[0]
                assert nulls == 0, f"Column {col} has {nulls} null values"
        finally:
            conn.close()

    def test_all_source_files_represented(self, _multi_file_warehouse: Path) -> None:
        """Both source files must appear in lineage data."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            files = {
                row[0]
                for row in conn.execute(
                    "SELECT DISTINCT source_file FROM transactions"
                ).fetchall()
            }
            assert files == {"batch_001.parquet", "batch_002.parquet"}
        finally:
            conn.close()


class TestConsistency:
    """Verify data relationships and referential integrity."""

    def test_unique_transaction_ids(self, _multi_file_warehouse: Path) -> None:
        """No duplicate transaction_ids in the warehouse."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            total = conn.execute(
                "SELECT COUNT(*) FROM transactions"
            ).fetchone()[0]
            distinct = conn.execute(
                "SELECT COUNT(DISTINCT transaction_id) FROM transactions"
            ).fetchone()[0]
            assert total == distinct
        finally:
            conn.close()

    def test_transaction_date_matches_timestamp(
        self, _multi_file_warehouse: Path,
    ) -> None:
        """transaction_date must equal the DATE portion of timestamp for all records."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            mismatches = conn.execute(
                "SELECT COUNT(*) FROM transactions "
                "WHERE transaction_date != CAST(timestamp AS DATE)"
            ).fetchone()[0]
            assert mismatches == 0
        finally:
            conn.close()

    def test_run_id_consistent_within_file(
        self, _multi_file_warehouse: Path,
    ) -> None:
        """All records from the same source_file must share the same run_id."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            rows = conn.execute(
                "SELECT source_file, COUNT(DISTINCT run_id) as run_count "
                "FROM transactions "
                "GROUP BY source_file"
            ).fetchall()
            for row in rows:
                assert row[1] == 1, (
                    f"File {row[0]} has {row[1]} distinct run_ids, expected 1"
                )
        finally:
            conn.close()

    def test_ingestion_runs_matches_transaction_counts(
        self, _multi_file_warehouse: Path,
    ) -> None:
        """ingestion_runs.records_loaded must match actual record counts."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            run_loaded = conn.execute(
                "SELECT SUM(records_loaded) FROM ingestion_runs"
            ).fetchone()[0]
            actual = conn.execute(
                "SELECT COUNT(*) FROM transactions"
            ).fetchone()[0]
            assert run_loaded == actual
        finally:
            conn.close()


class TestConformity:
    """Verify data values conform to defined schemas and rules."""

    def test_currencies_in_valid_set(self, _multi_file_warehouse: Path) -> None:
        """All currency values must be in {USD, EUR, GBP, JPY}."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            currencies = {
                row[0]
                for row in conn.execute(
                    "SELECT DISTINCT currency FROM transactions"
                ).fetchall()
            }
            valid = {"USD", "EUR", "GBP", "JPY"}
            assert currencies.issubset(valid), f"Invalid currencies: {currencies - valid}"
        finally:
            conn.close()

    def test_transaction_types_in_valid_set(
        self, _multi_file_warehouse: Path,
    ) -> None:
        """All transaction_type values must be in {debit, credit}."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            types = {
                row[0]
                for row in conn.execute(
                    "SELECT DISTINCT transaction_type FROM transactions"
                ).fetchall()
            }
            assert types.issubset({"debit", "credit"})
        finally:
            conn.close()

    def test_statuses_in_valid_set(self, _multi_file_warehouse: Path) -> None:
        """All status values must be in {completed, pending, failed}."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            statuses = {
                row[0]
                for row in conn.execute(
                    "SELECT DISTINCT status FROM transactions"
                ).fetchall()
            }
            assert statuses.issubset({"completed", "pending", "failed"})
        finally:
            conn.close()

    def test_amounts_are_positive(self, _multi_file_warehouse: Path) -> None:
        """All amount values must be strictly positive."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            non_positive = conn.execute(
                "SELECT COUNT(*) FROM transactions WHERE amount <= 0"
            ).fetchone()[0]
            assert non_positive == 0
        finally:
            conn.close()

    def test_account_id_format(self, _multi_file_warehouse: Path) -> None:
        """All account_id values must match ACC-XXXXX format."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            invalid = conn.execute(
                "SELECT COUNT(*) FROM transactions "
                "WHERE NOT regexp_matches(account_id, '^ACC-\\d{5}$')"
            ).fetchone()[0]
            assert invalid == 0
        finally:
            conn.close()


class TestCorrectness:
    """Verify derived and computed values are correct."""

    def test_transaction_date_values(self, _multi_file_warehouse: Path) -> None:
        """transaction_date must contain the expected dates from source data."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            dates = {
                row[0]
                for row in conn.execute(
                    "SELECT DISTINCT transaction_date FROM transactions "
                    "ORDER BY transaction_date"
                ).fetchall()
            }
            expected = {
                dt.date(2026, 1, 15),
                dt.date(2026, 1, 16),
                dt.date(2026, 1, 17),
                dt.date(2026, 1, 18),
            }
            assert dates == expected
        finally:
            conn.close()

    def test_date_record_counts(self, _multi_file_warehouse: Path) -> None:
        """Each date must have the correct number of records."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            rows = conn.execute(
                "SELECT transaction_date, COUNT(*) as cnt "
                "FROM transactions "
                "GROUP BY transaction_date "
                "ORDER BY transaction_date"
            ).fetchall()
            counts = {row[0]: row[1] for row in rows}
            assert counts[dt.date(2026, 1, 15)] == 2
            assert counts[dt.date(2026, 1, 16)] == 2
            assert counts[dt.date(2026, 1, 17)] == 2
            assert counts[dt.date(2026, 1, 18)] == 2
        finally:
            conn.close()

    def test_ingestion_run_completed_status(
        self, _multi_file_warehouse: Path,
    ) -> None:
        """All ingestion runs must have 'completed' status."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            statuses = {
                row[0]
                for row in conn.execute(
                    "SELECT DISTINCT status FROM ingestion_runs"
                ).fetchall()
            }
            assert statuses == {"completed"}
        finally:
            conn.close()


class TestCurrency:
    """Verify data freshness — ingested_at reflects actual ingestion time."""

    def test_ingested_at_is_recent(self, _multi_file_warehouse: Path) -> None:
        """All ingested_at values must be within a reasonable window of now."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            rows = conn.execute(
                "SELECT MIN(ingested_at), MAX(ingested_at) FROM transactions"
            ).fetchone()
            min_ts = rows[0]
            max_ts = rows[1]
            now = datetime.now(UTC)
            # Ingested_at should be within 60 seconds of now
            assert (now - min_ts).total_seconds() < 60
            assert (now - max_ts).total_seconds() < 60
        finally:
            conn.close()

    def test_run_elapsed_seconds_reasonable(
        self, _multi_file_warehouse: Path,
    ) -> None:
        """Pipeline elapsed time should be reasonable (< 30 seconds for 8 records)."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            elapsed = conn.execute(
                "SELECT elapsed_seconds FROM ingestion_runs"
            ).fetchone()[0]
            assert elapsed is not None
            assert elapsed < 30.0
            assert elapsed >= 0.0
        finally:
            conn.close()


class TestCoverage:
    """Verify warehouse schema has all expected tables and columns."""

    def test_all_tables_exist(self, _multi_file_warehouse: Path) -> None:
        """Warehouse must contain transactions, quarantine, and ingestion_runs."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT table_name FROM information_schema.tables"
                ).fetchall()
            }
            assert {"transactions", "quarantine", "ingestion_runs"}.issubset(tables)
        finally:
            conn.close()

    def test_transactions_has_13_columns(
        self, _multi_file_warehouse: Path,
    ) -> None:
        """Transactions table must have exactly 13 columns."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            cols = conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'transactions' "
                "ORDER BY ordinal_position"
            ).fetchall()
            col_names = [row[0] for row in cols]
            expected = [
                "transaction_id", "timestamp", "amount", "currency",
                "merchant_name", "category", "account_id",
                "transaction_type", "status",
                "transaction_date", "source_file", "ingested_at", "run_id",
            ]
            assert col_names == expected
        finally:
            conn.close()

    def test_quarantine_has_6_columns(self, _multi_file_warehouse: Path) -> None:
        """Quarantine table must have exactly 6 columns."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            cols = conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'quarantine' "
                "ORDER BY ordinal_position"
            ).fetchall()
            col_names = [row[0] for row in cols]
            expected = [
                "quarantine_id", "source_file", "record_data",
                "rejection_reason", "rejected_at", "run_id",
            ]
            assert col_names == expected
        finally:
            conn.close()

    def test_ingestion_runs_has_9_columns(
        self, _multi_file_warehouse: Path,
    ) -> None:
        """Ingestion_runs table must have exactly 9 columns."""
        conn = duckdb.connect(str(_multi_file_warehouse))
        try:
            cols = conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'ingestion_runs' "
                "ORDER BY ordinal_position"
            ).fetchall()
            col_names = [row[0] for row in cols]
            expected = [
                "run_id", "started_at", "completed_at", "status",
                "files_processed", "records_loaded", "records_quarantined",
                "duplicates_skipped", "elapsed_seconds",
            ]
            assert col_names == expected
        finally:
            conn.close()
