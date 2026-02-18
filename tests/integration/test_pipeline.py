"""Integration tests for the ingestion pipeline (T013, T024)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl

from src.ingestion.models import RunStatus
from src.ingestion.pipeline import run_pipeline


class TestEndToEndIngestion:
    """Integration tests for Parquet → DuckDB ingestion."""

    def test_ingests_single_file(
        self,
        tmp_source_dir: Path,
        tmp_db_path: Path,
        sample_transactions_df: pl.DataFrame,
    ) -> None:
        """Given a Parquet file, When pipeline runs, Then all records are in DuckDB."""
        _write_parquet(tmp_source_dir, "txns_001.parquet", sample_transactions_df)

        result = run_pipeline(source_dir=tmp_source_dir, db_path=tmp_db_path)

        assert result.status == RunStatus.COMPLETED
        assert result.files_processed == 1
        assert result.records_loaded == 5

        # Verify records in DuckDB
        conn = duckdb.connect(str(tmp_db_path))
        try:
            count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
            assert count == 5
        finally:
            conn.close()

    def test_ingests_multiple_files(
        self,
        tmp_source_dir: Path,
        tmp_db_path: Path,
        sample_transactions_df: pl.DataFrame,
    ) -> None:
        """Given multiple files, When pipeline runs, Then all records loaded."""
        df1 = sample_transactions_df.head(3).with_columns(
            pl.Series("transaction_id", ["id-a", "id-b", "id-c"])
        )
        df2 = sample_transactions_df.head(2).with_columns(
            pl.Series("transaction_id", ["id-d", "id-e"])
        )
        _write_parquet(tmp_source_dir, "batch_001.parquet", df1)
        _write_parquet(tmp_source_dir, "batch_002.parquet", df2)

        result = run_pipeline(source_dir=tmp_source_dir, db_path=tmp_db_path)

        assert result.files_processed == 2
        assert result.records_loaded == 5

        conn = duckdb.connect(str(tmp_db_path))
        try:
            count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
            assert count == 5
        finally:
            conn.close()

    def test_empty_directory_succeeds(
        self,
        tmp_source_dir: Path,
        tmp_db_path: Path,
    ) -> None:
        """Given empty directory, When pipeline runs, Then succeeds with zero records."""
        result = run_pipeline(source_dir=tmp_source_dir, db_path=tmp_db_path)

        assert result.status == RunStatus.COMPLETED
        assert result.files_processed == 0
        assert result.records_loaded == 0

    def test_creates_database_on_first_run(
        self,
        tmp_source_dir: Path,
        tmp_path: Path,
    ) -> None:
        """Given no existing DB, When pipeline runs, Then DB and tables are created."""
        db_path = tmp_path / "new_warehouse" / "test.duckdb"
        assert not db_path.exists()

        result = run_pipeline(source_dir=tmp_source_dir, db_path=db_path)

        assert result.status == RunStatus.COMPLETED
        assert db_path.exists()

        conn = duckdb.connect(str(db_path))
        try:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT table_name FROM information_schema.tables"
                ).fetchall()
            }
            assert "transactions" in tables
            assert "quarantine" in tables
            assert "ingestion_runs" in tables
        finally:
            conn.close()

    def test_records_queryable_after_ingestion(
        self,
        tmp_source_dir: Path,
        tmp_db_path: Path,
        sample_transactions_df: pl.DataFrame,
    ) -> None:
        """Given ingested records, When queried, Then all columns are present."""
        _write_parquet(tmp_source_dir, "txns.parquet", sample_transactions_df)

        run_pipeline(source_dir=tmp_source_dir, db_path=tmp_db_path)

        conn = duckdb.connect(str(tmp_db_path))
        try:
            row = conn.execute(
                "SELECT transaction_id, amount, currency, source_file, run_id, "
                "transaction_date, ingested_at "
                "FROM transactions LIMIT 1"
            ).fetchone()
            # All fields should be non-null
            assert all(v is not None for v in row)
        finally:
            conn.close()

    def test_lineage_columns_populated(
        self,
        tmp_source_dir: Path,
        tmp_db_path: Path,
        sample_transactions_df: pl.DataFrame,
    ) -> None:
        """Given ingested records, When queried, Then lineage columns are set."""
        _write_parquet(tmp_source_dir, "txns_lineage.parquet", sample_transactions_df)

        result = run_pipeline(source_dir=tmp_source_dir, db_path=tmp_db_path)

        conn = duckdb.connect(str(tmp_db_path))
        try:
            rows = conn.execute(
                "SELECT DISTINCT source_file, run_id FROM transactions"
            ).fetchall()
            assert len(rows) == 1
            assert rows[0][0] == "txns_lineage.parquet"
            assert rows[0][1] == result.run_id
        finally:
            conn.close()

    def test_run_tracked_in_ingestion_runs(
        self,
        tmp_source_dir: Path,
        tmp_db_path: Path,
        sample_transactions_df: pl.DataFrame,
    ) -> None:
        """Given a pipeline run, When complete, Then ingestion_runs table updated."""
        _write_parquet(tmp_source_dir, "txns.parquet", sample_transactions_df)

        result = run_pipeline(source_dir=tmp_source_dir, db_path=tmp_db_path)

        conn = duckdb.connect(str(tmp_db_path))
        try:
            row = conn.execute(
                "SELECT run_id, status, files_processed, records_loaded "
                "FROM ingestion_runs WHERE run_id = ?",
                [result.run_id],
            ).fetchone()
            assert row[0] == result.run_id
            assert row[1] == "completed"
            assert row[2] == 1
            assert row[3] == 5
        finally:
            conn.close()

    def test_summary_contains_correct_counts(
        self,
        tmp_source_dir: Path,
        tmp_db_path: Path,
        sample_transactions_df: pl.DataFrame,
    ) -> None:
        """Given a completed run, When summary generated, Then counts are accurate."""
        _write_parquet(tmp_source_dir, "txns.parquet", sample_transactions_df)

        result = run_pipeline(source_dir=tmp_source_dir, db_path=tmp_db_path)
        summary = result.summary()

        assert result.run_id in summary
        assert "completed" in summary
        assert "1" in summary   # files processed
        assert "5" in summary   # records loaded

    def test_transaction_date_column_populated(
        self,
        tmp_source_dir: Path,
        tmp_db_path: Path,
        sample_transactions_df: pl.DataFrame,
    ) -> None:
        """Given ingested records, When queried by date, Then date column is correct."""
        _write_parquet(tmp_source_dir, "txns.parquet", sample_transactions_df)

        run_pipeline(source_dir=tmp_source_dir, db_path=tmp_db_path)

        conn = duckdb.connect(str(tmp_db_path))
        try:
            rows = conn.execute(
                "SELECT DISTINCT transaction_date FROM transactions "
                "ORDER BY transaction_date"
            ).fetchall()
            import datetime as dt
            dates = [row[0] for row in rows]
            # Sample data spans 2026-01-15, 2026-01-16, 2026-01-17
            assert dt.date(2026, 1, 15) in dates
            assert dt.date(2026, 1, 16) in dates
            assert dt.date(2026, 1, 17) in dates
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _write_parquet(directory: Path, filename: str, df: pl.DataFrame) -> Path:
    """Write a DataFrame as a Parquet file in the given directory."""
    path = directory / filename
    df.write_parquet(path)
    return path


def _make_invalid_records_df() -> pl.DataFrame:
    """Create a DataFrame with a mix of valid and invalid records."""
    return pl.DataFrame({
        "transaction_id": [
            "valid-001",
            "invalid-neg-amount",
            "invalid-bad-currency",
            "valid-002",
            "invalid-bad-status",
        ],
        "timestamp": [
            datetime(2026, 1, 15, 10, 0, 0, tzinfo=UTC),
            datetime(2026, 1, 15, 11, 0, 0, tzinfo=UTC),
            datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC),
            datetime(2026, 1, 15, 13, 0, 0, tzinfo=UTC),
            datetime(2026, 1, 15, 14, 0, 0, tzinfo=UTC),
        ],
        "amount": [10.00, -5.00, 20.00, 30.00, 40.00],
        "currency": ["USD", "USD", "XXX", "EUR", "GBP"],
        "merchant_name": [
            "Shop A", "Shop B", "Shop C", "Shop D", "Shop E",
        ],
        "category": [
            "Groceries", "Groceries", "Groceries", "Groceries", "Groceries",
        ],
        "account_id": [
            "ACC-00001", "ACC-00002", "ACC-00003", "ACC-00004", "ACC-00005",
        ],
        "transaction_type": ["debit", "debit", "debit", "credit", "debit"],
        "status": [
            "completed", "completed", "completed", "completed", "invalid_status",
        ],
    })


# ---------------------------------------------------------------------------
# Phase 4 (T024): Validation + quarantine integration tests
# ---------------------------------------------------------------------------


class TestValidationIntegration:
    """Integration tests for schema validation and quarantine routing."""

    def test_mixed_valid_invalid_records(
        self,
        tmp_source_dir: Path,
        tmp_db_path: Path,
    ) -> None:
        """Given mixed records, valid load to transactions, invalid to quarantine."""
        df = _make_invalid_records_df()
        _write_parquet(tmp_source_dir, "mixed.parquet", df)

        result = run_pipeline(source_dir=tmp_source_dir, db_path=tmp_db_path)

        assert result.status == RunStatus.COMPLETED
        assert result.records_loaded == 2
        assert result.records_quarantined == 3

        conn = duckdb.connect(str(tmp_db_path))
        try:
            txn_count = conn.execute(
                "SELECT COUNT(*) FROM transactions"
            ).fetchone()[0]
            assert txn_count == 2

            quar_count = conn.execute(
                "SELECT COUNT(*) FROM quarantine"
            ).fetchone()[0]
            assert quar_count == 3
        finally:
            conn.close()

    def test_quarantine_contains_rejection_reason(
        self,
        tmp_source_dir: Path,
        tmp_db_path: Path,
    ) -> None:
        """Given invalid records, quarantine stores rejection reason."""
        df = _make_invalid_records_df()
        _write_parquet(tmp_source_dir, "mixed.parquet", df)

        run_pipeline(source_dir=tmp_source_dir, db_path=tmp_db_path)

        conn = duckdb.connect(str(tmp_db_path))
        try:
            rows = conn.execute(
                "SELECT record_data, rejection_reason FROM quarantine"
            ).fetchall()
            assert len(rows) == 3
            # Each quarantine record should have a non-empty rejection reason
            for row in rows:
                assert row[0]  # record_data is not empty
                assert row[1]  # rejection_reason is not empty
        finally:
            conn.close()

    def test_quarantine_lineage_matches_run(
        self,
        tmp_source_dir: Path,
        tmp_db_path: Path,
    ) -> None:
        """Given quarantined records, lineage columns link to correct run."""
        df = _make_invalid_records_df()
        _write_parquet(tmp_source_dir, "mixed.parquet", df)

        result = run_pipeline(source_dir=tmp_source_dir, db_path=tmp_db_path)

        conn = duckdb.connect(str(tmp_db_path))
        try:
            rows = conn.execute(
                "SELECT DISTINCT source_file, run_id FROM quarantine"
            ).fetchall()
            assert len(rows) == 1
            assert rows[0][0] == "mixed.parquet"
            assert rows[0][1] == result.run_id
        finally:
            conn.close()

    def test_schema_failure_quarantines_entire_file(
        self,
        tmp_source_dir: Path,
        tmp_db_path: Path,
    ) -> None:
        """Given a file with wrong schema, entire file is rejected."""
        bad_df = pl.DataFrame({
            "wrong_column": ["a", "b"],
            "another_wrong": [1, 2],
        })
        _write_parquet(tmp_source_dir, "bad_schema.parquet", bad_df)

        result = run_pipeline(source_dir=tmp_source_dir, db_path=tmp_db_path)

        assert result.status == RunStatus.COMPLETED
        assert result.records_loaded == 0
        assert result.files_processed == 1
        # Schema failure is treated as a file-level error
        assert result.file_results[0].error is not None

        conn = duckdb.connect(str(tmp_db_path))
        try:
            txn_count = conn.execute(
                "SELECT COUNT(*) FROM transactions"
            ).fetchone()[0]
            assert txn_count == 0
        finally:
            conn.close()

    def test_all_valid_records_none_quarantined(
        self,
        tmp_source_dir: Path,
        tmp_db_path: Path,
        sample_transactions_df: pl.DataFrame,
    ) -> None:
        """Given all valid records, zero quarantined, all loaded."""
        _write_parquet(tmp_source_dir, "valid.parquet", sample_transactions_df)

        result = run_pipeline(source_dir=tmp_source_dir, db_path=tmp_db_path)

        assert result.records_loaded == 5
        assert result.records_quarantined == 0

    def test_run_summary_includes_quarantine_count(
        self,
        tmp_source_dir: Path,
        tmp_db_path: Path,
    ) -> None:
        """Given quarantined records, run summary reflects count (T029)."""
        df = _make_invalid_records_df()
        _write_parquet(tmp_source_dir, "mixed.parquet", df)

        result = run_pipeline(source_dir=tmp_source_dir, db_path=tmp_db_path)
        summary = result.summary()

        assert "3" in summary  # records quarantined
        assert "2" in summary  # records loaded

    def test_ingestion_runs_records_quarantine_count(
        self,
        tmp_source_dir: Path,
        tmp_db_path: Path,
    ) -> None:
        """Given quarantined records, ingestion_runs table tracks count."""
        df = _make_invalid_records_df()
        _write_parquet(tmp_source_dir, "mixed.parquet", df)

        result = run_pipeline(source_dir=tmp_source_dir, db_path=tmp_db_path)

        conn = duckdb.connect(str(tmp_db_path))
        try:
            row = conn.execute(
                "SELECT records_loaded, records_quarantined "
                "FROM ingestion_runs WHERE run_id = ?",
                [result.run_id],
            ).fetchone()
            assert row[0] == 2
            assert row[1] == 3
        finally:
            conn.close()
