"""Integration tests for the ingestion pipeline (T013)."""

from __future__ import annotations

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
