"""Unit tests for ingestion pipeline functions (T011)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from src.ingestion.pipeline import (
    discover_parquet_files,
    enrich_with_lineage,
    read_parquet,
)


class TestDiscoverParquetFiles:
    """Tests for Parquet file discovery."""

    def test_finds_parquet_files(self, tmp_source_dir: Path) -> None:
        (tmp_source_dir / "file1.parquet").write_bytes(b"")
        (tmp_source_dir / "file2.parquet").write_bytes(b"")

        files = discover_parquet_files(tmp_source_dir)

        assert len(files) == 2
        assert all(f.suffix == ".parquet" for f in files)

    def test_ignores_non_parquet_files(self, tmp_source_dir: Path) -> None:
        (tmp_source_dir / "data.csv").write_text("a,b\n1,2")
        (tmp_source_dir / "readme.txt").write_text("hello")
        (tmp_source_dir / "file.parquet").write_bytes(b"")

        files = discover_parquet_files(tmp_source_dir)

        assert len(files) == 1
        assert files[0].name == "file.parquet"

    def test_empty_directory_returns_empty_list(self, tmp_source_dir: Path) -> None:
        files = discover_parquet_files(tmp_source_dir)
        assert files == []

    def test_nonexistent_directory_returns_empty_list(self, tmp_path: Path) -> None:
        files = discover_parquet_files(tmp_path / "does_not_exist")
        assert files == []

    def test_returns_sorted_list(self, tmp_source_dir: Path) -> None:
        (tmp_source_dir / "c_file.parquet").write_bytes(b"")
        (tmp_source_dir / "a_file.parquet").write_bytes(b"")
        (tmp_source_dir / "b_file.parquet").write_bytes(b"")

        files = discover_parquet_files(tmp_source_dir)

        names = [f.name for f in files]
        assert names == ["a_file.parquet", "b_file.parquet", "c_file.parquet"]

    def test_does_not_recurse_subdirectories(self, tmp_source_dir: Path) -> None:
        subdir = tmp_source_dir / "subdir"
        subdir.mkdir()
        (subdir / "nested.parquet").write_bytes(b"")
        (tmp_source_dir / "top.parquet").write_bytes(b"")

        files = discover_parquet_files(tmp_source_dir)

        assert len(files) == 1
        assert files[0].name == "top.parquet"


class TestReadParquet:
    """Tests for Parquet file reading."""

    def test_reads_valid_parquet(
        self,
        sample_parquet_file: Path,
        sample_transactions_df: pl.DataFrame,
    ) -> None:
        df = read_parquet(sample_parquet_file)

        assert len(df) == len(sample_transactions_df)
        assert df.columns == sample_transactions_df.columns

    def test_raises_on_missing_file(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            read_parquet(tmp_path / "missing.parquet")

    def test_raises_on_corrupted_file(self, tmp_source_dir: Path) -> None:
        bad_file = tmp_source_dir / "corrupted.parquet"
        bad_file.write_bytes(b"this is not a parquet file")

        with pytest.raises(pl.exceptions.ComputeError):
            read_parquet(bad_file)


class TestEnrichWithLineage:
    """Tests for lineage column enrichment."""

    def test_adds_four_columns(self, sample_transactions_df: pl.DataFrame) -> None:
        enriched = enrich_with_lineage(
            sample_transactions_df,
            source_file="test.parquet",
            run_id="run-001",
        )

        assert "transaction_date" in enriched.columns
        assert "source_file" in enriched.columns
        assert "ingested_at" in enriched.columns
        assert "run_id" in enriched.columns
        assert len(enriched.columns) == len(sample_transactions_df.columns) + 4

    def test_transaction_date_derived_from_timestamp(
        self,
        sample_transactions_df: pl.DataFrame,
    ) -> None:
        enriched = enrich_with_lineage(
            sample_transactions_df,
            source_file="test.parquet",
            run_id="run-001",
        )

        dates = enriched["transaction_date"].to_list()
        # First two records are on 2026-01-15
        import datetime as dt

        assert dates[0] == dt.date(2026, 1, 15)
        assert dates[2] == dt.date(2026, 1, 16)

    def test_source_file_set_correctly(
        self,
        sample_transactions_df: pl.DataFrame,
    ) -> None:
        enriched = enrich_with_lineage(
            sample_transactions_df,
            source_file="my_file.parquet",
            run_id="run-001",
        )

        assert enriched["source_file"].unique().to_list() == ["my_file.parquet"]

    def test_run_id_set_correctly(
        self,
        sample_transactions_df: pl.DataFrame,
    ) -> None:
        enriched = enrich_with_lineage(
            sample_transactions_df,
            source_file="test.parquet",
            run_id="run-xyz",
        )

        assert enriched["run_id"].unique().to_list() == ["run-xyz"]

    def test_ingested_at_is_recent(
        self,
        sample_transactions_df: pl.DataFrame,
    ) -> None:
        before = datetime.now(UTC)
        enriched = enrich_with_lineage(
            sample_transactions_df,
            source_file="test.parquet",
            run_id="run-001",
        )
        after = datetime.now(UTC)

        ingested = enriched["ingested_at"][0]
        # ingested_at should be between before and after
        assert before <= ingested <= after

    def test_preserves_original_columns(
        self,
        sample_transactions_df: pl.DataFrame,
    ) -> None:
        original_cols = sample_transactions_df.columns
        enriched = enrich_with_lineage(
            sample_transactions_df,
            source_file="test.parquet",
            run_id="run-001",
        )

        for col in original_cols:
            assert col in enriched.columns
        assert len(enriched) == len(sample_transactions_df)
