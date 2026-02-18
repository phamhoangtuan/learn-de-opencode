"""Integration tests for end-to-end transaction generation."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from src.generate_transactions import main


class TestEndToEndGeneration:
    """Tests for the complete generation pipeline."""

    def test_generates_exact_record_count(self, tmp_output_dir: Path) -> None:
        """Output file must contain exactly the requested number of records."""
        exit_code = main([
            "--count", "1000",
            "--seed", "42",
            "--format", "parquet",
            "--output-dir", str(tmp_output_dir),
        ])
        assert exit_code == 0

        files = list(tmp_output_dir.glob("transactions_*.parquet"))
        assert len(files) == 1
        df = pl.read_parquet(files[0])
        assert len(df) == 1000

    def test_all_required_fields_present(self, tmp_output_dir: Path) -> None:
        """Output must contain all 9 required fields per FR-001."""
        main([
            "--count", "100",
            "--seed", "42",
            "--output-dir", str(tmp_output_dir),
        ])

        files = list(tmp_output_dir.glob("transactions_*.parquet"))
        df = pl.read_parquet(files[0])

        required_fields = {
            "transaction_id", "timestamp", "amount", "currency",
            "merchant_name", "category", "account_id",
            "transaction_type", "status",
        }
        assert set(df.columns) == required_fields

    def test_no_null_values_in_required_fields(self, tmp_output_dir: Path) -> None:
        """No required field may contain null values per SC-002."""
        main([
            "--count", "500",
            "--seed", "42",
            "--output-dir", str(tmp_output_dir),
        ])

        files = list(tmp_output_dir.glob("transactions_*.parquet"))
        df = pl.read_parquet(files[0])

        for col in df.columns:
            null_count = df[col].null_count()
            assert null_count == 0, f"Column '{col}' has {null_count} nulls"

    def test_parquet_schema_types(self, tmp_output_dir: Path) -> None:
        """Output Parquet file must have correct column types."""
        main([
            "--count", "100",
            "--seed", "42",
            "--output-dir", str(tmp_output_dir),
        ])

        files = list(tmp_output_dir.glob("transactions_*.parquet"))
        df = pl.read_parquet(files[0])

        assert df.schema["transaction_id"] == pl.Utf8
        assert df.schema["amount"] == pl.Float64
        assert df.schema["currency"] == pl.Utf8
        assert df.schema["account_id"] == pl.Utf8

    def test_csv_output_format(self, tmp_output_dir: Path) -> None:
        """CSV format option must produce a valid CSV file."""
        main([
            "--count", "100",
            "--seed", "42",
            "--format", "csv",
            "--output-dir", str(tmp_output_dir),
        ])

        files = list(tmp_output_dir.glob("transactions_*.csv"))
        assert len(files) == 1
        df = pl.read_csv(files[0])
        assert len(df) == 100

    def test_zero_records_produces_empty_file_with_schema(self, tmp_output_dir: Path) -> None:
        """Zero record count must produce file with headers but no data rows."""
        main([
            "--count", "0",
            "--seed", "42",
            "--output-dir", str(tmp_output_dir),
        ])

        files = list(tmp_output_dir.glob("transactions_*.parquet"))
        assert len(files) == 1
        df = pl.read_parquet(files[0])
        assert len(df) == 0
        assert len(df.columns) == 9

    def test_metadata_printed_to_stdout(
        self, tmp_output_dir: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Generation metadata must be printed as JSON to stdout per FR-009."""
        import json

        main([
            "--count", "100",
            "--seed", "42",
            "--output-dir", str(tmp_output_dir),
        ])

        captured = capsys.readouterr()
        metadata = json.loads(captured.out)
        assert metadata["records_generated"] == 100
        assert metadata["seed"] == 42
        assert "output_path" in metadata
        assert "duration_seconds" in metadata
