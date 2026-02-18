"""Integration tests for end-to-end transaction generation."""

from __future__ import annotations

from datetime import UTC, date, datetime
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

    def test_no_null_values_in_required_fields(
        self, tmp_output_dir: Path,
    ) -> None:
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

    def test_zero_records_produces_empty_file_with_schema(
        self, tmp_output_dir: Path,
    ) -> None:
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


class TestParameterCombinations:
    """T023: Integration tests for parameter combinations (US2)."""

    def test_date_range_filtering(self, tmp_output_dir: Path) -> None:
        """All timestamps must fall within the configured date range per FR-003."""
        main([
            "--count", "500",
            "--seed", "42",
            "--start-date", "2025-06-01",
            "--end-date", "2025-06-30",
            "--output-dir", str(tmp_output_dir),
        ])

        files = list(tmp_output_dir.glob("transactions_*.parquet"))
        df = pl.read_parquet(files[0])

        start_dt = datetime(2025, 6, 1, tzinfo=UTC)
        end_dt = datetime(2025, 6, 30, 23, 59, 59, tzinfo=UTC)

        timestamps = df["timestamp"].to_list()
        for ts in timestamps:
            assert ts >= start_dt, (
                f"Timestamp {ts} is before start {start_dt}"
            )
            assert ts <= end_dt, (
                f"Timestamp {ts} is after end {end_dt}"
            )

    def test_same_day_range_produces_valid_timestamps(
        self, tmp_output_dir: Path,
    ) -> None:
        """Same start and end date must produce timestamps within that day."""
        main([
            "--count", "100",
            "--seed", "42",
            "--start-date", "2025-07-15",
            "--end-date", "2025-07-15",
            "--output-dir", str(tmp_output_dir),
        ])

        files = list(tmp_output_dir.glob("transactions_*.parquet"))
        df = pl.read_parquet(files[0])

        assert len(df) == 100
        timestamps = df["timestamp"].to_list()
        for ts in timestamps:
            assert ts.date() == date(2025, 7, 15), (
                f"Timestamp {ts} not on expected date 2025-07-15"
            )

    def test_account_count_accuracy(self, tmp_output_dir: Path) -> None:
        """Output must contain exactly N distinct account IDs per FR-004."""
        main([
            "--count", "500",
            "--seed", "42",
            "--accounts", "25",
            "--output-dir", str(tmp_output_dir),
        ])

        files = list(tmp_output_dir.glob("transactions_*.parquet"))
        df = pl.read_parquet(files[0])

        distinct_accounts = df["account_id"].n_unique()
        assert distinct_accounts == 25, (
            f"Expected 25 distinct accounts, got {distinct_accounts}"
        )

    def test_large_count_handling(self, tmp_output_dir: Path) -> None:
        """Large record counts (50k) must complete successfully."""
        exit_code = main([
            "--count", "50000",
            "--seed", "42",
            "--accounts", "200",
            "--output-dir", str(tmp_output_dir),
        ])
        assert exit_code == 0

        files = list(tmp_output_dir.glob("transactions_*.parquet"))
        df = pl.read_parquet(files[0])
        assert len(df) == 50000
        assert df["account_id"].n_unique() == 200

    def test_csv_with_custom_params(self, tmp_output_dir: Path) -> None:
        """CSV output with custom parameters must be correctly applied."""
        main([
            "--count", "200",
            "--seed", "99",
            "--accounts", "10",
            "--format", "csv",
            "--start-date", "2025-01-01",
            "--end-date", "2025-12-31",
            "--output-dir", str(tmp_output_dir),
        ])

        files = list(tmp_output_dir.glob("transactions_*.csv"))
        assert len(files) == 1
        df = pl.read_csv(files[0])
        assert len(df) == 200
        assert df["account_id"].n_unique() == 10

    def test_single_record_boundary(self, tmp_output_dir: Path) -> None:
        """count=1 must produce exactly one valid record."""
        exit_code = main([
            "--count", "1",
            "--seed", "42",
            "--accounts", "1",
            "--output-dir", str(tmp_output_dir),
        ])
        assert exit_code == 0

        files = list(tmp_output_dir.glob("transactions_*.parquet"))
        df = pl.read_parquet(files[0])
        assert len(df) == 1
        assert df["account_id"].n_unique() == 1
        # Verify all fields present and non-null
        for col in df.columns:
            assert df[col].null_count() == 0

    def test_accounts_capped_at_count_with_warning(
        self,
        tmp_output_dir: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Accounts exceeding count must be capped, with warning logged."""
        import json

        exit_code = main([
            "--count", "5",
            "--seed", "42",
            "--accounts", "100",
            "--output-dir", str(tmp_output_dir),
        ])
        assert exit_code == 0

        files = list(tmp_output_dir.glob("transactions_*.parquet"))
        df = pl.read_parquet(files[0])
        # Accounts capped to transaction count
        assert df["account_id"].n_unique() <= 5

        # Metadata should reflect capped account count
        captured = capsys.readouterr()
        metadata = json.loads(captured.out)
        assert metadata["accounts"] == 5


class TestReproducibility:
    """T028: Integration tests for reproducible generation (US3)."""

    def test_same_seed_produces_identical_output(
        self, tmp_path: Path,
    ) -> None:
        """Two runs with the same seed must produce identical DataFrames."""
        dir1 = tmp_path / "run1"
        dir2 = tmp_path / "run2"
        dir1.mkdir()
        dir2.mkdir()

        main([
            "--count", "500",
            "--seed", "42",
            "--start-date", "2025-01-01",
            "--end-date", "2025-06-30",
            "--accounts", "20",
            "--output-dir", str(dir1),
        ])
        main([
            "--count", "500",
            "--seed", "42",
            "--start-date", "2025-01-01",
            "--end-date", "2025-06-30",
            "--accounts", "20",
            "--output-dir", str(dir2),
        ])

        files1 = list(dir1.glob("transactions_*.parquet"))
        files2 = list(dir2.glob("transactions_*.parquet"))
        df1 = pl.read_parquet(files1[0])
        df2 = pl.read_parquet(files2[0])

        assert df1.equals(df2), (
            "Same seed + same params must produce identical DataFrames"
        )

    def test_different_seeds_produce_different_output(
        self, tmp_path: Path,
    ) -> None:
        """Different seeds must produce different DataFrames."""
        dir1 = tmp_path / "run1"
        dir2 = tmp_path / "run2"
        dir1.mkdir()
        dir2.mkdir()

        main([
            "--count", "100",
            "--seed", "42",
            "--start-date", "2025-01-01",
            "--end-date", "2025-06-30",
            "--output-dir", str(dir1),
        ])
        main([
            "--count", "100",
            "--seed", "99",
            "--start-date", "2025-01-01",
            "--end-date", "2025-06-30",
            "--output-dir", str(dir2),
        ])

        files1 = list(dir1.glob("transactions_*.parquet"))
        files2 = list(dir2.glob("transactions_*.parquet"))
        df1 = pl.read_parquet(files1[0])
        df2 = pl.read_parquet(files2[0])

        assert not df1.equals(df2), (
            "Different seeds must produce different DataFrames"
        )

    def test_auto_generated_seed_logged_in_metadata(
        self,
        tmp_output_dir: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """When no seed provided, generated seed must appear in metadata."""
        import json

        main([
            "--count", "50",
            "--output-dir", str(tmp_output_dir),
        ])

        captured = capsys.readouterr()
        metadata = json.loads(captured.out)
        assert "seed" in metadata
        assert isinstance(metadata["seed"], int)
        assert metadata["seed"] >= 0


class TestChunkedGeneration:
    """Tests for streaming/chunked writes for large datasets (T032)."""

    def test_chunked_generation_produces_correct_count(
        self, tmp_output_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Chunked mode must produce exactly the requested record count."""
        import src.generate_transactions as gt

        monkeypatch.setattr(gt, "CHUNK_THRESHOLD", 500)
        monkeypatch.setattr(gt, "CHUNK_SIZE", 200)

        exit_code = main([
            "--count", "750",
            "--seed", "42",
            "--output-dir", str(tmp_output_dir),
        ])
        assert exit_code == 0

        files = list(tmp_output_dir.glob("transactions_*.parquet"))
        df = pl.read_parquet(files[0])
        assert len(df) == 750

    def test_chunked_csv_output(
        self, tmp_output_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Chunked CSV mode must produce correct output."""
        import src.generate_transactions as gt

        monkeypatch.setattr(gt, "CHUNK_THRESHOLD", 300)
        monkeypatch.setattr(gt, "CHUNK_SIZE", 100)

        exit_code = main([
            "--count", "350",
            "--seed", "42",
            "--format", "csv",
            "--output-dir", str(tmp_output_dir),
        ])
        assert exit_code == 0

        files = list(tmp_output_dir.glob("transactions_*.csv"))
        df = pl.read_csv(files[0])
        assert len(df) == 350

    def test_chunked_preserves_schema(
        self, tmp_output_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Chunked output must have all 9 required fields."""
        import src.generate_transactions as gt

        monkeypatch.setattr(gt, "CHUNK_THRESHOLD", 200)
        monkeypatch.setattr(gt, "CHUNK_SIZE", 100)

        main([
            "--count", "300",
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
        for col in df.columns:
            assert df[col].null_count() == 0
