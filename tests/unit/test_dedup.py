"""Unit tests for deduplication logic (T030, T031)."""

from __future__ import annotations

import polars as pl

from src.ingestion.dedup import deduplicate_cross_file, deduplicate_within_file


class TestDeduplicateWithinFile:
    """Tests for within-file deduplication (T030)."""

    def test_no_duplicates_unchanged(self) -> None:
        """Given unique records, When deduped, Then all records kept."""
        df = pl.DataFrame({
            "transaction_id": ["id-1", "id-2", "id-3"],
            "amount": [10.0, 20.0, 30.0],
        })

        result, skipped = deduplicate_within_file(df)

        assert len(result) == 3
        assert skipped == 0

    def test_duplicates_keep_first(self) -> None:
        """Given duplicates, When deduped, Then first occurrence kept."""
        df = pl.DataFrame({
            "transaction_id": ["id-1", "id-2", "id-1", "id-3", "id-2"],
            "amount": [10.0, 20.0, 99.0, 30.0, 88.0],
        })

        result, skipped = deduplicate_within_file(df)

        assert len(result) == 3
        assert skipped == 2
        # First occurrence amounts preserved
        amounts = result.sort("transaction_id")["amount"].to_list()
        assert amounts == [10.0, 20.0, 30.0]

    def test_all_duplicates_of_same_id(self) -> None:
        """Given all same ID, When deduped, Then one record kept."""
        df = pl.DataFrame({
            "transaction_id": ["id-1", "id-1", "id-1"],
            "amount": [10.0, 20.0, 30.0],
        })

        result, skipped = deduplicate_within_file(df)

        assert len(result) == 1
        assert skipped == 2
        assert result["amount"][0] == 10.0

    def test_empty_dataframe(self) -> None:
        """Given empty DataFrame, When deduped, Then empty result."""
        df = pl.DataFrame({
            "transaction_id": pl.Series([], dtype=pl.Utf8),
            "amount": pl.Series([], dtype=pl.Float64),
        })

        result, skipped = deduplicate_within_file(df)

        assert len(result) == 0
        assert skipped == 0

    def test_preserves_all_columns(self) -> None:
        """Given multi-column DataFrame, When deduped, Then all columns kept."""
        df = pl.DataFrame({
            "transaction_id": ["id-1", "id-2", "id-1"],
            "amount": [10.0, 20.0, 99.0],
            "currency": ["USD", "EUR", "GBP"],
        })

        result, _skipped = deduplicate_within_file(df)

        assert result.columns == ["transaction_id", "amount", "currency"]
        assert len(result) == 2


class TestDeduplicateCrossFile:
    """Tests for cross-file deduplication (T031)."""

    def test_no_existing_ids_all_kept(self) -> None:
        """Given no existing IDs, When deduped, Then all records kept."""
        df = pl.DataFrame({
            "transaction_id": ["id-1", "id-2", "id-3"],
            "amount": [10.0, 20.0, 30.0],
        })
        existing_ids: set[str] = set()

        result, skipped = deduplicate_cross_file(df, existing_ids=existing_ids)

        assert len(result) == 3
        assert skipped == 0

    def test_all_existing_ids_all_skipped(self) -> None:
        """Given all IDs exist, When deduped, Then all records skipped."""
        df = pl.DataFrame({
            "transaction_id": ["id-1", "id-2", "id-3"],
            "amount": [10.0, 20.0, 30.0],
        })
        existing_ids = {"id-1", "id-2", "id-3"}

        result, skipped = deduplicate_cross_file(df, existing_ids=existing_ids)

        assert len(result) == 0
        assert skipped == 3

    def test_partial_overlap(self) -> None:
        """Given partial overlap, When deduped, Then only new records kept."""
        df = pl.DataFrame({
            "transaction_id": ["id-1", "id-2", "id-3", "id-4"],
            "amount": [10.0, 20.0, 30.0, 40.0],
        })
        existing_ids = {"id-2", "id-4"}

        result, skipped = deduplicate_cross_file(df, existing_ids=existing_ids)

        assert len(result) == 2
        assert skipped == 2
        result_ids = set(result["transaction_id"].to_list())
        assert result_ids == {"id-1", "id-3"}

    def test_empty_dataframe(self) -> None:
        """Given empty DataFrame, When deduped, Then empty result."""
        df = pl.DataFrame({
            "transaction_id": pl.Series([], dtype=pl.Utf8),
            "amount": pl.Series([], dtype=pl.Float64),
        })
        existing_ids = {"id-1"}

        result, skipped = deduplicate_cross_file(df, existing_ids=existing_ids)

        assert len(result) == 0
        assert skipped == 0

    def test_preserves_all_columns(self) -> None:
        """Given multi-column DataFrame, When deduped, Then columns preserved."""
        df = pl.DataFrame({
            "transaction_id": ["id-1", "id-2"],
            "amount": [10.0, 20.0],
            "currency": ["USD", "EUR"],
        })
        existing_ids = {"id-1"}

        result, _skipped = deduplicate_cross_file(df, existing_ids=existing_ids)

        assert result.columns == ["transaction_id", "amount", "currency"]
        assert len(result) == 1
