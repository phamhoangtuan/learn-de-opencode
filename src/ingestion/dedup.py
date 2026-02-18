"""Deduplication logic for transaction records.

Provides within-file and cross-file deduplication per research decision #4.
Within-file uses Polars unique(); cross-file uses anti-join against existing IDs.
"""

from __future__ import annotations

import logging

import polars as pl

logger = logging.getLogger("ingest_transactions")


def deduplicate_within_file(df: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    """Remove duplicate transaction_ids within a single DataFrame.

    Keeps the first occurrence per research decision #4:
    ``unique(subset="transaction_id", keep="first")``.

    Args:
        df: DataFrame that may contain duplicate transaction_ids.

    Returns:
        Tuple of (deduplicated DataFrame, number of duplicates skipped).
    """
    original_count = len(df)
    deduped = df.unique(subset=["transaction_id"], keep="first", maintain_order=True)
    skipped = original_count - len(deduped)

    if skipped > 0:
        logger.info(
            "Within-file dedup: removed %d duplicate(s) from %d records",
            skipped,
            original_count,
        )

    return deduped, skipped


def deduplicate_cross_file(
    df: pl.DataFrame,
    *,
    existing_ids: set[str],
) -> tuple[pl.DataFrame, int]:
    """Remove records whose transaction_id already exists in the warehouse.

    Uses Polars filtering against a set of known IDs per research decision #4.

    Args:
        df: DataFrame with new records to check.
        existing_ids: Set of transaction_ids already in the warehouse.

    Returns:
        Tuple of (new-only DataFrame, number of duplicates skipped).
    """
    if not existing_ids or df.is_empty():
        return df, 0

    original_count = len(df)
    new_only = df.filter(~pl.col("transaction_id").is_in(list(existing_ids)))
    skipped = original_count - len(new_only)

    if skipped > 0:
        logger.info(
            "Cross-file dedup: skipped %d existing record(s) from %d",
            skipped,
            original_count,
        )

    return new_only, skipped
