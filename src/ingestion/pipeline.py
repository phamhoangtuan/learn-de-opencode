"""Ingestion pipeline orchestrator.

Coordinates the end-to-end flow: discover Parquet files, read via Polars,
validate, deduplicate, enrich with lineage, and load into DuckDB.

Phase 3 (US1) implements the MVP: discover → read → load → summary.
Phase 4 (US2) adds schema validation and quarantine routing.
Phase 5 (US3) adds within-file and cross-file deduplication.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl

from src.ingestion.dedup import deduplicate_cross_file, deduplicate_within_file
from src.ingestion.loader import (
    DEFAULT_DB_PATH,
    complete_run,
    connect,
    create_run,
    create_tables,
    get_existing_transaction_ids,
    insert_quarantine_batch,
    insert_transactions,
)
from src.ingestion.models import FileResult, RunResult, RunStatus
from src.ingestion.validator import validate_file_schema, validate_records

# Default source directory per FR-001
DEFAULT_SOURCE_DIR: str = "data/raw"

logger = logging.getLogger("ingest_transactions")


def discover_parquet_files(source_dir: str | Path) -> list[Path]:
    """Find all Parquet files in the source directory.

    Discovers files matching *.parquet in the given directory (non-recursive).
    Files are sorted by name for deterministic processing order.

    Args:
        source_dir: Directory to scan for Parquet files.

    Returns:
        Sorted list of Path objects for discovered Parquet files.
    """
    source_path = Path(source_dir)
    if not source_path.exists():
        logger.warning("Source directory does not exist: %s", source_path)
        return []

    files = sorted(source_path.glob("*.parquet"))
    logger.info("Discovered %d Parquet file(s) in %s", len(files), source_path)
    return files


def read_parquet(file_path: Path) -> pl.DataFrame:
    """Read a Parquet file into a Polars DataFrame.

    Args:
        file_path: Path to the Parquet file.

    Returns:
        Polars DataFrame with the file contents.

    Raises:
        Exception: If the file cannot be read (corrupted, missing, etc.).
    """
    logger.info("Reading %s", file_path.name)
    return pl.read_parquet(file_path)


def enrich_with_lineage(
    df: pl.DataFrame,
    *,
    source_file: str,
    run_id: str,
) -> pl.DataFrame:
    """Add lineage and derived columns to a transaction DataFrame.

    Adds source_file, ingested_at, run_id (per FR-012/FR-013) and
    transaction_date (per FR-014) columns.

    Args:
        df: Source DataFrame with 9 transaction columns.
        source_file: Name of the source Parquet file.
        run_id: Pipeline run identifier.

    Returns:
        DataFrame with 13 columns (9 source + 4 enrichment).
    """
    now = datetime.now(UTC)
    return df.with_columns(
        pl.col("timestamp").dt.date().alias("transaction_date"),
        pl.lit(source_file).alias("source_file"),
        pl.lit(now).cast(pl.Datetime("us", time_zone="UTC")).alias("ingested_at"),
        pl.lit(run_id).alias("run_id"),
    )


def _process_file(
    conn: duckdb.DuckDBPyConnection,
    file_path: Path,
    run_id: str,
    existing_ids: set[str],
) -> FileResult:
    """Process a single Parquet file through the pipeline.

    Flow: read → schema validate → record validate → dedup → enrich → load.
    Invalid records are routed to the quarantine table.

    Args:
        conn: An open DuckDB connection with tables created.
        file_path: Path to the Parquet file.
        run_id: Pipeline run identifier.
        existing_ids: Transaction IDs already in the warehouse (for cross-file dedup).

    Returns:
        FileResult with processing outcome.
    """
    file_name = file_path.name
    try:
        df = read_parquet(file_path)
    except Exception as exc:
        logger.error("Failed to read %s: %s", file_name, exc)
        return FileResult(file_name=file_name, error=str(exc))

    # File-level schema validation (FR-006)
    schema_errors = validate_file_schema(df)
    if schema_errors:
        error_msg = "; ".join(schema_errors)
        logger.warning("Schema validation failed for %s: %s", file_name, error_msg)
        return FileResult(file_name=file_name, error=error_msg)

    # Record-level validation (FR-005, FR-007)
    validated = validate_records(df)
    valid_df = validated.filter(pl.col("_is_valid")).drop("_is_valid", "_rejection_reason")
    invalid_df = validated.filter(~pl.col("_is_valid"))

    # Quarantine invalid records (FR-008)
    quarantined = 0
    if not invalid_df.is_empty():
        quarantine_records = _build_quarantine_records(
            invalid_df, source_file=file_name, run_id=run_id,
        )
        quarantined = insert_quarantine_batch(conn, records=quarantine_records)
        logger.info(
            "Quarantined %d invalid record(s) from %s", quarantined, file_name,
        )

    if valid_df.is_empty():
        return FileResult(
            file_name=file_name,
            records_quarantined=quarantined,
        )

    # Within-file deduplication (US3)
    deduped, within_skipped = deduplicate_within_file(valid_df)

    # Cross-file deduplication (US3)
    deduped, cross_skipped = deduplicate_cross_file(
        deduped, existing_ids=existing_ids,
    )
    total_skipped = within_skipped + cross_skipped

    if deduped.is_empty():
        return FileResult(
            file_name=file_name,
            records_quarantined=quarantined,
            duplicates_skipped=total_skipped,
        )

    # Enrich valid, unique records with lineage columns
    enriched = enrich_with_lineage(deduped, source_file=file_name, run_id=run_id)

    # Load into DuckDB
    loaded = insert_transactions(conn, enriched)

    # Update existing_ids with newly loaded IDs for subsequent files
    new_ids = set(deduped["transaction_id"].to_list())
    existing_ids.update(new_ids)

    return FileResult(
        file_name=file_name,
        records_loaded=loaded,
        records_quarantined=quarantined,
        duplicates_skipped=total_skipped,
    )


def _build_quarantine_records(
    invalid_df: pl.DataFrame,
    *,
    source_file: str,
    run_id: str,
) -> list[dict[str, str]]:
    """Convert invalid DataFrame rows to quarantine record dicts.

    Serializes each row as JSON and extracts the rejection reason.

    Args:
        invalid_df: DataFrame with _is_valid and _rejection_reason columns.
        source_file: Name of the source Parquet file.
        run_id: Pipeline run identifier.

    Returns:
        List of dicts suitable for insert_quarantine_batch.
    """
    records: list[dict[str, str]] = []
    # Get column names excluding internal validation columns
    data_columns = [
        c for c in invalid_df.columns
        if c not in ("_is_valid", "_rejection_reason")
    ]

    for row in invalid_df.iter_rows(named=True):
        record_data = {col: _serialize_value(row[col]) for col in data_columns}
        records.append({
            "source_file": source_file,
            "record_data": json.dumps(record_data),
            "rejection_reason": row["_rejection_reason"],
            "run_id": run_id,
        })

    return records


def _serialize_value(value: object) -> str:
    """Convert a value to a JSON-safe string representation.

    Args:
        value: Any value from a DataFrame row.

    Returns:
        String representation of the value.
    """
    if value is None:
        return "null"
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def run_pipeline(
    *,
    source_dir: str | Path = DEFAULT_SOURCE_DIR,
    db_path: str | Path = DEFAULT_DB_PATH,
) -> RunResult:
    """Execute the ingestion pipeline.

    Discovers Parquet files in source_dir, reads each one, enriches with
    lineage metadata, and loads into the DuckDB warehouse. Tracks the run
    in the ingestion_runs table per FR-016.

    Args:
        source_dir: Directory containing source Parquet files.
        db_path: Path to the DuckDB database file.

    Returns:
        RunResult with aggregate processing statistics.
    """
    run_id = uuid.uuid4().hex
    started_at = datetime.now(UTC)
    start_time = time.monotonic()

    result = RunResult(run_id=run_id)

    # Connect and ensure tables exist (FR-003)
    conn = connect(db_path)
    try:
        create_tables(conn)
        create_run(conn, run_id=run_id, started_at=started_at.isoformat())

        # Load existing IDs for cross-file deduplication (US3)
        existing_ids = get_existing_transaction_ids(conn)

        # Discover files
        files = discover_parquet_files(source_dir)
        if not files:
            logger.info("No Parquet files found — nothing to ingest")

        # Process each file
        for file_path in files:
            file_result = _process_file(conn, file_path, run_id, existing_ids)
            result.add_file_result(file_result)

        # Finalize
        result.status = RunStatus.COMPLETED
        result.elapsed_seconds = time.monotonic() - start_time

        complete_run(
            conn,
            run_id=run_id,
            status=result.status.value,
            completed_at=datetime.now(UTC).isoformat(),
            files_processed=result.files_processed,
            records_loaded=result.records_loaded,
            records_quarantined=result.records_quarantined,
            duplicates_skipped=result.duplicates_skipped,
            elapsed_seconds=result.elapsed_seconds,
        )

    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        result.status = RunStatus.FAILED
        result.elapsed_seconds = time.monotonic() - start_time

        try:
            complete_run(
                conn,
                run_id=run_id,
                status=result.status.value,
                completed_at=datetime.now(UTC).isoformat(),
                files_processed=result.files_processed,
                records_loaded=result.records_loaded,
                records_quarantined=result.records_quarantined,
                duplicates_skipped=result.duplicates_skipped,
                elapsed_seconds=result.elapsed_seconds,
            )
        except Exception:
            logger.exception("Failed to update run record")

        raise

    finally:
        conn.close()

    logger.info(result.summary())
    return result
