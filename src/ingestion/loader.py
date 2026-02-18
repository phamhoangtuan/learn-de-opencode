"""DuckDB connection management and data loading operations."""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import polars as pl

# Default warehouse location per FR-002
DEFAULT_DB_PATH: str = "data/warehouse/transactions.duckdb"

# DDL statements for the three warehouse tables per data-model.md
_TRANSACTIONS_DDL: str = """
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id     VARCHAR PRIMARY KEY,
    timestamp          TIMESTAMPTZ NOT NULL,
    amount             DOUBLE NOT NULL,
    currency           VARCHAR NOT NULL,
    merchant_name      VARCHAR NOT NULL,
    category           VARCHAR NOT NULL,
    account_id         VARCHAR NOT NULL,
    transaction_type   VARCHAR NOT NULL,
    status             VARCHAR NOT NULL,
    transaction_date   DATE NOT NULL,
    source_file        VARCHAR NOT NULL,
    ingested_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    run_id             VARCHAR NOT NULL
);
"""

_QUARANTINE_DDL: str = """
CREATE TABLE IF NOT EXISTS quarantine (
    quarantine_id      INTEGER PRIMARY KEY DEFAULT nextval('quarantine_id_seq'),
    source_file        VARCHAR NOT NULL,
    record_data        VARCHAR NOT NULL,
    rejection_reason   VARCHAR NOT NULL,
    rejected_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    run_id             VARCHAR NOT NULL
);
"""

_QUARANTINE_SEQ_DDL: str = """
CREATE SEQUENCE IF NOT EXISTS quarantine_id_seq START 1;
"""

_INGESTION_RUNS_DDL: str = """
CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id              VARCHAR PRIMARY KEY,
    started_at          TIMESTAMPTZ NOT NULL,
    completed_at        TIMESTAMPTZ,
    status              VARCHAR NOT NULL,
    files_processed     INTEGER NOT NULL DEFAULT 0,
    records_loaded      INTEGER NOT NULL DEFAULT 0,
    records_quarantined INTEGER NOT NULL DEFAULT 0,
    duplicates_skipped  INTEGER NOT NULL DEFAULT 0,
    elapsed_seconds     DOUBLE
);
"""

logger = logging.getLogger("ingest_transactions")


def connect(db_path: str | Path = DEFAULT_DB_PATH) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection, ensuring the parent directory exists.

    Args:
        db_path: Path to the DuckDB database file.

    Returns:
        An open DuckDB connection.
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Connecting to DuckDB at %s", db_path)
    return duckdb.connect(str(db_path))


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create warehouse tables if they do not exist.

    Creates the transactions, quarantine, and ingestion_runs tables
    per FR-003. Safe to call multiple times (idempotent).

    Args:
        conn: An open DuckDB connection.
    """
    conn.execute(_QUARANTINE_SEQ_DDL)
    conn.execute(_TRANSACTIONS_DDL)
    conn.execute(_QUARANTINE_DDL)
    conn.execute(_INGESTION_RUNS_DDL)
    logger.info("Warehouse tables ready")


def insert_transactions(
    conn: duckdb.DuckDBPyConnection,
    df: pl.DataFrame,
) -> int:
    """Insert validated transaction records into the transactions table.

    Uses DuckDB's zero-copy Arrow interchange with Polars for efficient
    bulk insertion per research decision #2.

    Args:
        conn: An open DuckDB connection with tables created.
        df: A Polars DataFrame with all 13 transaction columns
            (9 source + transaction_date + source_file + ingested_at + run_id).

    Returns:
        Number of records inserted.
    """
    if df.is_empty():
        return 0

    # Register the Polars DataFrame as a DuckDB view for bulk insert
    conn.register("_staged_transactions", df.to_arrow())
    conn.execute("""
        INSERT INTO transactions (
            transaction_id, timestamp, amount, currency,
            merchant_name, category, account_id, transaction_type,
            status, transaction_date, source_file, ingested_at, run_id
        )
        SELECT
            transaction_id, timestamp, amount, currency,
            merchant_name, category, account_id, transaction_type,
            status, transaction_date, source_file, ingested_at, run_id
        FROM _staged_transactions
    """)
    conn.unregister("_staged_transactions")

    row_count = len(df)
    logger.info("Inserted %d records into transactions", row_count)
    return row_count


def insert_quarantine(
    conn: duckdb.DuckDBPyConnection,
    *,
    source_file: str,
    record_data: str,
    rejection_reason: str,
    run_id: str,
) -> None:
    """Insert a single quarantine record per FR-008.

    Args:
        conn: An open DuckDB connection with tables created.
        source_file: Name of the source Parquet file.
        record_data: JSON-serialized original record data.
        rejection_reason: Description of why the record was rejected.
        run_id: Pipeline run identifier.
    """
    conn.execute(
        """
        INSERT INTO quarantine (source_file, record_data, rejection_reason, run_id)
        VALUES (?, ?, ?, ?)
        """,
        [source_file, record_data, rejection_reason, run_id],
    )


def insert_quarantine_batch(
    conn: duckdb.DuckDBPyConnection,
    *,
    records: list[dict[str, str]],
) -> int:
    """Insert multiple quarantine records efficiently.

    Each dict must contain: source_file, record_data, rejection_reason, run_id.

    Args:
        conn: An open DuckDB connection with tables created.
        records: List of quarantine record dicts.

    Returns:
        Number of records inserted.
    """
    if not records:
        return 0

    params = [
        (r["source_file"], r["record_data"], r["rejection_reason"], r["run_id"])
        for r in records
    ]
    conn.executemany(
        """
        INSERT INTO quarantine (source_file, record_data, rejection_reason, run_id)
        VALUES (?, ?, ?, ?)
        """,
        params,
    )
    count = len(params)
    logger.info("Quarantined %d records", count)
    return count


def create_run(
    conn: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    started_at: str,
) -> None:
    """Create an ingestion_runs record at pipeline start.

    Args:
        conn: An open DuckDB connection with tables created.
        run_id: Unique pipeline run identifier.
        started_at: ISO-format timestamp of when the run began.
    """
    conn.execute(
        """
        INSERT INTO ingestion_runs (run_id, started_at, status)
        VALUES (?, ?::TIMESTAMPTZ, 'running')
        """,
        [run_id, started_at],
    )
    logger.info("Created ingestion run %s", run_id)


def complete_run(
    conn: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    status: str,
    completed_at: str,
    files_processed: int,
    records_loaded: int,
    records_quarantined: int,
    duplicates_skipped: int,
    elapsed_seconds: float,
) -> None:
    """Update an ingestion_runs record upon pipeline completion.

    Args:
        conn: An open DuckDB connection with tables created.
        run_id: Unique pipeline run identifier.
        status: Final run status ('completed' or 'failed').
        completed_at: ISO-format timestamp of when the run finished.
        files_processed: Number of files processed.
        records_loaded: Total records successfully loaded.
        records_quarantined: Total records sent to quarantine.
        duplicates_skipped: Total duplicate records skipped.
        elapsed_seconds: Total run duration in seconds.
    """
    conn.execute(
        """
        UPDATE ingestion_runs
        SET completed_at = ?::TIMESTAMPTZ,
            status = ?,
            files_processed = ?,
            records_loaded = ?,
            records_quarantined = ?,
            duplicates_skipped = ?,
            elapsed_seconds = ?
        WHERE run_id = ?
        """,
        [
            completed_at,
            status,
            files_processed,
            records_loaded,
            records_quarantined,
            duplicates_skipped,
            elapsed_seconds,
            run_id,
        ],
    )
    logger.info("Completed ingestion run %s with status=%s", run_id, status)


def get_existing_transaction_ids(
    conn: duckdb.DuckDBPyConnection,
) -> set[str]:
    """Retrieve all existing transaction_ids from the warehouse.

    Used for cross-file deduplication via anti-join (research decision #4).

    Args:
        conn: An open DuckDB connection with tables created.

    Returns:
        Set of transaction_id strings already in the warehouse.
    """
    result = conn.execute(
        "SELECT transaction_id FROM transactions"
    ).fetchall()
    return {row[0] for row in result}


def table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """Check whether a table exists in the DuckDB database.

    Args:
        conn: An open DuckDB connection.
        table_name: Name of the table to check.

    Returns:
        True if the table exists, False otherwise.
    """
    result = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return result is not None and result[0] > 0
