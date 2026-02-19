"""Transform runner: orchestrates SQL model discovery, resolution, and execution.

Handles DuckDB connection, transform_runs metadata table, and per-model
execution with error handling per FR-015.
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from src.transformer.graph import (
    CircularDependencyError,
    resolve_execution_order,
)
from src.transformer.models import (
    ModelResult,
    TransformRunResult,
    TransformStatus,
)
from src.transformer.parser import discover_models

logger = logging.getLogger("run_transforms")

# Default paths per plan.md
DEFAULT_DB_PATH: str = "data/warehouse/transactions.duckdb"
DEFAULT_TRANSFORMS_DIR: str = "src/transforms"

# DDL for the transform_runs metadata table per FR-010
_TRANSFORM_RUNS_DDL: str = """
CREATE TABLE IF NOT EXISTS transform_runs (
    run_id             VARCHAR PRIMARY KEY,
    started_at         TIMESTAMPTZ NOT NULL,
    completed_at       TIMESTAMPTZ,
    status             VARCHAR NOT NULL,
    models_executed    INTEGER NOT NULL DEFAULT 0,
    models_failed      INTEGER NOT NULL DEFAULT 0,
    elapsed_seconds    DOUBLE,
    error_message      VARCHAR
);
"""


def create_transform_runs_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the transform_runs metadata table if it does not exist.

    Args:
        conn: An open DuckDB connection.
    """
    conn.execute(_TRANSFORM_RUNS_DDL)
    logger.debug("transform_runs table ready")


def create_run(
    conn: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    started_at: str,
) -> None:
    """Insert a new transform_runs record at pipeline start.

    Args:
        conn: An open DuckDB connection.
        run_id: Unique pipeline run identifier.
        started_at: ISO-format timestamp of when the run began.
    """
    conn.execute(
        """
        INSERT INTO transform_runs (run_id, started_at, status)
        VALUES (?, ?::TIMESTAMPTZ, 'running')
        """,
        [run_id, started_at],
    )
    logger.info("Created transform run %s", run_id)


def complete_run(
    conn: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    status: str,
    completed_at: str,
    models_executed: int,
    models_failed: int,
    elapsed_seconds: float,
    error_message: str | None = None,
) -> None:
    """Update a transform_runs record upon pipeline completion.

    Args:
        conn: An open DuckDB connection.
        run_id: Unique pipeline run identifier.
        status: Final run status ('completed' or 'failed').
        completed_at: ISO-format timestamp of when the run finished.
        models_executed: Number of models successfully executed.
        models_failed: Number of models that failed.
        elapsed_seconds: Total run duration in seconds.
        error_message: Error description if status is 'failed'.
    """
    conn.execute(
        """
        UPDATE transform_runs
        SET completed_at = ?::TIMESTAMPTZ,
            status = ?,
            models_executed = ?,
            models_failed = ?,
            elapsed_seconds = ?,
            error_message = ?
        WHERE run_id = ?
        """,
        [
            completed_at,
            status,
            models_executed,
            models_failed,
            elapsed_seconds,
            error_message,
            run_id,
        ],
    )
    logger.info("Completed transform run %s with status=%s", run_id, status)


def _source_table_exists(conn: duckdb.DuckDBPyConnection) -> bool:
    """Check whether the source transactions table exists per FR-016.

    Args:
        conn: An open DuckDB connection.

    Returns:
        True if the transactions table exists and has data.
    """
    result = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = 'transactions'
        """
    ).fetchone()
    return result is not None and result[0] > 0


def _execute_model(
    conn: duckdb.DuckDBPyConnection,
    model_name: str,
    sql: str,
) -> ModelResult:
    """Execute a single SQL model against the DuckDB connection.

    Each model executes as its own auto-committed statement per
    research decision #3.

    Args:
        conn: An open DuckDB connection.
        model_name: Name of the model being executed.
        sql: SQL statement to execute.

    Returns:
        ModelResult indicating success or failure.
    """
    start = time.monotonic()
    try:
        conn.execute(sql)
        elapsed = time.monotonic() - start
        logger.info("Executed model '%s' in %.2fs", model_name, elapsed)
        return ModelResult(name=model_name, success=True, elapsed_seconds=elapsed)
    except duckdb.Error as e:
        elapsed = time.monotonic() - start
        error_msg = str(e)
        logger.error("Failed to execute model '%s': %s", model_name, error_msg)
        return ModelResult(
            name=model_name,
            success=False,
            error=error_msg,
            elapsed_seconds=elapsed,
        )


def run_transforms(
    *,
    db_path: str | Path = DEFAULT_DB_PATH,
    transforms_dir: str | Path = DEFAULT_TRANSFORMS_DIR,
) -> TransformRunResult:
    """Execute all SQL transforms in dependency order.

    This is the main entry point for the transform pipeline. It:
    1. Connects to the DuckDB warehouse
    2. Validates the source transactions table exists (FR-016)
    3. Creates the transform_runs metadata table (FR-010)
    4. Discovers and parses SQL files (FR-001)
    5. Resolves dependencies (FR-003, FR-004)
    6. Executes models in topological order (FR-005, FR-006)
    7. Records execution metadata (FR-011)
    8. Produces a run summary (FR-013)

    Args:
        db_path: Path to the DuckDB database file.
        transforms_dir: Directory containing .sql transform files.

    Returns:
        TransformRunResult with execution details.
    """
    db_path = Path(db_path)
    transforms_dir = Path(transforms_dir)
    run_id = str(uuid.uuid4())
    start_time = time.monotonic()
    started_at = datetime.now(UTC).isoformat()

    result = TransformRunResult(run_id=run_id)

    # Connect to DuckDB
    if not db_path.exists():
        result.status = TransformStatus.FAILED
        result.error_message = f"Database not found: {db_path}"
        result.elapsed_seconds = time.monotonic() - start_time
        logger.error(result.error_message)
        return result

    conn = duckdb.connect(str(db_path))

    try:
        # Create metadata table and record run start
        create_transform_runs_table(conn)
        create_run(conn, run_id=run_id, started_at=started_at)

        # Validate source table exists (FR-016)
        if not _source_table_exists(conn):
            result.status = TransformStatus.FAILED
            result.error_message = "Source 'transactions' table does not exist"
            result.elapsed_seconds = time.monotonic() - start_time
            complete_run(
                conn,
                run_id=run_id,
                status="failed",
                completed_at=datetime.now(UTC).isoformat(),
                models_executed=0,
                models_failed=0,
                elapsed_seconds=result.elapsed_seconds,
                error_message=result.error_message,
            )
            logger.error(result.error_message)
            return result

        # Discover models (FR-001)
        models = discover_models(transforms_dir)
        if not models:
            result.status = TransformStatus.COMPLETED
            result.error_message = "No transform models found"
            result.elapsed_seconds = time.monotonic() - start_time
            complete_run(
                conn,
                run_id=run_id,
                status="completed",
                completed_at=datetime.now(UTC).isoformat(),
                models_executed=0,
                models_failed=0,
                elapsed_seconds=result.elapsed_seconds,
            )
            logger.warning(result.error_message)
            return result

        # Resolve execution order (FR-003, FR-004)
        try:
            ordered_models = resolve_execution_order(models)
        except CircularDependencyError as e:
            result.status = TransformStatus.FAILED
            result.error_message = str(e)
            result.elapsed_seconds = time.monotonic() - start_time
            complete_run(
                conn,
                run_id=run_id,
                status="failed",
                completed_at=datetime.now(UTC).isoformat(),
                models_executed=0,
                models_failed=0,
                elapsed_seconds=result.elapsed_seconds,
                error_message=result.error_message,
            )
            logger.error(result.error_message)
            return result

        # Execute models in order (FR-005, FR-015)
        for model in ordered_models:
            model_result = _execute_model(conn, model.name, model.sql)
            result.add_model_result(model_result)

        # Determine final status
        if result.models_failed > 0 and result.models_executed == 0:
            result.status = TransformStatus.FAILED
            result.error_message = "All models failed"
        else:
            result.status = TransformStatus.COMPLETED

        result.elapsed_seconds = time.monotonic() - start_time

        # Record completion (FR-011)
        complete_run(
            conn,
            run_id=run_id,
            status=result.status.value,
            completed_at=datetime.now(UTC).isoformat(),
            models_executed=result.models_executed,
            models_failed=result.models_failed,
            elapsed_seconds=result.elapsed_seconds,
            error_message=result.error_message,
        )

        logger.info(result.summary())
        return result

    finally:
        conn.close()
