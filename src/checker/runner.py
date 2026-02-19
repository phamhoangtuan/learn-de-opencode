"""Check runner: discovers, executes, and records data quality checks.

Connects to the DuckDB warehouse, discovers check SQL files, executes
each check, captures violations, records metadata in ``check_runs`` and
``check_results`` tables, and computes the overall run status.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from src.checker.models import (
    CheckModel,
    CheckResult,
    CheckRunResult,
    CheckStatus,
    RunStatus,
)
from src.checker.parser import discover_checks

logger = logging.getLogger("run_checks")

# Default paths matching project conventions
DEFAULT_DB_PATH: str = "data/warehouse/transactions.duckdb"
DEFAULT_CHECKS_DIR: str = "src/checks"

# Maximum number of sample violation rows to capture per check
MAX_SAMPLE_VIOLATIONS: int = 5

# DDL for the check_runs metadata table (per data-model.md)
_CHECK_RUNS_DDL: str = """
CREATE TABLE IF NOT EXISTS check_runs (
    run_id           VARCHAR PRIMARY KEY,
    started_at       TIMESTAMPTZ NOT NULL,
    completed_at     TIMESTAMPTZ,
    status           VARCHAR NOT NULL,
    total_checks     INTEGER NOT NULL DEFAULT 0,
    checks_passed    INTEGER NOT NULL DEFAULT 0,
    checks_failed    INTEGER NOT NULL DEFAULT 0,
    checks_errored   INTEGER NOT NULL DEFAULT 0,
    elapsed_seconds  DOUBLE NOT NULL DEFAULT 0.0,
    error_message    VARCHAR
);
"""

# DDL for the check_results metadata table (per data-model.md)
_CHECK_RESULTS_DDL: str = """
CREATE TABLE IF NOT EXISTS check_results (
    id                VARCHAR PRIMARY KEY,
    run_id            VARCHAR NOT NULL,
    check_name        VARCHAR NOT NULL,
    severity          VARCHAR NOT NULL,
    description       VARCHAR,
    status            VARCHAR NOT NULL,
    violation_count   INTEGER,
    sample_violations VARCHAR,
    elapsed_seconds   DOUBLE NOT NULL,
    error_message     VARCHAR
);
"""


def create_metadata_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the check_runs and check_results metadata tables if absent.

    Args:
        conn: An open DuckDB connection.
    """
    conn.execute(_CHECK_RUNS_DDL)
    conn.execute(_CHECK_RESULTS_DDL)
    logger.debug("check_runs and check_results tables ready")


def create_check_run(
    conn: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    started_at: str,
) -> None:
    """Insert a new check_runs record at the start of a run.

    Args:
        conn: An open DuckDB connection.
        run_id: Unique run identifier.
        started_at: ISO-format timestamp of when the run began.
    """
    conn.execute(
        """
        INSERT INTO check_runs (run_id, started_at, status)
        VALUES (?, ?::TIMESTAMPTZ, 'running')
        """,
        [run_id, started_at],
    )
    logger.info("Created check run %s", run_id)


def complete_check_run(
    conn: duckdb.DuckDBPyConnection,
    *,
    run_result: CheckRunResult,
) -> None:
    """Update a check_runs record upon run completion.

    Args:
        conn: An open DuckDB connection.
        run_result: The completed run result with final counters.
    """
    conn.execute(
        """
        UPDATE check_runs
        SET completed_at     = ?::TIMESTAMPTZ,
            status           = ?,
            total_checks     = ?,
            checks_passed    = ?,
            checks_failed    = ?,
            checks_errored   = ?,
            elapsed_seconds  = ?,
            error_message    = ?
        WHERE run_id = ?
        """,
        [
            datetime.now(UTC).isoformat(),
            run_result.status.value,
            run_result.total_checks,
            run_result.checks_passed,
            run_result.checks_failed,
            run_result.checks_errored,
            run_result.elapsed_seconds,
            run_result.error_message,
            run_result.run_id,
        ],
    )
    logger.info(
        "Completed check run %s with status=%s",
        run_result.run_id,
        run_result.status.value,
    )


def _execute_check(
    conn: duckdb.DuckDBPyConnection,
    check: CheckModel,
) -> CheckResult:
    """Execute a single check SQL and return its result.

    Runs the SQL query. If zero rows are returned the check passes.
    If one or more rows are returned the check fails, and up to
    ``MAX_SAMPLE_VIOLATIONS`` rows are captured. SQL errors produce
    an ERROR status with the error message captured.

    Args:
        conn: An open DuckDB connection.
        check: The parsed check model to execute.

    Returns:
        A CheckResult with status, violation info, and timing.
    """
    start = time.monotonic()
    try:
        rel = conn.execute(check.sql)
        rows = rel.fetchall()
        elapsed = time.monotonic() - start

        if len(rows) == 0:
            logger.info("Check '%s' passed in %.2fs", check.name, elapsed)
            return CheckResult(
                name=check.name,
                severity=check.severity,
                status=CheckStatus.PASS,
                violation_count=0,
                sample_violations=[],
                elapsed_seconds=elapsed,
            )

        # Collect column names for sample dicts
        columns = [desc[0] for desc in rel.description]
        sample_rows = rows[:MAX_SAMPLE_VIOLATIONS]
        samples = [
            dict(zip(columns, row, strict=True))
            for row in sample_rows
        ]

        logger.warning(
            "Check '%s' FAILED: %d violations in %.2fs",
            check.name,
            len(rows),
            elapsed,
        )
        return CheckResult(
            name=check.name,
            severity=check.severity,
            status=CheckStatus.FAIL,
            violation_count=len(rows),
            sample_violations=samples,
            elapsed_seconds=elapsed,
        )

    except duckdb.Error as exc:
        elapsed = time.monotonic() - start
        error_msg = str(exc)
        logger.error(
            "Check '%s' ERROR: %s (%.2fs)", check.name, error_msg, elapsed
        )
        return CheckResult(
            name=check.name,
            severity=check.severity,
            status=CheckStatus.ERROR,
            elapsed_seconds=elapsed,
            error=error_msg,
        )


def run_checks(
    *,
    db_path: str | Path = DEFAULT_DB_PATH,
    checks_dir: str | Path = DEFAULT_CHECKS_DIR,
) -> CheckRunResult:
    """Execute all data quality checks against the DuckDB warehouse.

    This is the main entry point for the check runner. It:
    1. Validates the database file exists (FR-015)
    2. Connects to DuckDB and creates metadata tables
    3. Discovers and parses check SQL files (FR-001, FR-002)
    4. Executes each check and captures results (FR-005, FR-006, FR-007)
    5. Records metadata in check_runs and check_results (FR-008, FR-009)
    6. Computes overall run status (FR-010)
    7. Returns a CheckRunResult with full details

    Args:
        db_path: Path to the DuckDB database file.
        checks_dir: Directory containing check SQL files.

    Returns:
        CheckRunResult with execution details and per-check results.
    """
    db_path = Path(db_path)
    checks_dir = Path(checks_dir)
    run_id = str(uuid.uuid4())
    start_time = time.monotonic()
    started_at = datetime.now(UTC).isoformat()

    result = CheckRunResult(run_id=run_id)

    # FR-015: Validate database exists
    if not db_path.exists():
        result.status = RunStatus.ERROR
        result.error_message = f"Database not found: {db_path}"
        result.elapsed_seconds = time.monotonic() - start_time
        logger.error(result.error_message)
        return result

    conn = duckdb.connect(str(db_path))

    try:
        # Create metadata tables and record run start
        create_metadata_tables(conn)
        create_check_run(conn, run_id=run_id, started_at=started_at)

        # Discover checks (FR-001)
        try:
            checks = discover_checks(checks_dir)
        except FileNotFoundError:
            result.status = RunStatus.ERROR
            result.error_message = (
                f"Checks directory not found: {checks_dir}"
            )
            result.elapsed_seconds = time.monotonic() - start_time
            complete_check_run(conn, run_result=result)
            logger.error(result.error_message)
            return result

        result.total_checks = len(checks)

        if not checks:
            result.status = RunStatus.PASSED
            result.elapsed_seconds = time.monotonic() - start_time
            complete_check_run(conn, run_result=result)
            logger.warning("No check files found in %s", checks_dir)
            return result

        # Execute each check (FR-005, FR-006, FR-007)
        for check in checks:
            check_result = _execute_check(conn, check)

            # Persist per-check result inline with description from model
            result.add_check_result(check_result)
            _record_check_result_with_description(
                conn,
                run_id=run_id,
                check=check,
                result=check_result,
            )

        # Compute overall status (FR-010)
        result.compute_status()
        result.elapsed_seconds = time.monotonic() - start_time

        # Record run completion
        complete_check_run(conn, run_result=result)

        logger.info(result.summary())
        return result

    finally:
        conn.close()


def _record_check_result_with_description(
    conn: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    check: CheckModel,
    result: CheckResult,
) -> None:
    """Insert a check_results row including the description from the model.

    This wrapper uses the description from the CheckModel rather than
    the CheckResult, since the result dataclass doesn't carry it.

    Args:
        conn: An open DuckDB connection.
        run_id: The parent run identifier.
        check: The parsed check model (carries description).
        result: The per-check execution result.
    """
    result_id = str(uuid.uuid4())
    sample_json: str | None = None
    if result.sample_violations:
        sample_json = json.dumps(result.sample_violations, default=str)

    conn.execute(
        """
        INSERT INTO check_results (
            id, run_id, check_name, severity, description,
            status, violation_count, sample_violations,
            elapsed_seconds, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            result_id,
            run_id,
            result.name,
            result.severity.value,
            check.description,
            result.status.value,
            result.violation_count,
            sample_json,
            result.elapsed_seconds,
            result.error,
        ],
    )
