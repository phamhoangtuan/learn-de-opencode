"""DuckDB metadata writer for pipeline run records.

Manages the ``pipeline_runs`` table — idempotent DDL creation and INSERT
of one row per non-dry-run pipeline invocation.
"""

from __future__ import annotations

import json
import logging

import duckdb

from src.orchestrator.models import PipelineRun

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id       VARCHAR PRIMARY KEY,
    status       VARCHAR NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL,
    finished_at  TIMESTAMPTZ,
    steps_json   VARCHAR NOT NULL,
    dry_run      BOOLEAN NOT NULL DEFAULT FALSE
);
"""


def ensure_pipeline_runs_table(db_path: str) -> None:
    """Create the ``pipeline_runs`` table if it does not already exist.

    Idempotent — safe to call on every run. Uses ``CREATE TABLE IF NOT EXISTS``
    so repeated calls are a no-op.

    Args:
        db_path: Path to the DuckDB database file.

    Raises:
        duckdb.Error: If the database cannot be opened or DDL fails.
    """
    logger.debug("Ensuring pipeline_runs table exists in %s", db_path)
    with duckdb.connect(db_path) as con:
        con.execute(_DDL)
    logger.debug("pipeline_runs table ready")


def record_pipeline_run(db_path: str, run: PipelineRun) -> None:
    """Insert one row into ``pipeline_runs`` for the given run.

    Args:
        db_path: Path to the DuckDB database file.
        run: The completed (or failed) ``PipelineRun`` to persist.

    Raises:
        duckdb.Error: If the INSERT fails.
    """
    steps_json = json.dumps([r.to_dict() for r in run.step_results])

    logger.debug("Recording pipeline run %s (status=%s)", run.run_id, run.status)
    with duckdb.connect(db_path) as con:
        con.execute(
            """
            INSERT INTO pipeline_runs
                (run_id, status, started_at, finished_at, steps_json, dry_run)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                run.run_id,
                run.status,
                run.started_at,
                run.finished_at,
                steps_json,
                run.dry_run,
            ],
        )
    logger.debug("Recorded pipeline run %s", run.run_id)
