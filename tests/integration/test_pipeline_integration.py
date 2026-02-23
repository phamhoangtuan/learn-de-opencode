"""Integration tests for pipeline orchestration metadata (T013).

Uses a real DuckDB temp database but mocks subprocess to avoid executing
the actual pipeline steps.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from src.orchestrator.metadata import ensure_pipeline_runs_table, record_pipeline_run
from src.orchestrator.models import PipelineRun, PipelineStep, StepResult
from src.orchestrator.runner import run_pipeline


@pytest.fixture()
def db_path(tmp_path: Path) -> str:
    """Return a temp DuckDB path for each test."""
    return str(tmp_path / "test_pipeline.duckdb")


@pytest.fixture()
def linear_steps() -> list[PipelineStep]:
    """Return the 5-step pipeline definition."""
    return [
        PipelineStep(name="generate", command=["echo", "generate"]),
        PipelineStep(
            name="ingest",
            command=["echo", "ingest"],
            depends_on=["generate"],
        ),
        PipelineStep(
            name="transforms",
            command=["echo", "transforms"],
            depends_on=["ingest"],
        ),
        PipelineStep(
            name="checks",
            command=["echo", "checks"],
            depends_on=["transforms"],
        ),
        PipelineStep(
            name="dashboard",
            command=["echo", "dashboard"],
            depends_on=["checks"],
        ),
    ]


# ---------------------------------------------------------------------------
# DDL idempotency (CHK006, CHK042)
# ---------------------------------------------------------------------------


class TestEnsurePipelineRunsTable:
    """Tests for idempotent DDL creation."""

    def test_creates_table_on_first_call(self, db_path: str) -> None:
        ensure_pipeline_runs_table(db_path)

        with duckdb.connect(db_path) as con:
            result = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_name = 'pipeline_runs'"
            ).fetchone()
        assert result is not None
        assert result[0] == 1

    def test_idempotent_double_call(self, db_path: str) -> None:
        """Calling twice must not raise an error."""
        ensure_pipeline_runs_table(db_path)
        ensure_pipeline_runs_table(db_path)  # Should not raise

    def test_table_schema_has_required_columns(self, db_path: str) -> None:
        ensure_pipeline_runs_table(db_path)

        with duckdb.connect(db_path) as con:
            cols = {
                row[0]
                for row in con.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'pipeline_runs'"
                ).fetchall()
            }
        assert cols >= {
            "run_id",
            "status",
            "started_at",
            "finished_at",
            "steps_json",
            "dry_run",
        }


# ---------------------------------------------------------------------------
# Metadata record correctness (CHK007-CHK013, CHK043)
# ---------------------------------------------------------------------------


class TestRecordPipelineRun:
    """Tests for pipeline run metadata writing."""

    def test_success_run_writes_one_row(self, db_path: str) -> None:
        ensure_pipeline_runs_table(db_path)

        run = PipelineRun()
        run.step_results.append(
            StepResult(step="generate", status="success", duration_s=1.0, exit_code=0)
        )
        run.complete()
        record_pipeline_run(db_path, run)

        with duckdb.connect(db_path) as con:
            row = con.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()
        assert row is not None
        assert row[0] == 1

    def test_run_id_is_uuid(self, db_path: str) -> None:
        import re

        ensure_pipeline_runs_table(db_path)
        run = PipelineRun()
        run.complete()
        record_pipeline_run(db_path, run)

        with duckdb.connect(db_path) as con:
            row = con.execute("SELECT run_id FROM pipeline_runs").fetchone()
        assert row is not None
        uuid_re = re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        )
        assert uuid_re.match(row[0])

    def test_status_persisted(self, db_path: str) -> None:
        ensure_pipeline_runs_table(db_path)
        run = PipelineRun()
        run.mark_failed()
        run.complete()
        record_pipeline_run(db_path, run)

        with duckdb.connect(db_path) as con:
            row = con.execute("SELECT status FROM pipeline_runs").fetchone()
        assert row is not None
        assert row[0] == "failed"

    def test_started_at_and_finished_at_populated(self, db_path: str) -> None:
        ensure_pipeline_runs_table(db_path)
        run = PipelineRun()
        run.complete()
        record_pipeline_run(db_path, run)

        with duckdb.connect(db_path) as con:
            row = con.execute(
                "SELECT started_at, finished_at FROM pipeline_runs"
            ).fetchone()
        assert row is not None
        assert row[0] is not None
        assert row[1] is not None

    def test_steps_json_is_valid_json(self, db_path: str) -> None:
        ensure_pipeline_runs_table(db_path)
        run = PipelineRun()
        run.step_results.append(
            StepResult(step="generate", status="success", duration_s=0.5, exit_code=0)
        )
        run.step_results.append(
            StepResult(
                step="ingest",
                status="failed",
                duration_s=0.1,
                exit_code=1,
                error_output="err",
            )
        )
        run.complete()
        record_pipeline_run(db_path, run)

        with duckdb.connect(db_path) as con:
            row = con.execute("SELECT steps_json FROM pipeline_runs").fetchone()
        assert row is not None
        parsed = json.loads(row[0])
        assert isinstance(parsed, list)
        assert len(parsed) == 2

    def test_steps_json_skipped_has_null_exit_code(self, db_path: str) -> None:
        ensure_pipeline_runs_table(db_path)
        run = PipelineRun()
        run.step_results.append(StepResult(step="generate", status="skipped"))
        run.complete()
        record_pipeline_run(db_path, run)

        with duckdb.connect(db_path) as con:
            row = con.execute("SELECT steps_json FROM pipeline_runs").fetchone()
        assert row is not None
        parsed = json.loads(row[0])
        assert parsed[0]["exit_code"] is None
        assert parsed[0]["status"] == "skipped"

    def test_two_runs_create_two_rows(self, db_path: str) -> None:
        ensure_pipeline_runs_table(db_path)

        for _ in range(2):
            run = PipelineRun()
            run.complete()
            record_pipeline_run(db_path, run)

        with duckdb.connect(db_path) as con:
            row = con.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()
        assert row is not None
        assert row[0] == 2

    def test_dry_run_field_stored(self, db_path: str) -> None:
        ensure_pipeline_runs_table(db_path)
        run = PipelineRun(dry_run=True)
        run.complete()
        record_pipeline_run(db_path, run)

        with duckdb.connect(db_path) as con:
            row = con.execute("SELECT dry_run FROM pipeline_runs").fetchone()
        assert row is not None
        assert row[0] is True


# ---------------------------------------------------------------------------
# Full pipeline integration (mocked subprocesses)
# ---------------------------------------------------------------------------


class TestRunPipelineMetadataIntegration:
    """Tests combining run_pipeline + metadata writing (CHK014)."""

    def _success_result(self) -> MagicMock:
        m = MagicMock()
        m.returncode = 0
        return m

    def test_dry_run_does_not_write_to_db(
        self, linear_steps: list[PipelineStep], db_path: str, tmp_path: Path
    ) -> None:
        ensure_pipeline_runs_table(db_path)

        with patch("src.orchestrator.runner.subprocess.run"):
            run_pipeline(
                linear_steps,
                dry_run=True,
                project_root=str(tmp_path),
            )

        # Dry-run should NOT call record_pipeline_run; verify no rows
        with duckdb.connect(db_path) as con:
            row = con.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()
        assert row is not None
        assert row[0] == 0

    def test_successful_run_recorded_correctly(
        self, linear_steps: list[PipelineStep], db_path: str, tmp_path: Path
    ) -> None:
        ensure_pipeline_runs_table(db_path)

        with patch(
            "src.orchestrator.runner.subprocess.run",
            return_value=self._success_result(),
        ):
            run = run_pipeline(
                linear_steps,
                project_root=str(tmp_path),
            )
        record_pipeline_run(db_path, run)

        with duckdb.connect(db_path) as con:
            row = con.execute(
                "SELECT status, steps_json FROM pipeline_runs"
            ).fetchone()
        assert row is not None
        assert row[0] == "success"
        steps = json.loads(row[1])
        assert len(steps) == 5
        assert all(s["status"] == "success" for s in steps)

    def test_failed_run_recorded_correctly(
        self, linear_steps: list[PipelineStep], db_path: str, tmp_path: Path
    ) -> None:
        ensure_pipeline_runs_table(db_path)

        fail_result = MagicMock()
        fail_result.returncode = 1

        capture_result = MagicMock()
        capture_result.returncode = 1
        capture_result.stderr = "step error text"
        capture_result.stdout = ""

        def side_effect(cmd: list[str], **kwargs: object) -> MagicMock:
            if kwargs.get("capture_output"):
                return capture_result
            if cmd == ["echo", "ingest"]:
                return fail_result
            return self._success_result()

        with patch("src.orchestrator.runner.subprocess.run", side_effect=side_effect):
            run = run_pipeline(
                linear_steps,
                project_root=str(tmp_path),
            )
        record_pipeline_run(db_path, run)

        with duckdb.connect(db_path) as con:
            row = con.execute(
                "SELECT status, steps_json FROM pipeline_runs"
            ).fetchone()
        assert row is not None
        assert row[0] == "failed"
        steps = json.loads(row[1])
        failed = [s for s in steps if s["status"] == "failed"]
        assert len(failed) == 1
        assert failed[0]["step"] == "ingest"
