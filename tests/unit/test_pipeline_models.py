"""Unit tests for orchestrator domain models (T003)."""

from __future__ import annotations

from datetime import UTC, datetime

from src.orchestrator.models import PipelineRun, PipelineStep, StepResult


class TestPipelineStep:
    """Tests for PipelineStep dataclass."""

    def test_required_fields(self) -> None:
        step = PipelineStep(
            name="generate",
            command=["uv", "run", "src/generate_transactions.py"],
        )

        assert step.name == "generate"
        assert step.command == ["uv", "run", "src/generate_transactions.py"]
        assert step.cwd is None
        assert step.depends_on == []

    def test_with_cwd_and_depends(self) -> None:
        step = PipelineStep(
            name="dashboard",
            command=["npm", "run", "sources"],
            cwd="dashboard",
            depends_on=["checks"],
        )

        assert step.cwd == "dashboard"
        assert step.depends_on == ["checks"]

    def test_depends_on_is_list_not_shared(self) -> None:
        """Each PipelineStep must have its own depends_on list (no shared default mutable)."""
        step_a = PipelineStep(name="a", command=["cmd"])
        step_b = PipelineStep(name="b", command=["cmd"])
        step_a.depends_on.append("x")
        assert step_b.depends_on == []

    def test_satisfies_graph_interface(self) -> None:
        """PipelineStep must have .name and .depends_on for graph.resolve_execution_order."""
        step = PipelineStep(name="ingest", command=["uv", "run", "src/ingest_transactions.py"])
        assert hasattr(step, "name")
        assert hasattr(step, "depends_on")
        assert isinstance(step.name, str)
        assert isinstance(step.depends_on, list)


class TestStepResult:
    """Tests for StepResult dataclass."""

    def test_defaults(self) -> None:
        result = StepResult(step="generate", status="success")

        assert result.step == "generate"
        assert result.status == "success"
        assert result.duration_s == 0.0
        assert result.exit_code is None
        assert result.error_output is None

    def test_to_dict_success(self) -> None:
        result = StepResult(step="ingest", status="success", duration_s=1.234, exit_code=0)
        d = result.to_dict()

        assert d["step"] == "ingest"
        assert d["status"] == "success"
        assert d["duration_s"] == 1.234
        assert d["exit_code"] == 0
        assert d["error_output"] is None

    def test_to_dict_failed_with_error(self) -> None:
        result = StepResult(
            step="transforms",
            status="failed",
            duration_s=0.5,
            exit_code=1,
            error_output="DuckDB: table not found",
        )
        d = result.to_dict()

        assert d["status"] == "failed"
        assert d["exit_code"] == 1
        assert d["error_output"] == "DuckDB: table not found"

    def test_to_dict_skipped(self) -> None:
        result = StepResult(step="checks", status="skipped")
        d = result.to_dict()

        assert d["status"] == "skipped"
        assert d["exit_code"] is None
        assert d["error_output"] is None

    def test_to_dict_duration_rounded(self) -> None:
        result = StepResult(step="generate", status="success", duration_s=1.23456789)
        d = result.to_dict()

        assert d["duration_s"] == round(1.23456789, 3)

    def test_to_dict_is_json_serialisable(self) -> None:
        import json

        result = StepResult(
            step="generate",
            status="success",
            duration_s=1.0,
            exit_code=0,
        )
        # Should not raise
        json.dumps(result.to_dict())

    def test_to_dict_failed_is_json_serialisable(self) -> None:
        import json

        result = StepResult(
            step="ingest",
            status="failed",
            duration_s=0.1,
            exit_code=1,
            error_output="error text",
        )
        json.dumps(result.to_dict())


class TestPipelineRun:
    """Tests for PipelineRun dataclass."""

    def test_default_run_id_is_uuid(self) -> None:
        import re

        run = PipelineRun()
        uuid_re = re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        )
        assert uuid_re.match(run.run_id)

    def test_two_runs_have_different_run_ids(self) -> None:
        run_a = PipelineRun()
        run_b = PipelineRun()
        assert run_a.run_id != run_b.run_id

    def test_default_status_is_success(self) -> None:
        run = PipelineRun()
        assert run.status == "success"

    def test_default_dry_run_is_false(self) -> None:
        run = PipelineRun()
        assert run.dry_run is False

    def test_started_at_is_utc(self) -> None:
        before = datetime.now(UTC)
        run = PipelineRun()
        after = datetime.now(UTC)
        assert before <= run.started_at <= after

    def test_finished_at_none_by_default(self) -> None:
        run = PipelineRun()
        assert run.finished_at is None

    def test_complete_sets_finished_at(self) -> None:
        run = PipelineRun()
        before = datetime.now(UTC)
        run.complete()
        after = datetime.now(UTC)
        assert run.finished_at is not None
        assert before <= run.finished_at <= after

    def test_complete_finished_at_after_started_at(self) -> None:
        run = PipelineRun()
        run.complete()
        assert run.finished_at is not None
        assert run.finished_at >= run.started_at

    def test_mark_failed_sets_status(self) -> None:
        run = PipelineRun()
        run.mark_failed()
        assert run.status == "failed"

    def test_step_results_default_empty(self) -> None:
        run = PipelineRun()
        assert run.step_results == []

    def test_step_results_not_shared(self) -> None:
        run_a = PipelineRun()
        run_b = PipelineRun()
        run_a.step_results.append(StepResult(step="generate", status="success"))
        assert run_b.step_results == []
