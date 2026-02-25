"""Unit tests for the pipeline runner (T010).

Uses unittest.mock to avoid real subprocess calls or DuckDB writes.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.orchestrator.models import PipelineStep
from src.orchestrator.runner import _resolve_active_steps, run_pipeline

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_PATCH_SUBPROCESS = "src.orchestrator.runner.subprocess.run"


@pytest.fixture()
def linear_steps() -> list[PipelineStep]:
    """Return a 5-step linear pipeline matching the canonical definition."""
    return [
        PipelineStep(
            name="generate",
            command=["uv", "run", "src/generate_transactions.py"],
        ),
        PipelineStep(
            name="ingest",
            command=["uv", "run", "src/ingest_transactions.py"],
            depends_on=["generate"],
        ),
        PipelineStep(
            name="transforms",
            command=["uv", "run", "src/run_transforms.py"],
            depends_on=["ingest"],
        ),
        PipelineStep(
            name="checks",
            command=["uv", "run", "src/run_checks.py"],
            depends_on=["transforms"],
        ),
        PipelineStep(
            name="dashboard",
            command=["npm", "run", "sources"],
            cwd="dashboard",
            depends_on=["checks"],
        ),
    ]


# ---------------------------------------------------------------------------
# _resolve_active_steps
# ---------------------------------------------------------------------------


class TestResolveActiveSteps:
    """Tests for step filtering logic."""

    def test_no_filter_returns_all(self, linear_steps: list[PipelineStep]) -> None:
        result = _resolve_active_steps(linear_steps, None, None)
        assert [s.name for s in result] == [
            "generate",
            "ingest",
            "transforms",
            "checks",
            "dashboard",
        ]

    def test_step_name_single(self, linear_steps: list[PipelineStep]) -> None:
        result = _resolve_active_steps(linear_steps, "transforms", None)
        assert [s.name for s in result] == ["transforms"]

    def test_from_step_returns_slice(self, linear_steps: list[PipelineStep]) -> None:
        result = _resolve_active_steps(linear_steps, None, "transforms")
        assert [s.name for s in result] == ["transforms", "checks", "dashboard"]

    def test_from_step_first_step_returns_all(
        self, linear_steps: list[PipelineStep]
    ) -> None:
        result = _resolve_active_steps(linear_steps, None, "generate")
        assert len(result) == 5

    def test_from_step_last_step(self, linear_steps: list[PipelineStep]) -> None:
        result = _resolve_active_steps(linear_steps, None, "dashboard")
        assert [s.name for s in result] == ["dashboard"]

    def test_unknown_step_name_raises(self, linear_steps: list[PipelineStep]) -> None:
        with pytest.raises(ValueError, match="Unknown step 'foo'"):
            _resolve_active_steps(linear_steps, "foo", None)

    def test_unknown_from_step_raises(self, linear_steps: list[PipelineStep]) -> None:
        with pytest.raises(ValueError, match="Unknown step 'bar'"):
            _resolve_active_steps(linear_steps, None, "bar")

    def test_error_message_lists_available_steps(
        self, linear_steps: list[PipelineStep]
    ) -> None:
        with pytest.raises(ValueError, match="generate"):
            _resolve_active_steps(linear_steps, "bad", None)


# ---------------------------------------------------------------------------
# run_pipeline -- dry-run
# ---------------------------------------------------------------------------


class TestRunPipelineDryRun:
    """Tests for dry-run mode (no subprocess calls, no DuckDB writes)."""

    def test_dry_run_no_subprocess(
        self, linear_steps: list[PipelineStep], tmp_path: str
    ) -> None:
        with patch(_PATCH_SUBPROCESS) as mock_sub:
            run = run_pipeline(
                linear_steps,
                dry_run=True,
                project_root=str(tmp_path),
            )
        mock_sub.assert_not_called()
        assert run.dry_run is True

    def test_dry_run_status_success(
        self, linear_steps: list[PipelineStep], tmp_path: str
    ) -> None:
        with patch(_PATCH_SUBPROCESS):
            run = run_pipeline(
                linear_steps,
                dry_run=True,
                project_root=str(tmp_path),
            )
        assert run.status == "success"

    def test_dry_run_finished_at_set(
        self, linear_steps: list[PipelineStep], tmp_path: str
    ) -> None:
        with patch(_PATCH_SUBPROCESS):
            run = run_pipeline(
                linear_steps,
                dry_run=True,
                project_root=str(tmp_path),
            )
        assert run.finished_at is not None

    def test_dry_run_no_step_results(
        self, linear_steps: list[PipelineStep], tmp_path: str
    ) -> None:
        with patch(_PATCH_SUBPROCESS):
            run = run_pipeline(
                linear_steps,
                dry_run=True,
                project_root=str(tmp_path),
            )
        # Dry-run does not populate step_results
        assert run.step_results == []


# ---------------------------------------------------------------------------
# run_pipeline -- step filtering (mocked subprocess)
# ---------------------------------------------------------------------------


class TestRunPipelineStepFiltering:
    """Tests for --step and --from-step with mocked subprocesses."""

    def _success(self) -> MagicMock:
        mock = MagicMock()
        mock.returncode = 0
        return mock

    def test_single_step_runs_one(
        self, linear_steps: list[PipelineStep], tmp_path: str
    ) -> None:
        with patch(_PATCH_SUBPROCESS, return_value=self._success()):
            run = run_pipeline(
                linear_steps,
                step_name="transforms",
                project_root=str(tmp_path),
            )
        executed = [r for r in run.step_results if r.status == "success"]
        assert len(executed) == 1
        assert executed[0].step == "transforms"

    def test_single_step_others_skipped(
        self, linear_steps: list[PipelineStep], tmp_path: str
    ) -> None:
        with patch(_PATCH_SUBPROCESS, return_value=self._success()):
            run = run_pipeline(
                linear_steps,
                step_name="transforms",
                project_root=str(tmp_path),
            )
        skipped = [r for r in run.step_results if r.status == "skipped"]
        assert len(skipped) == 4

    def test_from_step_runs_slice(
        self, linear_steps: list[PipelineStep], tmp_path: str
    ) -> None:
        with patch(_PATCH_SUBPROCESS, return_value=self._success()):
            run = run_pipeline(
                linear_steps,
                from_step_name="transforms",
                project_root=str(tmp_path),
            )
        executed = [r for r in run.step_results if r.status == "success"]
        assert [r.step for r in executed] == ["transforms", "checks", "dashboard"]

    def test_from_step_earlier_steps_skipped(
        self, linear_steps: list[PipelineStep], tmp_path: str
    ) -> None:
        with patch(_PATCH_SUBPROCESS, return_value=self._success()):
            run = run_pipeline(
                linear_steps,
                from_step_name="transforms",
                project_root=str(tmp_path),
            )
        skipped = [r for r in run.step_results if r.status == "skipped"]
        assert {r.step for r in skipped} == {"generate", "ingest"}


# ---------------------------------------------------------------------------
# run_pipeline -- halt-on-failure
# ---------------------------------------------------------------------------


class TestRunPipelineHaltOnFailure:
    """Tests for pipeline halt behaviour on step failure."""

    def test_failure_halts_pipeline(
        self, linear_steps: list[PipelineStep], tmp_path: str
    ) -> None:
        """Pipeline must stop after first failed step."""
        fail_result = MagicMock()
        fail_result.returncode = 1

        capture_result = MagicMock()
        capture_result.returncode = 1
        capture_result.stderr = "error details"
        capture_result.stdout = ""

        def side_effect(cmd: list[str], **kwargs: object) -> MagicMock:
            if kwargs.get("capture_output"):
                return capture_result
            if cmd == ["uv", "run", "src/ingest_transactions.py"]:
                return fail_result
            success = MagicMock()
            success.returncode = 0
            return success

        with patch(_PATCH_SUBPROCESS, side_effect=side_effect):
            run = run_pipeline(
                linear_steps,
                project_root=str(tmp_path),
            )

        assert run.status == "failed"
        step_names = [r.step for r in run.step_results]
        assert "transforms" not in step_names
        assert "checks" not in step_names
        assert "dashboard" not in step_names

    def test_failure_recorded_in_results(
        self, linear_steps: list[PipelineStep], tmp_path: str
    ) -> None:
        fail_result = MagicMock()
        fail_result.returncode = 1

        capture_result = MagicMock()
        capture_result.returncode = 1
        capture_result.stderr = "DuckDB error"
        capture_result.stdout = ""

        def side_effect(cmd: list[str], **kwargs: object) -> MagicMock:
            if kwargs.get("capture_output"):
                return capture_result
            if cmd == ["uv", "run", "src/ingest_transactions.py"]:
                return fail_result
            success = MagicMock()
            success.returncode = 0
            return success

        with patch(_PATCH_SUBPROCESS, side_effect=side_effect):
            run = run_pipeline(
                linear_steps,
                project_root=str(tmp_path),
            )

        failed_steps = [r for r in run.step_results if r.status == "failed"]
        assert len(failed_steps) == 1
        assert failed_steps[0].step == "ingest"
        assert failed_steps[0].exit_code == 1

    def test_full_success_status(
        self, linear_steps: list[PipelineStep], tmp_path: str
    ) -> None:
        success_result = MagicMock()
        success_result.returncode = 0

        with patch(_PATCH_SUBPROCESS, return_value=success_result):
            run = run_pipeline(
                linear_steps,
                project_root=str(tmp_path),
            )

        assert run.status == "success"
        executed = [r for r in run.step_results if r.status == "success"]
        assert len(executed) == 5

    def test_step_and_from_step_rejected(
        self, linear_steps: list[PipelineStep], tmp_path: str
    ) -> None:
        with pytest.raises(ValueError, match="cannot be used together"):
            run_pipeline(
                linear_steps,
                step_name="generate",
                from_step_name="ingest",
                project_root=str(tmp_path),
            )


# ---------------------------------------------------------------------------
# _resolve_active_steps -- skip_step_names
# ---------------------------------------------------------------------------


class TestResolveActiveStepsSkip:
    """Tests for --skip-steps filtering logic."""

    def test_skip_dashboard_excludes_it(self, linear_steps: list[PipelineStep]) -> None:
        result = _resolve_active_steps(linear_steps, None, None, {"dashboard"})
        names = [s.name for s in result]
        assert "dashboard" not in names
        assert names == ["generate", "ingest", "transforms", "checks"]

    def test_skipped_step_in_results_with_skipped_status(
        self, linear_steps: list[PipelineStep], tmp_path: str
    ) -> None:
        success_result = MagicMock()
        success_result.returncode = 0

        with patch(_PATCH_SUBPROCESS, return_value=success_result):
            run = run_pipeline(
                linear_steps,
                skip_step_names={"dashboard"},
                project_root=str(tmp_path),
            )

        statuses = {r.step: r.status for r in run.step_results}
        assert statuses["dashboard"] == "skipped"
        assert statuses["generate"] == "success"
        assert statuses["checks"] == "success"

    def test_skip_all_steps_pipeline_succeeds(
        self, linear_steps: list[PipelineStep], tmp_path: str
    ) -> None:
        all_names = {s.name for s in linear_steps}
        with patch(_PATCH_SUBPROCESS) as mock_sub:
            run = run_pipeline(
                linear_steps,
                skip_step_names=all_names,
                project_root=str(tmp_path),
            )
        mock_sub.assert_not_called()
        assert run.status == "success"
        skipped = [r for r in run.step_results if r.status == "skipped"]
        assert len(skipped) == 5

    def test_skip_combined_with_step_skip_takes_effect(
        self, linear_steps: list[PipelineStep], tmp_path: str
    ) -> None:
        # --step generate --skip-steps generate → nothing runs
        success_result = MagicMock()
        success_result.returncode = 0

        with patch(_PATCH_SUBPROCESS) as mock_sub:
            run = run_pipeline(
                linear_steps,
                step_name="generate",
                skip_step_names={"generate"},
                project_root=str(tmp_path),
            )
        mock_sub.assert_not_called()
        assert run.status == "success"

    def test_skip_multiple_steps(self, linear_steps: list[PipelineStep]) -> None:
        result = _resolve_active_steps(
            linear_steps, None, None, {"checks", "dashboard"}
        )
        names = [s.name for s in result]
        assert names == ["generate", "ingest", "transforms"]
