"""Unit tests for ingestion domain models (T008)."""

from __future__ import annotations

from src.ingestion.models import (
    FileResult,
    RunResult,
    RunStatus,
    ValidationError,
    ValidationResult,
)


class TestRunStatus:
    """Tests for the RunStatus enum."""

    def test_running_value(self) -> None:
        assert RunStatus.RUNNING.value == "running"

    def test_completed_value(self) -> None:
        assert RunStatus.COMPLETED.value == "completed"

    def test_failed_value(self) -> None:
        assert RunStatus.FAILED.value == "failed"

    def test_all_values_present(self) -> None:
        values = {s.value for s in RunStatus}
        assert values == {"running", "completed", "failed"}


class TestFileResult:
    """Tests for the FileResult dataclass."""

    def test_defaults(self) -> None:
        result = FileResult(file_name="test.parquet")
        assert result.file_name == "test.parquet"
        assert result.records_loaded == 0
        assert result.records_quarantined == 0
        assert result.duplicates_skipped == 0
        assert result.error is None

    def test_with_counts(self) -> None:
        result = FileResult(
            file_name="data.parquet",
            records_loaded=100,
            records_quarantined=5,
            duplicates_skipped=3,
        )
        assert result.records_loaded == 100
        assert result.records_quarantined == 5
        assert result.duplicates_skipped == 3

    def test_with_error(self) -> None:
        result = FileResult(
            file_name="bad.parquet",
            error="Missing columns: ['amount']",
        )
        assert result.error == "Missing columns: ['amount']"
        assert result.records_loaded == 0


class TestRunResult:
    """Tests for the RunResult dataclass."""

    def test_defaults(self) -> None:
        result = RunResult(run_id="run-001")
        assert result.run_id == "run-001"
        assert result.status == RunStatus.RUNNING
        assert result.files_processed == 0
        assert result.records_loaded == 0
        assert result.records_quarantined == 0
        assert result.duplicates_skipped == 0
        assert result.elapsed_seconds == 0.0
        assert result.file_results == []

    def test_add_file_result_updates_counters(self) -> None:
        run = RunResult(run_id="run-002")
        file_result = FileResult(
            file_name="f1.parquet",
            records_loaded=50,
            records_quarantined=3,
            duplicates_skipped=2,
        )
        run.add_file_result(file_result)

        assert run.files_processed == 1
        assert run.records_loaded == 50
        assert run.records_quarantined == 3
        assert run.duplicates_skipped == 2
        assert len(run.file_results) == 1

    def test_add_multiple_file_results_accumulates(self) -> None:
        run = RunResult(run_id="run-003")
        run.add_file_result(FileResult(
            file_name="f1.parquet",
            records_loaded=100,
            records_quarantined=5,
            duplicates_skipped=0,
        ))
        run.add_file_result(FileResult(
            file_name="f2.parquet",
            records_loaded=200,
            records_quarantined=10,
            duplicates_skipped=3,
        ))

        assert run.files_processed == 2
        assert run.records_loaded == 300
        assert run.records_quarantined == 15
        assert run.duplicates_skipped == 3

    def test_summary_contains_key_info(self) -> None:
        run = RunResult(run_id="run-004")
        run.status = RunStatus.COMPLETED
        run.files_processed = 3
        run.records_loaded = 500
        run.records_quarantined = 10
        run.duplicates_skipped = 5
        run.elapsed_seconds = 1.23

        summary = run.summary()

        assert "run-004" in summary
        assert "completed" in summary
        assert "3" in summary
        assert "500" in summary
        assert "10" in summary
        assert "5" in summary
        assert "1.23" in summary

    def test_file_results_list_independence(self) -> None:
        """Verify each RunResult has its own file_results list (no sharing)."""
        run_a = RunResult(run_id="a")
        run_b = RunResult(run_id="b")
        run_a.add_file_result(FileResult(file_name="f.parquet", records_loaded=1))

        assert len(run_a.file_results) == 1
        assert len(run_b.file_results) == 0


class TestValidationError:
    """Tests for the ValidationError dataclass."""

    def test_field_and_message(self) -> None:
        err = ValidationError(field="amount", message="must be positive")
        assert err.field == "amount"
        assert err.message == "must be positive"

    def test_schema_level_error(self) -> None:
        err = ValidationError(field="schema", message="missing columns")
        assert err.field == "schema"


class TestValidationResult:
    """Tests for the ValidationResult dataclass."""

    def test_valid_result(self) -> None:
        result = ValidationResult(is_valid=True)
        assert result.is_valid is True
        assert result.errors == []
        assert result.valid_mask is None

    def test_invalid_result_with_errors(self) -> None:
        errors = [
            ValidationError(field="amount", message="negative"),
            ValidationError(field="currency", message="invalid"),
        ]
        result = ValidationResult(is_valid=False, errors=errors)
        assert result.is_valid is False
        assert len(result.errors) == 2

    def test_with_valid_mask(self) -> None:
        result = ValidationResult(
            is_valid=False,
            errors=[ValidationError(field="amount", message="negative")],
            valid_mask=[True, False, True],
        )
        assert result.valid_mask == [True, False, True]

    def test_errors_list_independence(self) -> None:
        """Verify each ValidationResult has its own errors list."""
        result_a = ValidationResult(is_valid=True)
        result_b = ValidationResult(is_valid=True)
        result_a.errors.append(ValidationError(field="x", message="y"))

        assert len(result_a.errors) == 1
        assert len(result_b.errors) == 0
