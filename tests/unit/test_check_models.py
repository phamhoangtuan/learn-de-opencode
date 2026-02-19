"""Unit tests for the check framework models."""

from __future__ import annotations

from src.checker.models import (
    CheckResult,
    CheckRunResult,
    CheckSeverity,
    CheckStatus,
    RunStatus,
)


class TestCheckSeverity:
    """Tests for CheckSeverity enum."""

    def test_critical_value(self) -> None:
        assert CheckSeverity.CRITICAL.value == "critical"

    def test_warn_value(self) -> None:
        assert CheckSeverity.WARN.value == "warn"

    def test_all_values(self) -> None:
        assert {s.value for s in CheckSeverity} == {"critical", "warn"}


class TestCheckStatus:
    """Tests for CheckStatus enum."""

    def test_pass_value(self) -> None:
        assert CheckStatus.PASS.value == "pass"

    def test_fail_value(self) -> None:
        assert CheckStatus.FAIL.value == "fail"

    def test_error_value(self) -> None:
        assert CheckStatus.ERROR.value == "error"


class TestRunStatus:
    """Tests for RunStatus enum."""

    def test_passed_value(self) -> None:
        assert RunStatus.PASSED.value == "passed"

    def test_warn_value(self) -> None:
        assert RunStatus.WARN.value == "warn"

    def test_failed_value(self) -> None:
        assert RunStatus.FAILED.value == "failed"

    def test_error_value(self) -> None:
        assert RunStatus.ERROR.value == "error"


class TestCheckResult:
    """Tests for CheckResult dataclass."""

    def test_defaults(self) -> None:
        r = CheckResult(
            name="test",
            severity=CheckSeverity.WARN,
            status=CheckStatus.PASS,
        )
        assert r.violation_count == 0
        assert r.sample_violations == []
        assert r.elapsed_seconds == 0.0
        assert r.error is None

    def test_with_violations(self) -> None:
        r = CheckResult(
            name="test",
            severity=CheckSeverity.CRITICAL,
            status=CheckStatus.FAIL,
            violation_count=3,
            sample_violations=[{"id": 1}, {"id": 2}, {"id": 3}],
        )
        assert r.violation_count == 3
        assert len(r.sample_violations) == 3


class TestCheckRunResult:
    """Tests for CheckRunResult dataclass."""

    def test_defaults(self) -> None:
        r = CheckRunResult()
        assert r.status == RunStatus.PASSED
        assert r.total_checks == 0
        assert r.checks_passed == 0
        assert r.checks_failed == 0
        assert r.checks_errored == 0
        assert r.check_results == []
        assert r.error_message is None

    def test_add_pass_result(self) -> None:
        r = CheckRunResult()
        r.add_check_result(
            CheckResult(
                name="c1",
                severity=CheckSeverity.WARN,
                status=CheckStatus.PASS,
            )
        )
        assert r.checks_passed == 1
        assert r.checks_failed == 0

    def test_add_fail_result(self) -> None:
        r = CheckRunResult()
        r.add_check_result(
            CheckResult(
                name="c1",
                severity=CheckSeverity.CRITICAL,
                status=CheckStatus.FAIL,
                violation_count=2,
            )
        )
        assert r.checks_failed == 1
        assert r.checks_passed == 0

    def test_add_error_result(self) -> None:
        r = CheckRunResult()
        r.add_check_result(
            CheckResult(
                name="c1",
                severity=CheckSeverity.WARN,
                status=CheckStatus.ERROR,
                error="bad sql",
            )
        )
        assert r.checks_errored == 1

    def test_compute_status_all_pass(self) -> None:
        r = CheckRunResult()
        r.add_check_result(
            CheckResult(
                name="c1",
                severity=CheckSeverity.CRITICAL,
                status=CheckStatus.PASS,
            )
        )
        r.compute_status()
        assert r.status == RunStatus.PASSED

    def test_compute_status_critical_fail(self) -> None:
        r = CheckRunResult()
        r.add_check_result(
            CheckResult(
                name="c1",
                severity=CheckSeverity.CRITICAL,
                status=CheckStatus.FAIL,
                violation_count=1,
            )
        )
        r.compute_status()
        assert r.status == RunStatus.FAILED

    def test_compute_status_warn_fail_only(self) -> None:
        r = CheckRunResult()
        r.add_check_result(
            CheckResult(
                name="c1",
                severity=CheckSeverity.WARN,
                status=CheckStatus.FAIL,
                violation_count=1,
            )
        )
        r.compute_status()
        assert r.status == RunStatus.WARN

    def test_compute_status_error_is_warn(self) -> None:
        """Errors (without critical fails) result in WARN status."""
        r = CheckRunResult()
        r.add_check_result(
            CheckResult(
                name="c1",
                severity=CheckSeverity.WARN,
                status=CheckStatus.ERROR,
                error="bad sql",
            )
        )
        r.compute_status()
        assert r.status == RunStatus.WARN

    def test_compute_status_critical_trumps_warn(self) -> None:
        r = CheckRunResult()
        r.add_check_result(
            CheckResult(
                name="c1",
                severity=CheckSeverity.WARN,
                status=CheckStatus.FAIL,
                violation_count=1,
            )
        )
        r.add_check_result(
            CheckResult(
                name="c2",
                severity=CheckSeverity.CRITICAL,
                status=CheckStatus.FAIL,
                violation_count=1,
            )
        )
        r.compute_status()
        assert r.status == RunStatus.FAILED

    def test_summary_contains_status(self) -> None:
        r = CheckRunResult()
        r.compute_status()
        summary = r.summary()
        assert "PASSED" in summary

    def test_summary_contains_check_details(self) -> None:
        r = CheckRunResult()
        r.add_check_result(
            CheckResult(
                name="my_check",
                severity=CheckSeverity.WARN,
                status=CheckStatus.FAIL,
                violation_count=3,
            )
        )
        summary = r.summary()
        assert "my_check" in summary
        assert "3 violations" in summary

    def test_run_id_is_uuid(self) -> None:
        import uuid

        r = CheckRunResult()
        uuid.UUID(r.run_id)  # Should not raise
