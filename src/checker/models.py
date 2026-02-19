"""Domain models for the data quality check framework."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path


class CheckSeverity(Enum):
    """Severity level of a quality check.

    Determines how a failure impacts the overall run status.
    """

    CRITICAL = "critical"
    WARN = "warn"


class CheckStatus(Enum):
    """Outcome of executing a single quality check.

    - PASS: zero violating rows returned.
    - FAIL: one or more violating rows returned.
    - ERROR: SQL execution error (syntax, missing table, etc.).
    """

    PASS = "pass"
    FAIL = "fail"
    ERROR = "error"


class RunStatus(Enum):
    """Overall status of a check-runner invocation.

    - PASSED: all checks passed.
    - WARN: at least one warn-severity check failed, no critical failures.
    - FAILED: at least one critical-severity check failed.
    - ERROR: runner-level error (e.g., missing database).
    """

    PASSED = "passed"
    WARN = "warn"
    FAILED = "failed"
    ERROR = "error"


@dataclass
class CheckModel:
    """A parsed quality check SQL file.

    Attributes:
        name: Check name from ``-- check:`` header or filename.
        file_path: Absolute path to the SQL file.
        sql: The SQL SELECT statement to execute.
        severity: Critical or warn.
        description: Human-readable description of what is checked.
    """

    name: str
    file_path: Path
    sql: str
    severity: CheckSeverity = CheckSeverity.WARN
    description: str = ""


@dataclass
class CheckResult:
    """Result of executing a single quality check.

    Attributes:
        name: Check name.
        severity: Critical or warn.
        status: Pass, fail, or error.
        violation_count: Number of violating rows (0 for pass).
        sample_violations: Up to 5 violating rows as dicts.
        elapsed_seconds: Execution time in seconds.
        error: Error message if status is ERROR.
    """

    name: str
    severity: CheckSeverity
    status: CheckStatus
    violation_count: int = 0
    sample_violations: list[dict[str, object]] = field(
        default_factory=list,
    )
    elapsed_seconds: float = 0.0
    error: str | None = None


@dataclass
class CheckRunResult:
    """Aggregate result for one check-runner invocation.

    Attributes:
        run_id: Unique identifier for this run.
        status: Overall run status.
        total_checks: Number of checks discovered.
        checks_passed: Number that passed.
        checks_failed: Number that failed.
        checks_errored: Number that errored.
        elapsed_seconds: Total wall-clock time.
        check_results: Per-check details.
        error_message: Top-level error if run couldn't complete.
    """

    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: RunStatus = RunStatus.PASSED
    total_checks: int = 0
    checks_passed: int = 0
    checks_failed: int = 0
    checks_errored: int = 0
    elapsed_seconds: float = 0.0
    check_results: list[CheckResult] = field(default_factory=list)
    error_message: str | None = None

    def add_check_result(self, result: CheckResult) -> None:
        """Add a check result and update counters.

        Args:
            result: The check result to add.
        """
        self.check_results.append(result)
        if result.status == CheckStatus.PASS:
            self.checks_passed += 1
        elif result.status == CheckStatus.FAIL:
            self.checks_failed += 1
        elif result.status == CheckStatus.ERROR:
            self.checks_errored += 1

    def compute_status(self) -> None:
        """Compute overall run status from individual check results.

        - Any critical failure => FAILED
        - Only warn failures => WARN
        - All pass => PASSED
        """
        has_critical_fail = any(
            r.status == CheckStatus.FAIL
            and r.severity == CheckSeverity.CRITICAL
            for r in self.check_results
        )
        has_warn_fail = any(
            r.status == CheckStatus.FAIL
            and r.severity == CheckSeverity.WARN
            for r in self.check_results
        )
        has_any_error = any(
            r.status == CheckStatus.ERROR for r in self.check_results
        )

        if has_critical_fail:
            self.status = RunStatus.FAILED
        elif has_warn_fail or has_any_error:
            self.status = RunStatus.WARN
        else:
            self.status = RunStatus.PASSED

    def summary(self) -> str:
        """Generate a human-readable summary of the check run.

        Returns:
            Multi-line string with run status and per-check details.
        """
        lines = [
            f"Check Run: {self.run_id}",
            f"Status: {self.status.value.upper()}",
            f"Total: {self.total_checks} | "
            f"Passed: {self.checks_passed} | "
            f"Failed: {self.checks_failed} | "
            f"Errored: {self.checks_errored}",
            f"Elapsed: {self.elapsed_seconds:.2f}s",
            "",
        ]

        for r in self.check_results:
            icon = (
                "PASS" if r.status == CheckStatus.PASS
                else "FAIL" if r.status == CheckStatus.FAIL
                else "ERROR"
            )
            severity_tag = f"[{r.severity.value}]"
            line = f"  {icon} {severity_tag} {r.name}"
            if r.status == CheckStatus.FAIL:
                line += f" ({r.violation_count} violations)"
            if r.status == CheckStatus.ERROR and r.error:
                line += f" -- {r.error}"
            lines.append(line)

        if self.error_message:
            lines.append(f"\nRun Error: {self.error_message}")

        return "\n".join(lines)
