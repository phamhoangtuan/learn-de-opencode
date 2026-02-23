"""Domain models for the pipeline orchestration layer."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass
class PipelineStep:
    """A declarative node in the pipeline DAG.

    Designed to satisfy the duck-type interface expected by
    ``src.transformer.graph.resolve_execution_order``, which only
    requires ``.name: str`` and ``.depends_on: list[str]``.

    Attributes:
        name: Canonical step identifier (e.g. ``"generate"``).
        command: Subprocess argv list (e.g. ``["uv", "run", "src/generate_transactions.py"]``).
        cwd: Working directory for the subprocess. ``None`` means project root.
        depends_on: Names of steps this step must run after.
    """

    name: str
    command: list[str]
    cwd: str | None = None
    depends_on: list[str] = field(default_factory=list)


@dataclass
class StepResult:
    """Outcome of executing one pipeline step.

    Attributes:
        step: Canonical step name.
        status: One of ``"success"``, ``"failed"``, ``"skipped"``.
        duration_s: Wall-clock execution time in seconds.
        exit_code: Subprocess exit code. ``None`` for skipped steps.
        error_output: Up to 2000 characters of stderr on failure. ``None`` otherwise.
    """

    step: str
    status: str  # "success" | "failed" | "skipped"
    duration_s: float = 0.0
    exit_code: int | None = None
    error_output: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serialisable dict of this result.

        Returns:
            Dict with ``step``, ``status``, ``duration_s``, ``exit_code``,
            ``error_output`` keys.
        """
        return {
            "step": self.step,
            "status": self.status,
            "duration_s": round(self.duration_s, 3),
            "exit_code": self.exit_code,
            "error_output": self.error_output,
        }


@dataclass
class PipelineRun:
    """Aggregate result for one orchestrator invocation.

    Attributes:
        run_id: UUID string identifying this run.
        status: Overall status: ``"success"`` or ``"failed"``.
        started_at: UTC timestamp when orchestration started.
        finished_at: UTC timestamp when orchestration finished. ``None`` if still running.
        step_results: Per-step outcomes, including skipped steps.
        dry_run: Whether this was a dry-run (no subprocess calls, no DuckDB writes).
    """

    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: str = "success"
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
    step_results: list[StepResult] = field(default_factory=list)
    dry_run: bool = False

    def complete(self) -> None:
        """Mark the run as finished, recording the end timestamp."""
        self.finished_at = datetime.now(UTC)

    def mark_failed(self) -> None:
        """Set overall status to ``"failed"``."""
        self.status = "failed"
