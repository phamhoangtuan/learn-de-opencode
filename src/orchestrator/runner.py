"""Core execution logic for the pipeline orchestrator.

Resolves the DAG execution order, applies step filters (--step / --from-step /
--skip-steps), invokes each step as a subprocess, emits structured JSON logs to
stdout, and collects per-step results into a PipelineRun.
"""

from __future__ import annotations

import json
import logging
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from src.orchestrator.models import PipelineRun, PipelineStep, StepResult
from src.transformer.graph import resolve_execution_order
from src.transformer.models import TransformModel

logger = logging.getLogger(__name__)

_MAX_ERROR_CHARS = 2000


def _emit_json_log(run_id: str, result: StepResult) -> None:
    """Emit one JSON log line to stdout for a completed step.

    Each line is independently parseable by ``json.loads()``.

    Args:
        run_id: The pipeline run UUID.
        result: The completed step result.
    """
    payload = {
        "run_id": run_id,
        "step": result.step,
        "status": result.status,
        "duration_s": result.duration_s,
        "exit_code": result.exit_code,
        "timestamp": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if result.error_output:
        payload["error_output"] = result.error_output

    print(json.dumps(payload), flush=True)


def _resolve_active_steps(
    ordered_steps: list[PipelineStep],
    step_name: str | None,
    from_step_name: str | None,
    skip_step_names: set[str] | None = None,
) -> list[PipelineStep]:
    """Return the subset of steps to execute based on CLI filters.

    Filters are applied in order:
    1. ``step_name`` — keep only this single step.
    2. ``from_step_name`` — keep from this step onwards (inclusive).
    3. ``skip_step_names`` — remove named steps from the active set.

    Args:
        ordered_steps: Full list of steps in resolved execution order.
        step_name: If set, run only this single step.
        from_step_name: If set, run from this step onwards (inclusive).
        skip_step_names: If set, exclude these step names from the active set.

    Returns:
        The filtered list of PipelineStep objects to run.

    Raises:
        ValueError: If ``step_name`` or ``from_step_name`` is not a known step.
    """
    names = [s.name for s in ordered_steps]

    if step_name is not None:
        if step_name not in names:
            raise ValueError(
                f"Unknown step '{step_name}'. Available: {', '.join(names)}"
            )
        active = [s for s in ordered_steps if s.name == step_name]
    elif from_step_name is not None:
        if from_step_name not in names:
            raise ValueError(
                f"Unknown step '{from_step_name}'. Available: {', '.join(names)}"
            )
        idx = names.index(from_step_name)
        active = ordered_steps[idx:]
    else:
        active = list(ordered_steps)

    if skip_step_names:
        active = [s for s in active if s.name not in skip_step_names]

    return active


def _run_step(step: PipelineStep, project_root: str) -> StepResult:
    """Execute a single pipeline step as a subprocess.

    Streams stdout/stderr to the terminal in real time. On failure,
    re-runs the step with output capture to extract an error snippet
    for metadata storage.

    Args:
        step: The step to execute.
        project_root: Absolute path to the project root directory.

    Returns:
        StepResult with status, duration, exit code, and optional error output.
    """
    cwd = str(Path(project_root) / step.cwd) if step.cwd else project_root

    print(
        f"\n[pipeline] Running step '{step.name}': {' '.join(step.command)}",
        file=sys.stderr,
        flush=True,
    )

    start = time.monotonic()
    result = subprocess.run(step.command, cwd=cwd)
    elapsed = time.monotonic() - start

    if result.returncode == 0:
        logger.debug("Step '%s' completed in %.2fs", step.name, elapsed)
        return StepResult(
            step=step.name,
            status="success",
            duration_s=elapsed,
            exit_code=0,
        )

    # Step failed — capture stderr for metadata (separate call)
    logger.warning("Step '%s' failed (exit %d)", step.name, result.returncode)
    capture = subprocess.run(
        step.command,
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    error_text = (capture.stderr or capture.stdout or "")[-_MAX_ERROR_CHARS:]

    return StepResult(
        step=step.name,
        status="failed",
        duration_s=elapsed,
        exit_code=result.returncode,
        error_output=error_text or None,
    )


def run_pipeline(
    steps: list[PipelineStep],
    *,
    step_name: str | None = None,
    from_step_name: str | None = None,
    skip_step_names: set[str] | None = None,
    dry_run: bool = False,
    project_root: str,
) -> PipelineRun:
    """Execute the pipeline DAG.

    Resolves the execution order via ``resolve_execution_order``, applies
    step filters, then either prints the dry-run plan or invokes each step
    as a subprocess. Halts on the first failure.

    Args:
        steps: Declarative list of PipelineStep nodes (unordered is fine).
        step_name: If set, run only this single named step.
        from_step_name: If set, run from this step through the end.
        skip_step_names: If set, exclude these named steps from execution.
        dry_run: If True, print the plan and return without executing anything.
        project_root: Absolute path to the project root (used as cwd base).

    Returns:
        A PipelineRun with all StepResult outcomes and overall status.

    Raises:
        ValueError: If an unknown step name is provided.
    """
    if step_name and from_step_name:
        raise ValueError(
            "--step and --from-step cannot be used together. "
            "Use --step to run one step or --from-step to run from a step onwards."
        )

    # Resolve topological order via Feature 003 graph module.
    # PipelineStep satisfies the duck-type interface (name, depends_on) expected
    # by resolve_execution_order; cast silences the static type checker without
    # modifying the upstream graph module.
    ordered_steps = cast(
        list[PipelineStep],
        resolve_execution_order(cast(list[TransformModel], steps)),
    )

    # Apply filters
    active_steps = _resolve_active_steps(
        ordered_steps, step_name, from_step_name, skip_step_names
    )
    active_names = {s.name for s in active_steps}

    run = PipelineRun(dry_run=dry_run)

    if dry_run:
        print("[pipeline] Dry-run mode — no steps will execute.", file=sys.stderr)
        print("[pipeline] Execution plan:", file=sys.stderr)
        for step in ordered_steps:
            marker = "→" if step.name in active_names else "  (skip)"
            cmd_str = " ".join(step.command)
            cwd_note = f" [cwd={step.cwd}]" if step.cwd else ""
            print(f"  {marker} {step.name}: {cmd_str}{cwd_note}", file=sys.stderr)
        run.complete()
        return run

    # Real execution
    skipped_names = {s.name for s in ordered_steps if s.name not in active_names}

    # Emit skipped steps first (in order)
    for step in ordered_steps:
        if step.name in skipped_names:
            skipped_result = StepResult(step=step.name, status="skipped")
            run.step_results.append(skipped_result)
            _emit_json_log(run.run_id, skipped_result)

    pipeline_failed = False
    for step in active_steps:
        result = _run_step(step, project_root)
        run.step_results.append(result)
        _emit_json_log(run.run_id, result)

        if result.status == "failed":
            run.mark_failed()
            pipeline_failed = True
            logger.error(
                "Pipeline halted after step '%s' failed (exit %d)",
                step.name,
                result.exit_code,
            )
            break

    if not pipeline_failed:
        print("\n[pipeline] All steps completed successfully.", file=sys.stderr)

    run.complete()
    return run
