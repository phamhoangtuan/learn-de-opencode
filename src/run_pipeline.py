# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "duckdb",
# ]
# ///
"""Run the full learn_de pipeline end-to-end.

Wires all 8 pipeline steps into a single runnable DAG:
  1. generate    — Synthetic transaction data generation
  2. ingest      — DuckDB ingestion from Parquet
  3. transforms  — SQL transformation layer
  4. dim_build   — SCD Type 2 accounts dimension build
  5. fact_build  — Star schema fact table materialization
  6. checks      — Data quality checks
  7. catalog     — Data catalog build
  8. dashboard   — Evidence dashboard source refresh

Usage:
    uv run src/run_pipeline.py                              # full pipeline
    uv run src/run_pipeline.py --step transforms            # one step only
    uv run src/run_pipeline.py --from-step transforms       # transforms onwards
    uv run src/run_pipeline.py --skip-steps dashboard       # skip dashboard (CI)
    uv run src/run_pipeline.py --dry-run                    # print plan, no execution
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Ensure project root is in sys.path for uv run script invocation
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.orchestrator.metadata import ensure_pipeline_runs_table, record_pipeline_run  # noqa: E402
from src.orchestrator.models import PipelineStep  # noqa: E402
from src.orchestrator.runner import run_pipeline  # noqa: E402

DEFAULT_DB_PATH = "data/warehouse/transactions.duckdb"

# ---------------------------------------------------------------------------
# Canonical pipeline step definitions (FR-010, FR-012)
# ---------------------------------------------------------------------------

PIPELINE_STEPS: list[PipelineStep] = [
    PipelineStep(
        name="generate",
        command=["uv", "run", "src/generate_transactions.py"],
        cwd=None,  # project root
        depends_on=[],
    ),
    PipelineStep(
        name="ingest",
        command=["uv", "run", "src/ingest_transactions.py"],
        cwd=None,
        depends_on=["generate"],
    ),
    PipelineStep(
        name="transforms",
        command=["uv", "run", "src/run_transforms.py"],
        cwd=None,
        depends_on=["ingest"],
    ),
    PipelineStep(
        name="dim_build",
        command=["uv", "run", "src/run_dim_build.py"],
        cwd=None,
        depends_on=["transforms"],
    ),
    PipelineStep(
        name="fact_build",
        command=["uv", "run", "src/run_transforms.py", "--transforms-dir", "src/fact_transforms"],
        cwd=None,
        depends_on=["dim_build"],
    ),
    PipelineStep(
        name="checks",
        command=["uv", "run", "src/run_checks.py"],
        cwd=None,
        depends_on=["fact_build"],
    ),
    PipelineStep(
        name="catalog",
        command=["uv", "run", "src/run_catalog.py"],
        cwd=None,
        depends_on=["checks"],
    ),
    PipelineStep(
        name="dashboard",
        command=["npm", "run", "sources"],
        cwd="dashboard",
        depends_on=["catalog"],
    ),
]

_VALID_STEP_NAMES = [s.name for s in PIPELINE_STEPS]


def _configure_logging(verbose: bool = False) -> None:
    """Configure logging for the orchestrator.

    Logs go to stderr so stdout remains clean for structured JSON output.

    Args:
        verbose: If True, set log level to DEBUG; otherwise WARNING.
    """
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stderr,
    )


def _validate_step_name(name: str, flag: str) -> None:
    """Validate that a step name is known.

    Args:
        name: The step name to validate.
        flag: The CLI flag name (for error message).

    Raises:
        SystemExit: With exit code 1 if the name is invalid.
    """
    if name not in _VALID_STEP_NAMES:
        print(
            f"Error: Unknown step '{name}' for {flag}.\n"
            f"Available steps: {', '.join(_VALID_STEP_NAMES)}",
            file=sys.stderr,
        )
        sys.exit(1)


def main() -> int:
    """CLI entrypoint for the pipeline orchestrator.

    Returns:
        Exit code: 0 on success or dry-run, 1 on any step failure.
    """
    parser = argparse.ArgumentParser(
        description="Run the learn_de pipeline end-to-end.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  uv run src/run_pipeline.py\n"
            "  uv run src/run_pipeline.py --step transforms\n"
            "  uv run src/run_pipeline.py --from-step transforms\n"
            "  uv run src/run_pipeline.py --skip-steps dashboard\n"
            "  uv run src/run_pipeline.py --dry-run\n"
            "\n"
            f"Available steps: {', '.join(_VALID_STEP_NAMES)}"
        ),
    )
    parser.add_argument(
        "--step",
        metavar="NAME",
        help="Run only the named step (cannot be combined with --from-step)",
    )
    parser.add_argument(
        "--from-step",
        metavar="NAME",
        help="Run from the named step through the end of the pipeline",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the execution plan without running anything",
    )
    parser.add_argument(
        "--db-path",
        default=DEFAULT_DB_PATH,
        help=f"Path to the DuckDB database (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--skip-steps",
        metavar="NAMES",
        default="",
        help="Comma-separated step names to skip (e.g. --skip-steps dashboard)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug-level logging to stderr",
    )

    args = parser.parse_args()
    _configure_logging(verbose=args.verbose)

    # FR-011: reject simultaneous --step + --from-step
    if args.step and args.from_step:
        print(
            "Error: --step and --from-step cannot be used together.\n"
            "Use --step to run one step or --from-step to run from a step onwards.",
            file=sys.stderr,
        )
        return 1

    # Validate step names early (FR-016)
    if args.step:
        _validate_step_name(args.step, "--step")
    if args.from_step:
        _validate_step_name(args.from_step, "--from-step")

    # Parse and validate --skip-steps
    skip_step_names: set[str] = set()
    if args.skip_steps:
        for name in args.skip_steps.split(","):
            name = name.strip()
            if name:
                _validate_step_name(name, "--skip-steps")
                skip_step_names.add(name)

    db_path = args.db_path

    # Ensure metadata table exists (FR-013)
    if not args.dry_run:
        try:
            ensure_pipeline_runs_table(db_path)
        except Exception as exc:
            print(
                f"Warning: Could not ensure pipeline_runs table: {exc}",
                file=sys.stderr,
            )

    project_root = str(Path(__file__).resolve().parent.parent)

    run = run_pipeline(
        PIPELINE_STEPS,
        step_name=args.step or None,
        from_step_name=args.from_step or None,
        skip_step_names=skip_step_names or None,
        dry_run=args.dry_run,
        project_root=project_root,
    )

    # Persist metadata for non-dry-runs (FR-007)
    if not args.dry_run:
        try:
            record_pipeline_run(db_path, run)
        except Exception as exc:
            print(
                f"Warning: Could not record pipeline run to DuckDB: {exc}",
                file=sys.stderr,
            )

    # FR-014: exit 0 on success or dry-run, 1 on failure
    return 0 if run.status == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
