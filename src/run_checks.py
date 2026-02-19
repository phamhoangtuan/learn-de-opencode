# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "duckdb",
# ]
# ///
"""Run data quality checks against the DuckDB warehouse.

Discovers .sql check files from a checks directory, executes each check
against the warehouse, and reports pass/fail/error with severity levels.

Usage:
    uv run src/run_checks.py [OPTIONS]

Examples:
    uv run src/run_checks.py
    uv run src/run_checks.py --db-path data/warehouse/transactions.duckdb \\
        --checks-dir src/checks
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

from src.checker.models import RunStatus  # noqa: E402
from src.checker.runner import (  # noqa: E402
    DEFAULT_CHECKS_DIR,
    DEFAULT_DB_PATH,
    run_checks,
)


def _configure_logging(verbose: bool = False) -> None:
    """Configure logging for the check runner.

    Args:
        verbose: If True, set log level to DEBUG; otherwise INFO.
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


def main() -> int:
    """CLI entrypoint for the data quality check runner.

    Returns:
        Exit code: 0 for success or warn-only, 1 for critical failure.
    """
    parser = argparse.ArgumentParser(
        description="Run data quality checks against the DuckDB warehouse.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  uv run src/run_checks.py\n"
            "  uv run src/run_checks.py --db-path data/warehouse/transactions.duckdb\n"
            "  uv run src/run_checks.py --checks-dir src/checks --verbose\n"
        ),
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=DEFAULT_DB_PATH,
        help=f"Path to the DuckDB database (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--checks-dir",
        type=str,
        default=DEFAULT_CHECKS_DIR,
        help=f"Directory containing .sql check files (default: {DEFAULT_CHECKS_DIR})",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug-level logging",
    )

    args = parser.parse_args()
    _configure_logging(verbose=args.verbose)

    result = run_checks(
        db_path=args.db_path,
        checks_dir=args.checks_dir,
    )

    print(result.summary())

    # FR-011: exit 1 only on critical failure (FAILED status)
    if result.status == RunStatus.FAILED:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
