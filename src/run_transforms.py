# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "duckdb",
# ]
# ///
"""Run SQL transformations against the DuckDB warehouse.

Discovers .sql files from a transforms directory, resolves dependencies,
and executes them in topological order against the warehouse database.

Usage:
    uv run src/run_transforms.py [OPTIONS]

Examples:
    uv run src/run_transforms.py
    uv run src/run_transforms.py --db-path data/warehouse/transactions.duckdb \\
        --transforms-dir src/transforms
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

from src.transformer.models import TransformStatus  # noqa: E402
from src.transformer.runner import (  # noqa: E402
    DEFAULT_DB_PATH,
    DEFAULT_TRANSFORMS_DIR,
    run_transforms,
)


def _configure_logging(verbose: bool = False) -> None:
    """Configure logging for the transform runner.

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
    """CLI entrypoint for the SQL transform runner.

    Returns:
        Exit code: 0 for success, 1 for failure.
    """
    parser = argparse.ArgumentParser(
        description="Run SQL transformations against the DuckDB warehouse.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  uv run src/run_transforms.py\n"
            "  uv run src/run_transforms.py --db-path data/warehouse/transactions.duckdb\n"
            "  uv run src/run_transforms.py --transforms-dir src/transforms --verbose\n"
        ),
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=DEFAULT_DB_PATH,
        help=f"Path to the DuckDB database (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--transforms-dir",
        type=str,
        default=DEFAULT_TRANSFORMS_DIR,
        help=f"Directory containing .sql transform files (default: {DEFAULT_TRANSFORMS_DIR})",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug-level logging",
    )

    args = parser.parse_args()
    _configure_logging(verbose=args.verbose)

    result = run_transforms(
        db_path=args.db_path,
        transforms_dir=args.transforms_dir,
    )

    print(result.summary())

    if result.status == TransformStatus.FAILED:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
