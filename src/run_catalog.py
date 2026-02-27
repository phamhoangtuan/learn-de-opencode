# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "duckdb",
#     "pyyaml",
# ]
# ///
"""Build the data catalog for the DuckDB warehouse.

Introspects all tables and views, merges business metadata from YAML,
and persists catalog_tables and catalog_columns for browsing.

Usage:
    uv run src/run_catalog.py
    uv run src/run_catalog.py --db-path data/warehouse/transactions.duckdb
    uv run src/run_catalog.py --yaml-path src/metadata/catalog.yaml
    uv run src/run_catalog.py --verbose
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

from src.catalog.models import CatalogStatus  # noqa: E402
from src.catalog.runner import (  # noqa: E402
    DEFAULT_DB_PATH,
    DEFAULT_YAML_PATH,
    build_catalog,
)


def _configure_logging(verbose: bool = False) -> None:
    """Configure logging for the catalog builder.

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
    """CLI entrypoint for the data catalog builder.

    Returns:
        Exit code: 0 on success, 1 on failure.
    """
    parser = argparse.ArgumentParser(
        description="Build the data catalog for the DuckDB warehouse.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  uv run src/run_catalog.py\n"
            "  uv run src/run_catalog.py --db-path data/warehouse/transactions.duckdb\n"
            "  uv run src/run_catalog.py --yaml-path src/metadata/catalog.yaml --verbose\n"
        ),
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=DEFAULT_DB_PATH,
        help=f"Path to the DuckDB database (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--yaml-path",
        type=str,
        default=DEFAULT_YAML_PATH,
        help=f"Path to YAML business metadata (default: {DEFAULT_YAML_PATH})",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug-level logging",
    )

    args = parser.parse_args()
    _configure_logging(verbose=args.verbose)

    result = build_catalog(
        db_path=args.db_path,
        yaml_path=args.yaml_path,
    )

    print(result.summary())

    if result.status == CatalogStatus.FAILED:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
