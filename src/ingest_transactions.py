# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "polars",
#     "duckdb",
#     "pyarrow",
# ]
# ///
"""Ingest raw Parquet transaction files into a local DuckDB warehouse.

Reads Parquet files from a source directory (default: data/raw/),
validates, deduplicates, and loads them into a DuckDB database
(default: data/warehouse/transactions.duckdb).

Usage:
    uv run src/ingest_transactions.py [OPTIONS]

Examples:
    uv run src/ingest_transactions.py
    uv run src/ingest_transactions.py --source-dir data/raw \\
        --db-path data/warehouse/transactions.duckdb
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure project root is in sys.path for uv run script invocation
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.ingestion.pipeline import (  # noqa: E402
    DEFAULT_SOURCE_DIR,
    run_pipeline,
)
from src.lib.logging_config import setup_logging  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for the ingestion pipeline.

    Args:
        argv: Command-line arguments. Defaults to sys.argv[1:].

    Returns:
        Parsed argument namespace.
    """
    parser = argparse.ArgumentParser(
        description="Ingest raw Parquet transaction files into DuckDB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  uv run src/ingest_transactions.py\n"
            "  uv run src/ingest_transactions.py --source-dir data/raw\n"
            "  uv run src/ingest_transactions.py --db-path data/warehouse/txns.duckdb\n"
        ),
    )
    parser.add_argument(
        "--source-dir",
        type=str,
        default=DEFAULT_SOURCE_DIR,
        help=f"Source directory for Parquet files (default: {DEFAULT_SOURCE_DIR})",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="data/warehouse/transactions.duckdb",
        help="Path to DuckDB database file (default: data/warehouse/transactions.duckdb)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Main entry point for the ingestion pipeline.

    Args:
        argv: Command-line arguments. Defaults to sys.argv[1:].

    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    logger = setup_logging()
    args = parse_args(argv)

    logger.info(
        "Starting ingestion: source=%s, db=%s",
        args.source_dir,
        args.db_path,
    )

    try:
        result = run_pipeline(
            source_dir=args.source_dir,
            db_path=args.db_path,
        )
    except Exception:
        logger.exception("Pipeline execution failed")
        return 1

    # Print summary to stdout per FR-016
    print(result.summary())

    return 0


if __name__ == "__main__":
    sys.exit(main())
