# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "duckdb",
#     "polars",
# ]
# ///
"""Run the dim_accounts SCD Type 2 dimension build step.

Reads from stg_transactions and builds/updates the dim_accounts table.
Emits DimBuildResult stats to stdout as JSON.

Usage:
    uv run src/run_dim_build.py
    uv run src/run_dim_build.py --db-path data/warehouse/transactions.duckdb
    uv run src/run_dim_build.py --run-date 2026-02-26
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import date
from pathlib import Path

# Ensure project root is in sys.path for uv run script invocation
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.dimensions.dim_accounts import (  # noqa: E402
    DimBuildError,
    build_dim_accounts,
    create_dim_tables,
)
from src.ingestion.loader import connect  # noqa: E402

DEFAULT_DB_PATH = "data/warehouse/transactions.duckdb"


def _configure_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stderr,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the dim_accounts SCD2 dimension build."
    )
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument(
        "--run-id",
        default=None,
        help="Pipeline run ID (default: auto-generated UUID)",
    )
    parser.add_argument(
        "--run-date",
        default=None,
        help="Run date YYYY-MM-DD (default: today)",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    _configure_logging(verbose=args.verbose)

    run_id = args.run_id or str(uuid.uuid4())
    run_date = date.fromisoformat(args.run_date) if args.run_date else date.today()

    conn = connect(args.db_path)
    try:
        create_dim_tables(conn)
        result = build_dim_accounts(conn, run_id=run_id, run_date=run_date)
        print(json.dumps({
            "accounts_processed": result.accounts_processed,
            "new_versions": result.new_versions,
            "unchanged": result.unchanged,
            "elapsed_seconds": round(result.elapsed_seconds, 3),
        }))
        return 0
    except DimBuildError as exc:
        print(f"dim_build failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
