# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "polars",
#     "numpy",
#     "faker",
# ]
# ///
"""Synthetic Financial Transaction Data Generator.

Generates realistic synthetic financial transaction data for pipeline
development and testing. Outputs Parquet (default) or CSV files to
data/raw/ with timestamped filenames.

Usage:
    uv run src/generate_transactions.py [OPTIONS]

Examples:
    uv run src/generate_transactions.py
    uv run src/generate_transactions.py --count 50000 --seed 42
    uv run src/generate_transactions.py --format csv --start-date 2025-01-01 --end-date 2025-12-31
"""

from __future__ import annotations

import argparse
import sys


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for transaction generation.

    Args:
        argv: Command-line arguments. Defaults to sys.argv[1:].

    Returns:
        Parsed argument namespace.
    """
    parser = argparse.ArgumentParser(
        description="Generate synthetic financial transaction data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  uv run src/generate_transactions.py\n"
            "  uv run src/generate_transactions.py --count 50000 --seed 42\n"
            "  uv run src/generate_transactions.py --format csv\n"
            "  uv run src/generate_transactions.py --start-date 2025-01-01 --end-date 2025-12-31\n"
        ),
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10_000,
        help="Number of transactions to generate (default: 10000)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date for transactions in YYYY-MM-DD format (default: 90 days ago)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date for transactions in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--accounts",
        type=int,
        default=100,
        help="Number of unique accounts (default: 100)",
    )
    parser.add_argument(
        "--format",
        type=str,
        choices=["parquet", "csv"],
        default="parquet",
        help="Output format: parquet or csv (default: parquet)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible generation (default: random)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Main entry point for transaction generation.

    Args:
        argv: Command-line arguments. Defaults to sys.argv[1:].

    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    _args = parse_args(argv)
    # TODO: Implementation in Phase 3 (US1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
