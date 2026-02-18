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
import time
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import polars as pl

from src.lib.distributions import generate_amounts
from src.lib.logging_config import GenerationMetadata, setup_logging
from src.lib.validators import ValidatedParams, validate_params
from src.models.account import generate_accounts
from src.models.merchant import load_merchant_catalog, select_merchants
from src.models.transaction import (
    CURRENCIES,
    CURRENCY_WEIGHTS,
    STATUS_WEIGHTS,
    STATUSES,
    TRANSACTION_TYPE_WEIGHTS,
    TRANSACTION_TYPES,
    TransactionSchema,
    build_transaction_dataframe,
    generate_timestamps,
    generate_transaction_ids,
    select_weighted,
)

DEFAULT_OUTPUT_DIR = "data/raw"


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
            "  uv run src/generate_transactions.py --start-date 2025-01-01"
            " --end-date 2025-12-31\n"
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
    parser.add_argument(
        "--output-dir",
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    return parser.parse_args(argv)


def generate_transactions(params: ValidatedParams, output_dir: Path) -> tuple[pl.DataFrame, Path]:
    """Generate synthetic transactions and write to file.

    Args:
        params: Validated generation parameters.
        output_dir: Directory to write output file.

    Returns:
        Tuple of (generated DataFrame, output file path).
    """
    logger = setup_logging()
    rng = np.random.default_rng(params.seed)

    # Handle zero records edge case
    if params.count == 0:
        logger.info("Generating 0 records — producing empty file with schema")
        df = pl.DataFrame(schema=TransactionSchema.polars_schema())
        output_path = _write_output(df, params.format, output_dir)
        return df, output_path

    # Log warning if accounts were capped
    if params.accounts < params.count:
        pass  # accounts already capped in validator
    else:
        logger.info(
            "Generating %d transactions across %d accounts",
            params.count,
            params.accounts,
        )

    # Generate accounts
    accounts = generate_accounts(rng, params.accounts, seed=params.seed)

    # Load merchant catalog and select merchants
    catalog = load_merchant_catalog()
    merchants = select_merchants(rng, catalog, params.count)

    # Generate weighted selections
    currencies = select_weighted(rng, CURRENCIES, CURRENCY_WEIGHTS, params.count)
    transaction_types = select_weighted(
        rng, TRANSACTION_TYPES, TRANSACTION_TYPE_WEIGHTS, params.count,
    )
    statuses = select_weighted(rng, STATUSES, STATUS_WEIGHTS, params.count)

    # Generate amounts (currency-scaled)
    amounts = generate_amounts(rng, params.count, currencies)

    # Generate timestamps (uniform within date range)
    start_dt = datetime(
        params.start_date.year, params.start_date.month, params.start_date.day,
        tzinfo=UTC,
    )
    end_dt = datetime(
        params.end_date.year, params.end_date.month, params.end_date.day,
        hour=23, minute=59, second=59,
        tzinfo=UTC,
    )
    timestamps = generate_timestamps(rng, params.count, start_dt, end_dt)

    # Generate transaction IDs
    transaction_ids = generate_transaction_ids(rng, params.count)

    # Assign transactions to accounts (uniform distribution)
    account_assignments = rng.integers(0, len(accounts), size=params.count)

    # Build DataFrame
    df = build_transaction_dataframe(
        transaction_ids=transaction_ids,
        timestamps=timestamps,
        amounts=amounts,
        currencies=currencies,
        merchants=merchants,
        accounts=accounts,
        account_assignments=account_assignments,
        transaction_types=transaction_types,
        statuses=statuses,
    )

    # Write output
    output_path = _write_output(df, params.format, output_dir)

    return df, output_path


def _write_output(df: pl.DataFrame, fmt: str, output_dir: Path) -> Path:
    """Write DataFrame to file in the specified format.

    Args:
        df: Polars DataFrame to write.
        fmt: Output format ('parquet' or 'csv').
        output_dir: Directory to write the file.

    Returns:
        Path to the written file.
    """
    # Auto-create output directory if missing
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"transactions_{timestamp_str}.{fmt}"
    output_path = output_dir / filename

    if fmt == "parquet":
        df.write_parquet(output_path)
    else:
        df.write_csv(output_path)

    return output_path


def main(argv: list[str] | None = None) -> int:
    """Main entry point for transaction generation.

    Args:
        argv: Command-line arguments. Defaults to sys.argv[1:].

    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    logger = setup_logging()
    args = parse_args(argv)

    # Validate parameters
    result = validate_params(
        count=args.count,
        start_date=args.start_date,
        end_date=args.end_date,
        accounts=args.accounts,
        output_format=args.format,
        seed=args.seed,
    )

    if isinstance(result, list):
        for error in result:
            logger.error("Validation error [%s]: %s", error.field, error.message)
        return 1

    params = result

    # Log if accounts were capped
    if args.accounts > params.accounts and params.count > 0:
        logger.warning(
            "Account count capped from %d to %d (cannot exceed transaction count)",
            args.accounts,
            params.accounts,
        )

    # Generate transactions
    start_time = time.monotonic()
    output_dir = Path(args.output_dir)

    try:
        _df, output_path = generate_transactions(params, output_dir)
    except Exception:
        logger.exception("Generation failed")
        return 1

    duration = time.monotonic() - start_time

    # Output metadata as JSON to stdout per FR-009
    metadata = GenerationMetadata(
        records_generated=params.count,
        seed=params.seed,
        start_date=str(params.start_date),
        end_date=str(params.end_date),
        accounts=params.accounts,
        format=params.format,
        output_path=str(output_path),
        duration_seconds=round(duration, 2),
    )
    print(metadata.to_json())

    return 0


if __name__ == "__main__":
    sys.exit(main())
