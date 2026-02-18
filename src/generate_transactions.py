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

# Ensure project root is in sys.path for uv run script invocation
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import numpy as np  # noqa: E402
import polars as pl  # noqa: E402

from src.lib.distributions import generate_amounts  # noqa: E402
from src.lib.logging_config import GenerationMetadata, setup_logging  # noqa: E402
from src.lib.validators import ValidatedParams, validate_params  # noqa: E402
from src.models.account import generate_accounts  # noqa: E402
from src.models.merchant import load_merchant_catalog, select_merchants  # noqa: E402
from src.models.transaction import (  # noqa: E402
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
CHUNK_THRESHOLD = 1_000_000
CHUNK_SIZE = 500_000


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


def _generate_chunk(
    rng: np.random.Generator,
    chunk_size: int,
    accounts: list,
    catalog: object,
    start_dt: datetime,
    end_dt: datetime,
) -> pl.DataFrame:
    """Generate a single chunk of transactions.

    Args:
        rng: NumPy random generator (mutated in-place for continuity).
        chunk_size: Number of records to generate in this chunk.
        accounts: Pre-generated account list.
        catalog: Loaded merchant catalog.
        start_dt: Start datetime for timestamp range.
        end_dt: End datetime for timestamp range.

    Returns:
        Polars DataFrame with chunk_size transaction records.
    """
    merchants = select_merchants(rng, catalog, chunk_size)
    currencies = select_weighted(rng, CURRENCIES, CURRENCY_WEIGHTS, chunk_size)
    transaction_types = select_weighted(
        rng, TRANSACTION_TYPES, TRANSACTION_TYPE_WEIGHTS, chunk_size,
    )
    statuses = select_weighted(rng, STATUSES, STATUS_WEIGHTS, chunk_size)
    amounts = generate_amounts(rng, chunk_size, currencies)
    timestamps = generate_timestamps(rng, chunk_size, start_dt, end_dt)
    transaction_ids = generate_transaction_ids(rng, chunk_size)
    account_assignments = rng.integers(0, len(accounts), size=chunk_size)

    return build_transaction_dataframe(
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


def generate_transactions(
    params: ValidatedParams, output_dir: Path,
) -> tuple[pl.DataFrame, Path]:
    """Generate synthetic transactions and write to file.

    For datasets exceeding CHUNK_THRESHOLD records, uses chunked generation
    to avoid memory exhaustion per spec edge case requirements.

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

    # Log generation info
    if params.accounts < params.count:
        pass  # accounts already capped in validator
    else:
        logger.info(
            "Generating %d transactions across %d accounts",
            params.count,
            params.accounts,
        )

    # Prepare shared state
    accounts = generate_accounts(rng, params.accounts, seed=params.seed)
    catalog = load_merchant_catalog()
    start_dt = datetime(
        params.start_date.year, params.start_date.month, params.start_date.day,
        tzinfo=UTC,
    )
    end_dt = datetime(
        params.end_date.year, params.end_date.month, params.end_date.day,
        hour=23, minute=59, second=59,
        tzinfo=UTC,
    )

    # Chunked generation for large datasets
    if params.count > CHUNK_THRESHOLD:
        logger.info(
            "Large dataset (%d records) — using chunked generation "
            "with chunk size %d",
            params.count,
            CHUNK_SIZE,
        )
        output_path = _write_chunked(
            rng, params, accounts, catalog, start_dt, end_dt, output_dir,
        )
        # Return empty DataFrame for large datasets (data is on disk)
        df = pl.DataFrame(schema=TransactionSchema.polars_schema())
        return df, output_path

    # Standard in-memory generation for smaller datasets
    df = _generate_chunk(
        rng, params.count, accounts, catalog, start_dt, end_dt,
    )
    output_path = _write_output(df, params.format, output_dir)
    return df, output_path


def _write_chunked(
    rng: np.random.Generator,
    params: ValidatedParams,
    accounts: list,
    catalog: object,
    start_dt: datetime,
    end_dt: datetime,
    output_dir: Path,
) -> Path:
    """Write large datasets in chunks to avoid memory exhaustion.

    Generates data in CHUNK_SIZE batches and appends to the output file
    incrementally. For Parquet, chunks are collected and written as a
    single file. For CSV, chunks are appended directly.

    Args:
        rng: NumPy random generator.
        params: Validated generation parameters.
        accounts: Pre-generated account list.
        catalog: Loaded merchant catalog.
        start_dt: Start datetime for timestamp range.
        end_dt: End datetime for timestamp range.
        output_dir: Directory to write the file.

    Returns:
        Path to the written file.
    """
    logger = setup_logging()
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"transactions_{timestamp_str}.{params.format}"
    output_path = output_dir / filename

    remaining = params.count
    chunk_num = 0
    chunks: list[pl.DataFrame] = []

    while remaining > 0:
        chunk_size = min(CHUNK_SIZE, remaining)
        chunk_num += 1
        logger.info("Generating chunk %d (%d records)", chunk_num, chunk_size)

        chunk_df = _generate_chunk(
            rng, chunk_size, accounts, catalog, start_dt, end_dt,
        )

        if params.format == "csv":
            # CSV: append directly to file (memory-efficient)
            if chunk_num == 1:
                chunk_df.write_csv(output_path)
            else:
                with open(output_path, "a") as f:
                    f.write(
                        chunk_df.write_csv(file=None, include_header=False),
                    )
        else:
            # Parquet: collect chunks, write once at end
            chunks.append(chunk_df)

        remaining -= chunk_size

    # Write collected Parquet chunks
    if params.format == "parquet" and chunks:
        combined = pl.concat(chunks)
        combined.write_parquet(output_path)

    return output_path


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
