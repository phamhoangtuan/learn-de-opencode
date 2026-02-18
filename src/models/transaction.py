"""Transaction schema definition and row-building logic."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import numpy as np
import polars as pl
from numpy.random import Generator

from src.models.account import Account
from src.models.merchant import Merchant

# Currency weights per FR-001: USD 70%, EUR 15%, GBP 10%, JPY 5%
CURRENCIES: list[str] = ["USD", "EUR", "GBP", "JPY"]
CURRENCY_WEIGHTS: list[float] = [0.70, 0.15, 0.10, 0.05]

# Transaction type weights per data-model.md: debit 85%, credit 15%
TRANSACTION_TYPES: list[str] = ["debit", "credit"]
TRANSACTION_TYPE_WEIGHTS: list[float] = [0.85, 0.15]

# Status weights per data-model.md: completed 92%, pending 5%, failed 3%
STATUSES: list[str] = ["completed", "pending", "failed"]
STATUS_WEIGHTS: list[float] = [0.92, 0.05, 0.03]


@dataclass
class TransactionSchema:
    """Defines the output schema for the transaction DataFrame.

    This class provides the Polars schema definition and methods
    to build a DataFrame from generated components.
    """

    @staticmethod
    def polars_schema() -> dict[str, pl.DataType]:
        """Return the Polars schema for the transaction output.

        Returns:
            Dictionary mapping column names to Polars data types.
        """
        return {
            "transaction_id": pl.Utf8,
            "timestamp": pl.Datetime("us", time_zone="UTC"),
            "amount": pl.Float64,
            "currency": pl.Utf8,
            "merchant_name": pl.Utf8,
            "category": pl.Utf8,
            "account_id": pl.Utf8,
            "transaction_type": pl.Utf8,
            "status": pl.Utf8,
        }


def generate_transaction_ids(rng: Generator, count: int) -> list[str]:
    """Generate unique transaction IDs using seeded random bytes.

    Uses the seeded RNG to produce deterministic UUIDs (not uuid4)
    for reproducibility per SC-003.

    Args:
        rng: NumPy random generator instance.
        count: Number of IDs to generate.

    Returns:
        List of UUID-formatted strings (8-4-4-4-12 hex format).
    """
    ids: list[str] = []
    for _ in range(count):
        # Generate 16 random bytes and format as UUID
        b = rng.bytes(16)
        hex_str = b.hex()
        uuid_str = (
            f"{hex_str[:8]}-{hex_str[8:12]}-{hex_str[12:16]}-"
            f"{hex_str[16:20]}-{hex_str[20:32]}"
        )
        ids.append(uuid_str)
    return ids


def generate_timestamps(
    rng: Generator,
    count: int,
    start_date: datetime,
    end_date: datetime,
) -> list[datetime]:
    """Generate uniformly distributed timestamps within a date range.

    Per FR-003, timestamps are distributed uniformly within the configured
    date range.

    Args:
        rng: NumPy random generator instance.
        count: Number of timestamps to generate.
        start_date: Start of the range (inclusive).
        end_date: End of the range (inclusive).

    Returns:
        List of timezone-aware UTC datetime objects.
    """
    start_ts = start_date.timestamp()
    end_ts = end_date.timestamp()
    random_ts = rng.uniform(start_ts, end_ts, size=count)
    return [datetime.fromtimestamp(ts, tz=start_date.tzinfo) for ts in random_ts]


def select_weighted(
    rng: Generator,
    options: list[str],
    weights: list[float],
    count: int,
) -> np.ndarray:
    """Select from options using weighted random choice.

    Args:
        rng: NumPy random generator instance.
        options: List of string options to choose from.
        weights: Corresponding probability weights (must sum to 1.0).
        count: Number of selections to make.

    Returns:
        NumPy array of selected option strings.
    """
    return rng.choice(options, size=count, p=weights)


def build_transaction_dataframe(
    *,
    transaction_ids: list[str],
    timestamps: list[datetime],
    amounts: np.ndarray,
    currencies: np.ndarray,
    merchants: list[Merchant],
    accounts: list[Account],
    account_assignments: np.ndarray,
    transaction_types: np.ndarray,
    statuses: np.ndarray,
) -> pl.DataFrame:
    """Build a Polars DataFrame from generated transaction components.

    Args:
        transaction_ids: Unique IDs for each transaction.
        timestamps: Transaction timestamps.
        amounts: Transaction amounts (currency-scaled).
        currencies: Currency codes for each transaction.
        merchants: Selected merchants for each transaction.
        accounts: List of all generated accounts.
        account_assignments: Indices into accounts list for each transaction.
        transaction_types: Transaction type for each transaction.
        statuses: Status for each transaction.

    Returns:
        Polars DataFrame with all 9 transaction fields.
    """
    return pl.DataFrame(
        {
            "transaction_id": transaction_ids,
            "timestamp": timestamps,
            "amount": amounts.tolist(),
            "currency": currencies.tolist(),
            "merchant_name": [m.merchant_name for m in merchants],
            "category": [m.category for m in merchants],
            "account_id": [accounts[i].account_id for i in account_assignments],
            "transaction_type": transaction_types.tolist(),
            "status": statuses.tolist(),
        },
        schema=TransactionSchema.polars_schema(),
    )
