"""Transaction amount distribution logic using log-normal distribution."""

from __future__ import annotations

import numpy as np
from numpy.random import Generator

# Currency scaling factors relative to USD base amount.
# Applied after log-normal generation to produce realistic amounts per currency.
CURRENCY_SCALES: dict[str, float] = {
    "USD": 1.0,
    "EUR": 0.9,
    "GBP": 0.8,
    "JPY": 110.0,
}

# Log-normal distribution parameters (USD base).
# mu=3.0, sigma=1.5 produces amounts centered around $20-50 with
# a long tail up to $5,000+.
AMOUNT_MU: float = 3.0
AMOUNT_SIGMA: float = 1.5


def generate_amounts(
    rng: Generator,
    count: int,
    currencies: np.ndarray,
) -> np.ndarray:
    """Generate transaction amounts using log-normal distribution.

    Amounts are generated from a log-normal distribution with parameters
    mu=3.0, sigma=1.5 (USD base), then scaled by currency-specific factors.
    JPY amounts are rounded to integers (no decimal subdivision).

    Args:
        rng: NumPy random generator instance (seeded for reproducibility).
        count: Number of amounts to generate.
        currencies: Array of currency codes (same length as count).

    Returns:
        Array of positive float amounts, currency-scaled and rounded to
        2 decimal places (integers for JPY).
    """
    # Generate base USD amounts from log-normal distribution
    raw_amounts = rng.lognormal(mean=AMOUNT_MU, sigma=AMOUNT_SIGMA, size=count)

    # Apply currency scaling
    amounts = np.empty(count, dtype=np.float64)
    for currency, scale in CURRENCY_SCALES.items():
        mask = currencies == currency
        amounts[mask] = raw_amounts[mask] * scale

    # Round JPY to integers (no decimal subdivision)
    jpy_mask = currencies == "JPY"
    amounts[jpy_mask] = np.round(amounts[jpy_mask], 0)

    # Round other currencies to 2 decimal places
    non_jpy_mask = ~jpy_mask
    amounts[non_jpy_mask] = np.round(amounts[non_jpy_mask], 2)

    return amounts
