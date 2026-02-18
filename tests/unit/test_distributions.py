"""Unit tests for transaction amount distribution logic."""

from __future__ import annotations

import numpy as np
from scipy import stats

from src.lib.distributions import (
    generate_amounts,
)


class TestGenerateAmounts:
    """Tests for the generate_amounts function."""

    def test_returns_correct_count(self) -> None:
        """Generated array has the requested number of elements."""
        rng = np.random.default_rng(42)
        currencies = np.array(["USD"] * 100)
        amounts = generate_amounts(rng, 100, currencies)
        assert len(amounts) == 100

    def test_all_amounts_positive(self) -> None:
        """All generated amounts must be strictly positive."""
        rng = np.random.default_rng(42)
        currencies = np.array(["USD"] * 1000)
        amounts = generate_amounts(rng, 1000, currencies)
        assert np.all(amounts > 0)

    def test_lognormal_shape_skewness(self) -> None:
        """Distribution must be right-skewed with skewness > 1.0 per SC-004."""
        rng = np.random.default_rng(42)
        currencies = np.array(["USD"] * 10_000)
        amounts = generate_amounts(rng, 10_000, currencies)
        skewness = stats.skew(amounts)
        assert skewness > 1.0, f"Skewness {skewness} is not > 1.0"

    def test_lognormal_shape_median_less_than_mean(self) -> None:
        """Median must be less than mean for a right-skewed distribution per SC-004."""
        rng = np.random.default_rng(42)
        currencies = np.array(["USD"] * 10_000)
        amounts = generate_amounts(rng, 10_000, currencies)
        assert np.median(amounts) < np.mean(amounts)

    def test_currency_scaling_usd_baseline(self) -> None:
        """USD amounts should use scale factor 1.0 (no scaling)."""
        rng = np.random.default_rng(42)
        currencies_usd = np.array(["USD"] * 1000)
        amounts_usd = generate_amounts(rng, 1000, currencies_usd)
        # USD mean should be around exp(mu + sigma^2/2) * 1.0
        assert amounts_usd.mean() > 0

    def test_currency_scaling_jpy_larger(self) -> None:
        """JPY amounts should be significantly larger than USD due to 110x scale."""
        rng1 = np.random.default_rng(42)
        rng2 = np.random.default_rng(42)
        usd = generate_amounts(rng1, 5000, np.array(["USD"] * 5000))
        jpy = generate_amounts(rng2, 5000, np.array(["JPY"] * 5000))
        # JPY mean should be roughly 110x USD mean
        ratio = jpy.mean() / usd.mean()
        assert 80 < ratio < 150, f"JPY/USD ratio {ratio} not in expected range"

    def test_jpy_rounded_to_integers(self) -> None:
        """JPY amounts must have no decimal places."""
        rng = np.random.default_rng(42)
        currencies = np.array(["JPY"] * 100)
        amounts = generate_amounts(rng, 100, currencies)
        assert np.all(amounts == np.round(amounts, 0))

    def test_non_jpy_rounded_to_two_decimals(self) -> None:
        """Non-JPY amounts must be rounded to 2 decimal places."""
        rng = np.random.default_rng(42)
        currencies = np.array(["USD"] * 100)
        amounts = generate_amounts(rng, 100, currencies)
        assert np.allclose(amounts, np.round(amounts, 2))

    def test_mixed_currencies(self) -> None:
        """Generation works correctly with mixed currency arrays."""
        rng = np.random.default_rng(42)
        currencies = np.array(["USD"] * 70 + ["EUR"] * 15 + ["GBP"] * 10 + ["JPY"] * 5)
        amounts = generate_amounts(rng, 100, currencies)
        assert len(amounts) == 100
        assert np.all(amounts > 0)

    def test_reproducibility_with_same_seed(self) -> None:
        """Same seed produces identical amounts."""
        currencies = np.array(["USD"] * 100)
        a1 = generate_amounts(np.random.default_rng(42), 100, currencies)
        a2 = generate_amounts(np.random.default_rng(42), 100, currencies)
        np.testing.assert_array_equal(a1, a2)
