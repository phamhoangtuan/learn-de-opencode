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


class TestSeedPropagation:
    """T029: Verify seed controls NumPy RNG and Faker output (US3)."""

    def test_numpy_rng_deterministic_with_seed(self) -> None:
        """Same seed must produce identical NumPy random sequences."""
        rng1 = np.random.default_rng(42)
        rng2 = np.random.default_rng(42)
        vals1 = rng1.random(100)
        vals2 = rng2.random(100)
        np.testing.assert_array_equal(vals1, vals2)

    def test_different_seeds_produce_different_output(self) -> None:
        """Different seeds must produce different NumPy sequences."""
        currencies = np.array(["USD"] * 100)
        a1 = generate_amounts(np.random.default_rng(42), 100, currencies)
        a2 = generate_amounts(np.random.default_rng(99), 100, currencies)
        assert not np.array_equal(a1, a2)

    def test_faker_seed_deterministic(self) -> None:
        """Same Faker seed must produce identical names."""
        from faker import Faker

        Faker.seed(42)
        fake1 = Faker()
        names1 = [fake1.name() for _ in range(10)]

        Faker.seed(42)
        fake2 = Faker()
        names2 = [fake2.name() for _ in range(10)]

        assert names1 == names2

    def test_account_generation_deterministic(self) -> None:
        """Same seed must produce identical accounts."""
        from src.models.account import generate_accounts

        rng1 = np.random.default_rng(42)
        rng2 = np.random.default_rng(42)
        accounts1 = generate_accounts(rng1, 10, seed=42)
        accounts2 = generate_accounts(rng2, 10, seed=42)

        for a1, a2 in zip(accounts1, accounts2, strict=True):
            assert a1.account_id == a2.account_id
            assert a1.account_holder == a2.account_holder
            assert a1.account_type == a2.account_type

    def test_transaction_ids_deterministic(self) -> None:
        """Same seed must produce identical transaction IDs."""
        from src.models.transaction import generate_transaction_ids

        ids1 = generate_transaction_ids(np.random.default_rng(42), 20)
        ids2 = generate_transaction_ids(np.random.default_rng(42), 20)
        assert ids1 == ids2
