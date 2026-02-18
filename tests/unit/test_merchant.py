"""Unit tests for merchant catalog loading and weighted selection."""

from __future__ import annotations

import numpy as np

from src.models.merchant import (
    load_merchant_catalog,
    select_merchants,
)


class TestLoadMerchantCatalog:
    """Tests for loading the merchant reference data."""

    def test_loads_18_categories(self) -> None:
        """Catalog must contain exactly 18 spending categories."""
        catalog = load_merchant_catalog()
        assert len(catalog.categories) == 18

    def test_total_merchants_in_range(self) -> None:
        """Catalog must contain 50-100 merchants per FR-012."""
        catalog = load_merchant_catalog()
        assert 50 <= catalog.total_merchants <= 100

    def test_category_weights_sum_to_one(self) -> None:
        """Category weights must sum to 1.0."""
        catalog = load_merchant_catalog()
        assert abs(catalog.category_weights.sum() - 1.0) < 1e-10

    def test_all_categories_have_merchants(self) -> None:
        """Every category must have at least one merchant."""
        catalog = load_merchant_catalog()
        for cat in catalog.categories:
            assert len(catalog.merchants_by_category[cat]) >= 1, (
                f"Category '{cat}' has no merchants"
            )

    def test_merchant_has_required_fields(self) -> None:
        """Each merchant must have name, category, and MCC."""
        catalog = load_merchant_catalog()
        for cat, merchants in catalog.merchants_by_category.items():
            for m in merchants:
                assert m.merchant_name, f"Merchant in {cat} has empty name"
                assert m.category == cat, f"Merchant {m.merchant_name} category mismatch"
                assert len(m.mcc) == 4, f"Merchant {m.merchant_name} MCC '{m.mcc}' is not 4 digits"

    def test_merchant_names_unique(self) -> None:
        """All merchant names must be unique across the catalog."""
        catalog = load_merchant_catalog()
        all_names: list[str] = []
        for merchants in catalog.merchants_by_category.values():
            all_names.extend(m.merchant_name for m in merchants)
        assert len(all_names) == len(set(all_names)), "Duplicate merchant names found"


class TestSelectMerchants:
    """Tests for the weighted merchant selection logic."""

    def test_returns_correct_count(self) -> None:
        """Selection returns exactly the requested number of merchants."""
        rng = np.random.default_rng(42)
        catalog = load_merchant_catalog()
        selected = select_merchants(rng, catalog, 500)
        assert len(selected) == 500

    def test_all_categories_reachable(self) -> None:
        """With enough selections, all categories should appear."""
        rng = np.random.default_rng(42)
        catalog = load_merchant_catalog()
        selected = select_merchants(rng, catalog, 10_000)
        selected_categories = {m.category for m in selected}
        assert selected_categories == set(catalog.categories)

    def test_category_distribution_roughly_matches_weights(self) -> None:
        """Selected category frequencies should approximate the weights."""
        rng = np.random.default_rng(42)
        catalog = load_merchant_catalog()
        selected = select_merchants(rng, catalog, 50_000)

        # Count per category
        counts: dict[str, int] = {}
        for m in selected:
            counts[m.category] = counts.get(m.category, 0) + 1

        # Check top category (Groceries at 18%) is within tolerance
        groceries_pct = counts.get("Groceries", 0) / 50_000
        assert 0.15 < groceries_pct < 0.21, f"Groceries at {groceries_pct:.3f}, expected ~0.18"

    def test_merchant_category_consistency(self) -> None:
        """Selected merchants must have consistent category mappings."""
        rng = np.random.default_rng(42)
        catalog = load_merchant_catalog()
        selected = select_merchants(rng, catalog, 1000)

        # Build expected mapping
        expected: dict[str, str] = {}
        for cat, merchants in catalog.merchants_by_category.items():
            for m in merchants:
                expected[m.merchant_name] = cat

        for m in selected:
            assert m.category == expected[m.merchant_name]

    def test_reproducibility(self) -> None:
        """Same seed produces identical selections."""
        catalog = load_merchant_catalog()
        s1 = select_merchants(np.random.default_rng(42), catalog, 100)
        s2 = select_merchants(np.random.default_rng(42), catalog, 100)
        assert [m.merchant_name for m in s1] == [m.merchant_name for m in s2]
