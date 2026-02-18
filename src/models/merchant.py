"""Merchant and category reference data loading and weighted selection."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from numpy.random import Generator

# Path to the merchant reference data file
_MERCHANTS_FILE = Path(__file__).parent.parent / "data" / "merchants.json"


@dataclass
class Merchant:
    """A merchant entity from reference data.

    Attributes:
        merchant_name: Business name.
        category: Spending category this merchant belongs to.
        mcc: Merchant Category Code (4-digit string).
    """

    merchant_name: str
    category: str
    mcc: str


@dataclass
class MerchantCatalog:
    """Loaded merchant catalog with weighted category selection.

    Attributes:
        categories: List of category names in weight order.
        category_weights: Corresponding selection weights (sum to 1.0).
        merchants_by_category: Mapping of category name to list of merchants.
    """

    categories: list[str]
    category_weights: np.ndarray
    merchants_by_category: dict[str, list[Merchant]]

    @property
    def total_merchants(self) -> int:
        """Return total number of merchants across all categories."""
        return sum(len(m) for m in self.merchants_by_category.values())


def load_merchant_catalog(path: Path | None = None) -> MerchantCatalog:
    """Load merchant reference data from JSON file.

    Args:
        path: Path to merchants.json. Defaults to src/data/merchants.json.

    Returns:
        MerchantCatalog with categories, weights, and merchants.

    Raises:
        FileNotFoundError: If the merchants.json file does not exist.
        ValueError: If the JSON structure is invalid.
    """
    file_path = path or _MERCHANTS_FILE

    with open(file_path) as f:
        data = json.load(f)

    categories: list[str] = []
    weights: list[float] = []
    merchants_by_category: dict[str, list[Merchant]] = {}

    for cat_data in data["categories"]:
        name = cat_data["name"]
        weight = cat_data["weight"]
        categories.append(name)
        weights.append(weight)

        merchants_by_category[name] = [
            Merchant(
                merchant_name=m["merchant_name"],
                category=name,
                mcc=m["mcc"],
            )
            for m in cat_data["merchants"]
        ]

    return MerchantCatalog(
        categories=categories,
        category_weights=np.array(weights),
        merchants_by_category=merchants_by_category,
    )


def select_merchants(
    rng: Generator,
    catalog: MerchantCatalog,
    count: int,
) -> list[Merchant]:
    """Select merchants using weighted category selection, then uniform within category.

    First selects a category based on category weights, then picks a merchant
    uniformly at random within that category. Per data-model.md relationship rules.

    Args:
        rng: NumPy random generator instance (seeded for reproducibility).
        catalog: Loaded merchant catalog with categories and weights.
        count: Number of merchants to select.

    Returns:
        List of selected Merchant instances (with repetition).
    """
    # Select categories based on weights
    selected_categories = rng.choice(
        catalog.categories,
        size=count,
        p=catalog.category_weights,
    )

    # For each selected category, pick a random merchant within it
    merchants: list[Merchant] = []
    for cat_name in selected_categories:
        cat_merchants = catalog.merchants_by_category[cat_name]
        idx = rng.integers(0, len(cat_merchants))
        merchants.append(cat_merchants[idx])

    return merchants
