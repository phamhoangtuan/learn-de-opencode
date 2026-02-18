"""Unit tests for account generation logic."""

from __future__ import annotations

import numpy as np

from src.models.account import (
    ACCOUNT_TYPES,
    generate_accounts,
)


class TestGenerateAccounts:
    """Tests for the generate_accounts function."""

    def test_returns_correct_count(self) -> None:
        """Generated list has the requested number of accounts."""
        rng = np.random.default_rng(42)
        accounts = generate_accounts(rng, 50, seed=42)
        assert len(accounts) == 50

    def test_account_id_format(self) -> None:
        """All account IDs must follow ACC-XXXXX format."""
        rng = np.random.default_rng(42)
        accounts = generate_accounts(rng, 100, seed=42)
        for acc in accounts:
            assert acc.account_id.startswith("ACC-"), (
                f"ID {acc.account_id} doesn't start with ACC-"
            )
            suffix = acc.account_id[4:]
            assert len(suffix) == 5, f"ID suffix '{suffix}' is not 5 chars"
            assert suffix.isdigit(), f"ID suffix '{suffix}' is not numeric"

    def test_account_ids_unique(self) -> None:
        """All account IDs must be unique."""
        rng = np.random.default_rng(42)
        accounts = generate_accounts(rng, 100, seed=42)
        ids = [a.account_id for a in accounts]
        assert len(ids) == len(set(ids))

    def test_account_holders_not_empty(self) -> None:
        """All account holders must have non-empty names."""
        rng = np.random.default_rng(42)
        accounts = generate_accounts(rng, 50, seed=42)
        for acc in accounts:
            assert acc.account_holder.strip(), f"Account {acc.account_id} has empty holder"

    def test_account_types_valid(self) -> None:
        """All account types must be one of checking, savings, credit."""
        rng = np.random.default_rng(42)
        accounts = generate_accounts(rng, 100, seed=42)
        valid_types = set(ACCOUNT_TYPES)
        for acc in accounts:
            assert acc.account_type in valid_types, f"Invalid type: {acc.account_type}"

    def test_account_type_distribution(self) -> None:
        """Account types should roughly match weighted distribution."""
        rng = np.random.default_rng(42)
        accounts = generate_accounts(rng, 10_000, seed=42)

        counts: dict[str, int] = {}
        for acc in accounts:
            counts[acc.account_type] = counts.get(acc.account_type, 0) + 1

        # Checking at 50% — should be within 45-55%
        checking_pct = counts.get("checking", 0) / 10_000
        assert 0.45 < checking_pct < 0.55, f"Checking at {checking_pct:.3f}, expected ~0.50"

    def test_reproducibility(self) -> None:
        """Same seed and RNG produce identical accounts."""
        a1 = generate_accounts(np.random.default_rng(42), 50, seed=42)
        a2 = generate_accounts(np.random.default_rng(42), 50, seed=42)
        assert [a.account_id for a in a1] == [a.account_id for a in a2]
        assert [a.account_holder for a in a1] == [a.account_holder for a in a2]
        assert [a.account_type for a in a1] == [a.account_type for a in a2]
