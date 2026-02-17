"""T021: Unit test — transaction_generator realistic distributions.

Per Constitution v2.0.0 Principle III: Tests written FIRST, must FAIL before implementation.
Per spec.md FR-001: Realistic data patterns with varying amounts, currencies, categories.
"""

from collections import Counter

import pytest


class TestTransactionGenerator:
    """Test transaction generation with realistic distribution patterns."""

    def test_generate_transaction_returns_transaction(self) -> None:
        """generate_transaction must return a Transaction instance."""
        from generator.src.generators.transaction_generator import generate_transaction
        from generator.src.models.account import create_account_pool
        from generator.src.models.transaction import Transaction

        pool = create_account_pool(size=10)
        tx = generate_transaction(pool)
        assert isinstance(tx, Transaction)

    def test_generate_transaction_uses_pool_account(self) -> None:
        """Generated transaction must use an account_id from the pool."""
        from generator.src.generators.transaction_generator import generate_transaction
        from generator.src.models.account import create_account_pool

        pool = create_account_pool(size=10)
        pool_ids = {a.account_id for a in pool}
        tx = generate_transaction(pool)
        assert tx.account_id in pool_ids

    def test_amount_distribution_is_log_normal(self) -> None:
        """Amounts should follow a log-normal-like distribution: mostly small, some large.

        Per spec.md FR-001: varying amounts from $0.50 to $50,000.
        """
        from generator.src.generators.transaction_generator import generate_transaction
        from generator.src.models.account import create_account_pool

        pool = create_account_pool(size=50)
        amounts = [abs(generate_transaction(pool).amount) for _ in range(1000)]

        # Most transactions should be under $1000
        under_1000 = sum(1 for a in amounts if a < 1000)
        assert under_1000 > 500, (
            f"Expected majority under $1000, got {under_1000}/1000"
        )

        # Some should be over $1000
        over_1000 = sum(1 for a in amounts if a >= 1000)
        assert over_1000 > 0, "Expected some transactions over $1000"

    def test_currency_distribution_matches_config(self) -> None:
        """Currency distribution should roughly match config weights.

        Config: USD 60%, EUR 20%, GBP 12%, JPY 8%
        """
        from generator.src.generators.transaction_generator import generate_transaction
        from generator.src.models.account import create_account_pool

        pool = create_account_pool(size=50)
        currencies = Counter(generate_transaction(pool).currency for _ in range(1000))

        # USD should be the most common (60% weight)
        assert currencies["USD"] > 400, f"USD count {currencies['USD']} too low (expected ~600)"
        # All 4 currencies should appear
        assert set(currencies.keys()) == {"USD", "EUR", "GBP", "JPY"}

    def test_merchant_category_distribution_covers_all(self) -> None:
        """All 7 merchant categories should appear in generated transactions."""
        from generator.src.generators.transaction_generator import generate_transaction
        from generator.src.models.account import create_account_pool

        pool = create_account_pool(size=50)
        categories = {generate_transaction(pool).merchant_category for _ in range(1000)}
        expected = {"retail", "dining", "travel", "online", "groceries", "entertainment", "utilities"}
        assert categories == expected, f"Missing categories: {expected - categories}"

    def test_transaction_type_distribution_covers_all(self) -> None:
        """All 4 transaction types should appear in generated transactions."""
        from generator.src.generators.transaction_generator import generate_transaction
        from generator.src.models.account import create_account_pool

        pool = create_account_pool(size=50)
        types = {generate_transaction(pool).transaction_type for _ in range(1000)}
        expected = {"purchase", "withdrawal", "transfer", "refund"}
        assert types == expected, f"Missing types: {expected - types}"

    def test_purchase_is_most_common_type(self) -> None:
        """Purchase should be the most common type (85% weight in config)."""
        from generator.src.generators.transaction_generator import generate_transaction
        from generator.src.models.account import create_account_pool

        pool = create_account_pool(size=50)
        types = Counter(generate_transaction(pool).transaction_type for _ in range(1000))
        assert types["purchase"] > 700, (
            f"Purchase count {types['purchase']} too low (expected ~850)"
        )

    def test_refund_has_negative_amount(self) -> None:
        """Refund transactions should have negative amounts."""
        from generator.src.generators.transaction_generator import generate_transaction
        from generator.src.models.account import create_account_pool

        pool = create_account_pool(size=50)
        refunds = []
        for _ in range(2000):
            tx = generate_transaction(pool)
            if tx.transaction_type == "refund":
                refunds.append(tx)
            if len(refunds) >= 10:
                break

        assert len(refunds) > 0, "No refund transactions generated in 2000 attempts"
        for tx in refunds:
            assert tx.amount < 0, f"Refund amount should be negative, got {tx.amount}"

    def test_generate_batch_returns_multiple(self) -> None:
        """generate_batch must return the requested number of transactions."""
        from generator.src.generators.transaction_generator import generate_batch
        from generator.src.models.account import create_account_pool

        pool = create_account_pool(size=10)
        batch = generate_batch(pool, count=25)
        assert len(batch) == 25

    def test_all_generated_transactions_have_unique_ids(self) -> None:
        """All transaction IDs in a batch must be unique."""
        from generator.src.generators.transaction_generator import generate_batch
        from generator.src.models.account import create_account_pool

        pool = create_account_pool(size=10)
        batch = generate_batch(pool, count=100)
        ids = [tx.transaction_id for tx in batch]
        assert len(ids) == len(set(ids)), "Duplicate transaction IDs found"
