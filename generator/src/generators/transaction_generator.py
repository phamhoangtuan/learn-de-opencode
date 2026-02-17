"""Transaction generator with realistic distributions.

Generates financial transactions using weighted random distributions
for amounts (log-normal), currencies, merchant categories, and types.

Per spec.md FR-001: Realistic data patterns with varying amounts,
currencies, categories, and transaction types.
"""

from generator.src.models.account import Account, sample_account
from generator.src.models.transaction import Transaction


def generate_transaction(
    account_pool: list[Account],
    currency_weights: dict | None = None,
    category_weights: dict | None = None,
    type_weights: dict | None = None,
) -> Transaction:
    """Generate a single transaction using a random account from the pool.

    Args:
        account_pool: Pool of accounts to sample from.
        currency_weights: Optional currency distribution overrides.
        category_weights: Optional category distribution overrides.
        type_weights: Optional transaction type distribution overrides.

    Returns:
        A new Transaction with realistic random values.
    """
    account = sample_account(account_pool)
    return Transaction.generate(
        account,
        currency_weights=currency_weights,
        category_weights=category_weights,
        type_weights=type_weights,
    )


def generate_batch(
    account_pool: list[Account],
    count: int = 10,
    currency_weights: dict | None = None,
    category_weights: dict | None = None,
    type_weights: dict | None = None,
) -> list[Transaction]:
    """Generate a batch of transactions.

    Args:
        account_pool: Pool of accounts to sample from.
        count: Number of transactions to generate.
        currency_weights: Optional currency distribution overrides.
        category_weights: Optional category distribution overrides.
        type_weights: Optional transaction type distribution overrides.

    Returns:
        List of Transaction instances.
    """
    return [
        generate_transaction(
            account_pool,
            currency_weights=currency_weights,
            category_weights=category_weights,
            type_weights=type_weights,
        )
        for _ in range(count)
    ]
