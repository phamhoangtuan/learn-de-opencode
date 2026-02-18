"""Account generation logic for synthetic financial data."""

from __future__ import annotations

from dataclasses import dataclass

from faker import Faker
from numpy.random import Generator

# Account type weights per data-model.md
ACCOUNT_TYPES: list[str] = ["checking", "savings", "credit"]
ACCOUNT_TYPE_WEIGHTS: list[float] = [0.50, 0.20, 0.30]


@dataclass
class Account:
    """A synthetic bank/financial account.

    Attributes:
        account_id: Unique identifier in ACC-XXXXX format.
        account_holder: Generated full name.
        account_type: One of checking, savings, credit.
    """

    account_id: str
    account_holder: str
    account_type: str


def generate_accounts(
    rng: Generator,
    count: int,
    seed: int,
) -> list[Account]:
    """Generate a list of unique synthetic accounts.

    Args:
        rng: NumPy random generator instance (for account type selection).
        count: Number of accounts to generate.
        seed: Seed for Faker name generation (reproducibility).

    Returns:
        List of Account instances with unique IDs, generated names,
        and weighted account types.
    """
    fake = Faker()
    Faker.seed(seed)

    # Generate weighted account types
    account_types = rng.choice(
        ACCOUNT_TYPES,
        size=count,
        p=ACCOUNT_TYPE_WEIGHTS,
    )

    accounts: list[Account] = []
    for i in range(count):
        account_id = f"ACC-{i:05d}"
        account_holder = fake.name()
        account_type = str(account_types[i])
        accounts.append(Account(account_id, account_holder, account_type))

    return accounts
