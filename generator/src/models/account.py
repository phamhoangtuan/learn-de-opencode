"""Account model with pool management for the data generator.

Maintains a pool of ~1000 pre-generated accounts that are sampled
during transaction generation to create realistic patterns.

Per data-model.md: Account is generator-internal, not persisted to Iceberg.
"""

import random
import string
from dataclasses import dataclass
from datetime import date, timedelta

from generator.src.models.enums import AccountType


# Countries weighted toward US for realistic distribution
_COUNTRIES = ["US", "US", "US", "GB", "GB", "DE", "JP", "FR", "CA", "AU"]

# Account type weights
_ACCOUNT_TYPE_WEIGHTS = {
    AccountType.CHECKING: 0.50,
    AccountType.SAVINGS: 0.30,
    AccountType.CREDIT: 0.20,
}


@dataclass(frozen=True)
class Account:
    """A financial account used by the data generator.

    Attributes:
        account_id: Unique alphanumeric identifier, 10-12 chars.
        account_type: One of checking, savings, credit.
        creation_date: ISO 8601 date string when account was opened.
        country: 2-letter ISO 3166 country code.
    """

    account_id: str
    account_type: str
    creation_date: str
    country: str

    @classmethod
    def generate(cls) -> "Account":
        """Generate a random Account with realistic attributes.

        Returns:
            A new Account with random but valid field values.
        """
        # Account ID: ACC prefix + 7-9 random alphanumeric = 10-12 total chars
        suffix_len = random.randint(7, 9)
        suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=suffix_len))
        account_id = f"ACC{suffix}"

        # Account type weighted selection
        types = list(_ACCOUNT_TYPE_WEIGHTS.keys())
        weights = list(_ACCOUNT_TYPE_WEIGHTS.values())
        account_type = random.choices(types, weights=weights, k=1)[0].value

        # Creation date: random date in the past 1-5 years
        days_ago = random.randint(365, 365 * 5)
        creation_date = (date.today() - timedelta(days=days_ago)).isoformat()

        # Country
        country = random.choice(_COUNTRIES)

        return cls(
            account_id=account_id,
            account_type=account_type,
            creation_date=creation_date,
            country=country,
        )


def create_account_pool(size: int = 1000) -> list[Account]:
    """Create a pool of pre-generated accounts.

    Args:
        size: Number of accounts to generate. Defaults to 1000.

    Returns:
        List of unique Account instances.
    """
    pool: list[Account] = []
    seen_ids: set[str] = set()

    while len(pool) < size:
        account = Account.generate()
        if account.account_id not in seen_ids:
            seen_ids.add(account.account_id)
            pool.append(account)

    return pool


def sample_account(pool: list[Account]) -> Account:
    """Sample a random account from the pool.

    Args:
        pool: List of Account instances to sample from.

    Returns:
        A randomly selected Account from the pool.
    """
    return random.choice(pool)
