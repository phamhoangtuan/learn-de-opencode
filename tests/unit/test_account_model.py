"""T019: Unit test — Account model pool generation and sampling.

Per Constitution v2.0.0 Principle III: Tests written FIRST, must FAIL before implementation.
"""

import re

import pytest


class TestAccountModel:
    """Test Account dataclass creation and validation."""

    def test_account_has_required_fields(self) -> None:
        """An Account must have account_id, account_type, creation_date, country."""
        from generator.src.models.account import Account

        account = Account(
            account_id="ACC123456789",
            account_type="checking",
            creation_date="2025-01-15",
            country="US",
        )
        assert account.account_id == "ACC123456789"
        assert account.account_type == "checking"
        assert account.creation_date == "2025-01-15"
        assert account.country == "US"

    def test_account_id_format_alphanumeric_10_to_12(self) -> None:
        """Account ID must be alphanumeric, 10-12 characters per data-model.md."""
        from generator.src.models.account import Account

        account = Account.generate()
        assert re.match(r"^[a-zA-Z0-9]{10,12}$", account.account_id), (
            f"Account ID '{account.account_id}' does not match pattern ^[a-zA-Z0-9]{{10,12}}$"
        )

    def test_account_type_is_valid_enum(self) -> None:
        """Account type must be one of: checking, savings, credit."""
        from generator.src.models.account import Account

        account = Account.generate()
        assert account.account_type in ("checking", "savings", "credit")

    def test_account_country_is_two_letter_code(self) -> None:
        """Country must be a 2-letter ISO 3166 code."""
        from generator.src.models.account import Account

        account = Account.generate()
        assert re.match(r"^[A-Z]{2}$", account.country)

    def test_account_creation_date_is_past(self) -> None:
        """Creation date must be a past date."""
        from datetime import date

        from generator.src.models.account import Account

        account = Account.generate()
        assert date.fromisoformat(account.creation_date) < date.today()


class TestAccountPool:
    """Test Account pool generation and sampling."""

    def test_create_account_pool_returns_correct_size(self) -> None:
        """create_account_pool(size=100) must return exactly 100 accounts."""
        from generator.src.models.account import create_account_pool

        pool = create_account_pool(size=100)
        assert len(pool) == 100

    def test_create_account_pool_default_size_1000(self) -> None:
        """create_account_pool() defaults to 1000 accounts per config."""
        from generator.src.models.account import create_account_pool

        pool = create_account_pool()
        assert len(pool) == 1000

    def test_account_pool_has_unique_ids(self) -> None:
        """All account IDs in the pool must be unique."""
        from generator.src.models.account import create_account_pool

        pool = create_account_pool(size=500)
        ids = [a.account_id for a in pool]
        assert len(ids) == len(set(ids)), "Duplicate account IDs found in pool"

    def test_account_pool_has_mixed_types(self) -> None:
        """Pool should contain all three account types."""
        from generator.src.models.account import create_account_pool

        pool = create_account_pool(size=100)
        types = {a.account_type for a in pool}
        assert types == {"checking", "savings", "credit"}, (
            f"Expected all 3 account types, got {types}"
        )

    def test_account_pool_has_mixed_countries(self) -> None:
        """Pool should contain multiple country codes."""
        from generator.src.models.account import create_account_pool

        pool = create_account_pool(size=100)
        countries = {a.country for a in pool}
        assert len(countries) >= 2, "Expected multiple countries in pool"

    def test_sample_account_returns_account_from_pool(self) -> None:
        """sample_account must return an Account from the provided pool."""
        from generator.src.models.account import Account, create_account_pool, sample_account

        pool = create_account_pool(size=50)
        sampled = sample_account(pool)
        assert isinstance(sampled, Account)
        assert sampled in pool
