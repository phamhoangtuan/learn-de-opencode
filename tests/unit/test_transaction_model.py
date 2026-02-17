"""T020: Unit test — Transaction model dataclass creation and JSON serialization.

Per Constitution v2.0.0 Principle III: Tests written FIRST, must FAIL before implementation.
"""

import json
import re
import uuid
from datetime import datetime, timezone

import pytest


class TestTransactionModel:
    """Test Transaction dataclass creation and field validation."""

    def test_transaction_has_all_required_fields(self) -> None:
        """A Transaction must have all 10 required fields per data-model.md."""
        from generator.src.models.transaction import Transaction

        tx = Transaction(
            transaction_id=str(uuid.uuid4()),
            timestamp=datetime.now(timezone.utc).isoformat(),
            account_id="ACC123456789",
            amount=125.50,
            currency="USD",
            merchant_name="Amazon",
            merchant_category="online",
            transaction_type="purchase",
            location_country="US",
            status="completed",
        )
        assert tx.transaction_id is not None
        assert tx.timestamp is not None
        assert tx.account_id == "ACC123456789"
        assert tx.amount == 125.50

    def test_transaction_id_is_uuid_format(self) -> None:
        """transaction_id must be a valid UUID v4 string."""
        from generator.src.models.transaction import Transaction

        tx = Transaction(
            transaction_id=str(uuid.uuid4()),
            timestamp=datetime.now(timezone.utc).isoformat(),
            account_id="ACC123456789",
            amount=100.0,
            currency="USD",
            merchant_name="Test",
            merchant_category="retail",
            transaction_type="purchase",
            location_country="US",
            status="pending",
        )
        # Must parse as valid UUID
        parsed = uuid.UUID(tx.transaction_id)
        assert str(parsed) == tx.transaction_id


class TestTransactionGenerate:
    """Test Transaction.generate() factory method."""

    def test_generate_creates_valid_transaction(self) -> None:
        """Transaction.generate() must produce a transaction with all required fields."""
        from generator.src.models.account import Account
        from generator.src.models.transaction import Transaction

        account = Account(
            account_id="ACC123456789",
            account_type="checking",
            creation_date="2025-01-15",
            country="US",
        )
        tx = Transaction.generate(account)
        assert tx.account_id == "ACC123456789"
        assert tx.amount != 0  # Non-zero per data-model.md
        assert tx.currency in ("USD", "EUR", "GBP", "JPY")
        assert tx.transaction_type in ("purchase", "withdrawal", "transfer", "refund")
        assert tx.merchant_category in (
            "retail", "dining", "travel", "online",
            "groceries", "entertainment", "utilities",
        )
        assert tx.status in ("pending", "completed", "failed", "reversed")
        assert re.match(r"^[A-Z]{2}$", tx.location_country)

    def test_generate_uses_account_country(self) -> None:
        """Generated transactions should use the account's country."""
        from generator.src.models.account import Account
        from generator.src.models.transaction import Transaction

        account = Account(
            account_id="ACC987654321",
            account_type="savings",
            creation_date="2025-06-01",
            country="GB",
        )
        tx = Transaction.generate(account)
        assert tx.location_country == "GB"

    def test_generate_amount_within_range(self) -> None:
        """Generated amount must be within -50000 to 50000 and non-zero."""
        from generator.src.models.account import Account
        from generator.src.models.transaction import Transaction

        account = Account(
            account_id="ACC123456789",
            account_type="checking",
            creation_date="2025-01-15",
            country="US",
        )
        for _ in range(100):
            tx = Transaction.generate(account)
            assert -50000 <= tx.amount <= 50000, f"Amount {tx.amount} out of range"
            assert tx.amount != 0, "Amount must be non-zero"


class TestTransactionSerialization:
    """Test Transaction JSON serialization per raw-transactions contract."""

    def test_to_dict_returns_all_required_fields(self) -> None:
        """to_dict() must include all 10 required fields from the schema."""
        from generator.src.models.transaction import Transaction

        tx = Transaction(
            transaction_id="550e8400-e29b-41d4-a716-446655440000",
            timestamp="2026-02-16T14:30:00.000Z",
            account_id="ACC123456789",
            amount=125.50,
            currency="USD",
            merchant_name="Amazon",
            merchant_category="online",
            transaction_type="purchase",
            location_country="US",
            status="completed",
        )
        d = tx.to_dict()
        required = [
            "transaction_id", "timestamp", "account_id", "amount",
            "currency", "merchant_name", "merchant_category",
            "transaction_type", "location_country", "status",
        ]
        for field in required:
            assert field in d, f"Missing field: {field}"

    def test_to_dict_excludes_extra_fields(self) -> None:
        """to_dict() must not include fields not in the raw-transactions schema."""
        from generator.src.models.transaction import Transaction

        tx = Transaction(
            transaction_id="550e8400-e29b-41d4-a716-446655440000",
            timestamp="2026-02-16T14:30:00.000Z",
            account_id="ACC123456789",
            amount=125.50,
            currency="USD",
            merchant_name="Amazon",
            merchant_category="online",
            transaction_type="purchase",
            location_country="US",
            status="completed",
        )
        d = tx.to_dict()
        allowed = {
            "transaction_id", "timestamp", "account_id", "amount",
            "currency", "merchant_name", "merchant_category",
            "transaction_type", "location_country", "status",
        }
        extra = set(d.keys()) - allowed
        assert not extra, f"Extra fields found: {extra}"

    def test_to_json_produces_valid_json_string(self) -> None:
        """to_json() must produce a valid JSON string."""
        from generator.src.models.transaction import Transaction

        tx = Transaction(
            transaction_id="550e8400-e29b-41d4-a716-446655440000",
            timestamp="2026-02-16T14:30:00.000Z",
            account_id="ACC123456789",
            amount=125.50,
            currency="USD",
            merchant_name="Amazon",
            merchant_category="online",
            transaction_type="purchase",
            location_country="US",
            status="completed",
        )
        json_str = tx.to_json()
        parsed = json.loads(json_str)
        assert parsed["transaction_id"] == "550e8400-e29b-41d4-a716-446655440000"
        assert parsed["amount"] == 125.50

    def test_to_json_roundtrip_preserves_data(self) -> None:
        """Serializing and deserializing must preserve all field values."""
        from generator.src.models.transaction import Transaction

        tx = Transaction(
            transaction_id="550e8400-e29b-41d4-a716-446655440000",
            timestamp="2026-02-16T14:30:00.000Z",
            account_id="ACC123456789",
            amount=-29.99,
            currency="EUR",
            merchant_name="Starbucks",
            merchant_category="dining",
            transaction_type="refund",
            location_country="DE",
            status="pending",
        )
        json_str = tx.to_json()
        parsed = json.loads(json_str)
        assert parsed["amount"] == -29.99
        assert parsed["currency"] == "EUR"
        assert parsed["transaction_type"] == "refund"
        assert parsed["status"] == "pending"
