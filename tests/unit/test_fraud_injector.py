"""T022: Unit test — fraud_injector anomalous pattern injection.

Per Constitution v2.0.0 Principle III: Tests written FIRST, must FAIL before implementation.
Per spec.md FR-001/FR-004: Fraud injection creates data for alerting rule testing.
"""

import pytest


class TestFraudInjector:
    """Test fraud injection of anomalous transaction patterns."""

    def test_inject_high_value_sets_large_amount(self) -> None:
        """inject_high_value must set amount above $10,000 threshold."""
        from generator.src.generators.fraud_injector import inject_high_value
        from generator.src.models.account import Account
        from generator.src.models.transaction import Transaction

        account = Account(
            account_id="ACC123456789",
            account_type="checking",
            creation_date="2025-01-15",
            country="US",
        )
        tx = Transaction.generate(account)
        fraudulent = inject_high_value(tx)
        assert fraudulent.amount > 10000, (
            f"High-value injection should produce amount > $10,000, got {fraudulent.amount}"
        )

    def test_inject_unusual_hour_sets_quiet_hours(self) -> None:
        """inject_unusual_hour must set timestamp in 01:00-05:00 UTC range."""
        from datetime import datetime

        from generator.src.generators.fraud_injector import inject_unusual_hour
        from generator.src.models.account import Account
        from generator.src.models.transaction import Transaction

        account = Account(
            account_id="ACC123456789",
            account_type="checking",
            creation_date="2025-01-15",
            country="US",
        )
        tx = Transaction.generate(account)
        fraudulent = inject_unusual_hour(tx)
        ts = datetime.fromisoformat(fraudulent.timestamp)
        assert 1 <= ts.hour < 5, (
            f"Unusual hour injection should be 01-05 UTC, got hour {ts.hour}"
        )

    def test_inject_fraud_percentage_respected(self) -> None:
        """maybe_inject_fraud should inject at roughly the configured percentage.

        Config default: fraud_percentage: 3 (3% of transactions).
        """
        from generator.src.generators.fraud_injector import maybe_inject_fraud
        from generator.src.models.account import create_account_pool
        from generator.src.models.transaction import Transaction

        pool = create_account_pool(size=50)
        injected_count = 0
        total = 2000
        for _ in range(total):
            tx = Transaction.generate(pool[0])
            result = maybe_inject_fraud(tx, fraud_percentage=3)
            if result.is_fraudulent:
                injected_count += 1

        # With 3% rate over 2000, expect ~60 ± 30 (generous bounds)
        assert 10 < injected_count < 150, (
            f"Expected ~60 fraud injections at 3%, got {injected_count}/{total}"
        )

    def test_inject_fraud_zero_percentage_produces_none(self) -> None:
        """With fraud_percentage=0, no transactions should be modified."""
        from generator.src.generators.fraud_injector import maybe_inject_fraud
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
            result = maybe_inject_fraud(tx, fraud_percentage=0)
            assert not result.is_fraudulent, "Zero percentage should produce no fraud"

    def test_inject_fraud_100_percentage_produces_all(self) -> None:
        """With fraud_percentage=100, all transactions should be modified."""
        from generator.src.generators.fraud_injector import maybe_inject_fraud
        from generator.src.models.account import Account
        from generator.src.models.transaction import Transaction

        account = Account(
            account_id="ACC123456789",
            account_type="checking",
            creation_date="2025-01-15",
            country="US",
        )
        for _ in range(20):
            tx = Transaction.generate(account)
            result = maybe_inject_fraud(tx, fraud_percentage=100)
            assert result.is_fraudulent, "100% percentage should make all fraudulent"

    def test_injected_transaction_still_valid(self) -> None:
        """Fraud-injected transactions must still pass schema validation."""
        import jsonschema

        from generator.src.generators.fraud_injector import maybe_inject_fraud
        from generator.src.models.account import Account
        from generator.src.models.transaction import Transaction

        # Load schema
        import json
        from pathlib import Path

        schema_path = (
            Path(__file__).parent.parent.parent
            / "specs"
            / "001-streaming-financial-pipeline"
            / "contracts"
            / "raw-transactions.schema.json"
        )
        with open(schema_path) as f:
            schema = json.load(f)

        account = Account(
            account_id="ACC123456789",
            account_type="checking",
            creation_date="2025-01-15",
            country="US",
        )
        for _ in range(50):
            tx = Transaction.generate(account)
            result = maybe_inject_fraud(tx, fraud_percentage=100)
            tx_dict = result.transaction.to_dict()
            jsonschema.validate(instance=tx_dict, schema=schema)

    def test_fraud_result_has_injection_type(self) -> None:
        """FraudResult must indicate which injection type was applied."""
        from generator.src.generators.fraud_injector import maybe_inject_fraud
        from generator.src.models.account import Account
        from generator.src.models.transaction import Transaction

        account = Account(
            account_id="ACC123456789",
            account_type="checking",
            creation_date="2025-01-15",
            country="US",
        )
        tx = Transaction.generate(account)
        result = maybe_inject_fraud(tx, fraud_percentage=100)
        assert result.injection_type in ("high_value", "unusual_hour", "rapid_activity", None)
