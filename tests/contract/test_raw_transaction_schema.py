"""T018: Contract test — validate generated transactions against raw-transactions.schema.json.

Per Constitution v2.0.0 Principle III: Tests written FIRST, must FAIL before implementation.
Per Constitution v2.0.0 Principle II: Data quality validated against canonical schema.
"""

import copy
import uuid
from datetime import datetime, timezone

import jsonschema
import pytest


class TestRawTransactionSchemaCompliance:
    """Validate that generated transactions conform to the raw-transactions contract."""

    def test_valid_transaction_passes_schema(
        self, raw_transaction_schema: dict, sample_valid_transaction: dict
    ) -> None:
        """A fully valid transaction must pass schema validation."""
        jsonschema.validate(instance=sample_valid_transaction, schema=raw_transaction_schema)

    def test_missing_required_field_fails_schema(
        self, raw_transaction_schema: dict, sample_valid_transaction: dict
    ) -> None:
        """Each required field, when removed, must cause schema validation failure."""
        required_fields = raw_transaction_schema["required"]
        for field in required_fields:
            invalid = {k: v for k, v in sample_valid_transaction.items() if k != field}
            with pytest.raises(jsonschema.ValidationError):
                jsonschema.validate(instance=invalid, schema=raw_transaction_schema)

    def test_invalid_currency_fails_schema(
        self, raw_transaction_schema: dict, sample_valid_transaction: dict
    ) -> None:
        """A currency not in the allowed enum must fail validation."""
        invalid = {**sample_valid_transaction, "currency": "BTC"}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=raw_transaction_schema)

    def test_invalid_transaction_type_fails_schema(
        self, raw_transaction_schema: dict, sample_valid_transaction: dict
    ) -> None:
        """A transaction_type not in the allowed enum must fail validation."""
        invalid = {**sample_valid_transaction, "transaction_type": "deposit"}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=raw_transaction_schema)

    def test_invalid_merchant_category_fails_schema(
        self, raw_transaction_schema: dict, sample_valid_transaction: dict
    ) -> None:
        """A merchant_category not in the allowed enum must fail validation."""
        invalid = {**sample_valid_transaction, "merchant_category": "gambling"}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=raw_transaction_schema)

    def test_invalid_status_fails_schema(
        self, raw_transaction_schema: dict, sample_valid_transaction: dict
    ) -> None:
        """A status not in the allowed enum must fail validation."""
        invalid = {**sample_valid_transaction, "status": "processing"}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=raw_transaction_schema)

    def test_zero_amount_fails_schema(
        self, raw_transaction_schema: dict, sample_valid_transaction: dict
    ) -> None:
        """An amount of exactly zero must fail validation (non-zero required)."""
        invalid = {**sample_valid_transaction, "amount": 0}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=raw_transaction_schema)

    def test_negative_amount_passes_schema(
        self, raw_transaction_schema: dict, sample_valid_transaction: dict
    ) -> None:
        """A negative amount (refund) must pass validation."""
        refund = {**sample_valid_transaction, "amount": -29.99, "transaction_type": "refund"}
        jsonschema.validate(instance=refund, schema=raw_transaction_schema)

    def test_amount_exceeds_max_fails_schema(
        self, raw_transaction_schema: dict, sample_valid_transaction: dict
    ) -> None:
        """An amount exceeding 50000.00 must fail validation."""
        invalid = {**sample_valid_transaction, "amount": 50001.00}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=raw_transaction_schema)

    def test_amount_below_min_fails_schema(
        self, raw_transaction_schema: dict, sample_valid_transaction: dict
    ) -> None:
        """An amount below -50000.00 must fail validation."""
        invalid = {**sample_valid_transaction, "amount": -50001.00}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=raw_transaction_schema)

    def test_invalid_account_id_pattern_fails_schema(
        self, raw_transaction_schema: dict, sample_valid_transaction: dict
    ) -> None:
        """An account_id not matching alphanumeric 10-12 chars must fail validation."""
        invalid = {**sample_valid_transaction, "account_id": "short"}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=raw_transaction_schema)

    def test_invalid_country_code_fails_schema(
        self, raw_transaction_schema: dict, sample_valid_transaction: dict
    ) -> None:
        """A location_country not matching 2-letter uppercase must fail validation."""
        invalid = {**sample_valid_transaction, "location_country": "USA"}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=raw_transaction_schema)

    def test_additional_properties_fails_schema(
        self, raw_transaction_schema: dict, sample_valid_transaction: dict
    ) -> None:
        """Extra fields not in the schema must fail (additionalProperties: false)."""
        invalid = {**sample_valid_transaction, "extra_field": "not_allowed"}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=raw_transaction_schema)

    def test_empty_merchant_name_fails_schema(
        self, raw_transaction_schema: dict, sample_valid_transaction: dict
    ) -> None:
        """An empty merchant_name must fail validation (minLength: 1)."""
        invalid = {**sample_valid_transaction, "merchant_name": ""}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=raw_transaction_schema)

    def test_generated_transaction_passes_schema(
        self, raw_transaction_schema: dict
    ) -> None:
        """A transaction from the Account/Transaction models must pass schema validation.

        This test will FAIL until T027-T028 are implemented.
        """
        from generator.src.models.account import Account, create_account_pool
        from generator.src.models.transaction import Transaction

        pool = create_account_pool(size=10)
        account = pool[0]
        tx = Transaction.generate(account)
        tx_dict = tx.to_dict()
        jsonschema.validate(instance=tx_dict, schema=raw_transaction_schema)
