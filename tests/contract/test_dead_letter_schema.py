"""T038: Contract test — validate dead letter records against dead-letter.schema.json.

Per Constitution v2.0.0 Principle III: Tests written FIRST, must FAIL before implementation.
Per Constitution v2.0.0 Principle II: Data quality validated against canonical schema.
"""

import json
from datetime import datetime, timezone

import jsonschema
import pytest


@pytest.fixture
def sample_valid_dead_letter() -> dict:
    """Return a sample valid dead letter record matching the contract."""
    return {
        "original_record": json.dumps({"transaction_id": "abc", "amount": 0, "currency": "XYZ"}),
        "error_field": "currency",
        "error_type": "invalid_enum",
        "expected_format": "one of: USD, EUR, GBP, JPY",
        "actual_value": "XYZ",
        "error_timestamp": datetime.now(timezone.utc).isoformat(),
        "processor_id": "transaction-validator-0",
    }


class TestDeadLetterSchemaCompliance:
    """Validate that dead letter records conform to the dead-letter contract."""

    def test_valid_dead_letter_passes_schema(
        self, dead_letter_schema: dict, sample_valid_dead_letter: dict
    ) -> None:
        """A fully valid dead letter record must pass schema validation."""
        jsonschema.validate(instance=sample_valid_dead_letter, schema=dead_letter_schema)

    def test_missing_required_field_fails_schema(
        self, dead_letter_schema: dict, sample_valid_dead_letter: dict
    ) -> None:
        """Each required field, when removed, must cause schema validation failure."""
        required_fields = dead_letter_schema["required"]
        for field in required_fields:
            invalid = {k: v for k, v in sample_valid_dead_letter.items() if k != field}
            with pytest.raises(jsonschema.ValidationError):
                jsonschema.validate(instance=invalid, schema=dead_letter_schema)

    def test_invalid_error_type_fails_schema(
        self, dead_letter_schema: dict, sample_valid_dead_letter: dict
    ) -> None:
        """An error_type not in the allowed enum must fail validation."""
        invalid = {**sample_valid_dead_letter, "error_type": "unknown_error"}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=dead_letter_schema)

    def test_all_valid_error_types(
        self, dead_letter_schema: dict, sample_valid_dead_letter: dict
    ) -> None:
        """All valid error_type values must pass schema validation."""
        valid_types = [
            "missing", "invalid_format", "out_of_range",
            "invalid_enum", "duplicate", "invalid_state_transition",
        ]
        for error_type in valid_types:
            valid = {**sample_valid_dead_letter, "error_type": error_type}
            jsonschema.validate(instance=valid, schema=dead_letter_schema)

    def test_null_actual_value_passes_schema(
        self, dead_letter_schema: dict, sample_valid_dead_letter: dict
    ) -> None:
        """A null actual_value (field was missing) must pass validation."""
        valid = {**sample_valid_dead_letter, "actual_value": None}
        jsonschema.validate(instance=valid, schema=dead_letter_schema)

    def test_empty_original_record_fails_schema(
        self, dead_letter_schema: dict, sample_valid_dead_letter: dict
    ) -> None:
        """An empty original_record must fail validation (minLength: 1)."""
        invalid = {**sample_valid_dead_letter, "original_record": ""}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=dead_letter_schema)

    def test_empty_error_field_fails_schema(
        self, dead_letter_schema: dict, sample_valid_dead_letter: dict
    ) -> None:
        """An empty error_field must fail validation (minLength: 1)."""
        invalid = {**sample_valid_dead_letter, "error_field": ""}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=dead_letter_schema)

    def test_empty_expected_format_fails_schema(
        self, dead_letter_schema: dict, sample_valid_dead_letter: dict
    ) -> None:
        """An empty expected_format must fail validation (minLength: 1)."""
        invalid = {**sample_valid_dead_letter, "expected_format": ""}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=dead_letter_schema)

    def test_empty_processor_id_fails_schema(
        self, dead_letter_schema: dict, sample_valid_dead_letter: dict
    ) -> None:
        """An empty processor_id must fail validation (minLength: 1)."""
        invalid = {**sample_valid_dead_letter, "processor_id": ""}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=dead_letter_schema)

    def test_additional_properties_fails_schema(
        self, dead_letter_schema: dict, sample_valid_dead_letter: dict
    ) -> None:
        """Extra fields not in the schema must fail (additionalProperties: false)."""
        invalid = {**sample_valid_dead_letter, "extra_field": "not_allowed"}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=dead_letter_schema)
