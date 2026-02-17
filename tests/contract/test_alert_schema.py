"""T037: Contract test — validate generated alerts against alerts.schema.json.

Per Constitution v2.0.0 Principle III: Tests written FIRST, must FAIL before implementation.
Per Constitution v2.0.0 Principle II: Data quality validated against canonical schema.
"""

import uuid
from datetime import datetime, timezone

import jsonschema
import pytest


@pytest.fixture
def sample_valid_alert() -> dict:
    """Return a sample valid alert matching the alerts contract."""
    return {
        "alert_id": str(uuid.uuid4()),
        "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
        "rule_name": "high-value-transaction",
        "severity": "high",
        "alert_timestamp": datetime.now(timezone.utc).isoformat(),
        "description": "Transaction amount $12,500.00 exceeds threshold of $10,000",
    }


class TestAlertSchemaCompliance:
    """Validate that generated alerts conform to the alerts contract."""

    def test_valid_alert_passes_schema(
        self, alert_schema: dict, sample_valid_alert: dict
    ) -> None:
        """A fully valid alert must pass schema validation."""
        jsonschema.validate(instance=sample_valid_alert, schema=alert_schema)

    def test_missing_required_field_fails_schema(
        self, alert_schema: dict, sample_valid_alert: dict
    ) -> None:
        """Each required field, when removed, must cause schema validation failure."""
        required_fields = alert_schema["required"]
        for field in required_fields:
            invalid = {k: v for k, v in sample_valid_alert.items() if k != field}
            with pytest.raises(jsonschema.ValidationError):
                jsonschema.validate(instance=invalid, schema=alert_schema)

    def test_invalid_severity_fails_schema(
        self, alert_schema: dict, sample_valid_alert: dict
    ) -> None:
        """A severity not in the allowed enum must fail validation."""
        invalid = {**sample_valid_alert, "severity": "urgent"}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=alert_schema)

    def test_valid_severity_values(
        self, alert_schema: dict, sample_valid_alert: dict
    ) -> None:
        """All valid severity values must pass schema validation."""
        for severity in ["low", "medium", "high", "critical"]:
            valid = {**sample_valid_alert, "severity": severity}
            jsonschema.validate(instance=valid, schema=alert_schema)

    def test_invalid_rule_name_format_fails_schema(
        self, alert_schema: dict, sample_valid_alert: dict
    ) -> None:
        """A rule_name not matching slug format must fail validation."""
        invalid = {**sample_valid_alert, "rule_name": "HighValue"}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=alert_schema)

    def test_empty_rule_name_fails_schema(
        self, alert_schema: dict, sample_valid_alert: dict
    ) -> None:
        """An empty rule_name must fail validation (minLength: 1)."""
        invalid = {**sample_valid_alert, "rule_name": ""}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=alert_schema)

    def test_empty_description_fails_schema(
        self, alert_schema: dict, sample_valid_alert: dict
    ) -> None:
        """An empty description must fail validation (minLength: 1)."""
        invalid = {**sample_valid_alert, "description": ""}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=alert_schema)

    def test_description_exceeds_max_length_fails_schema(
        self, alert_schema: dict, sample_valid_alert: dict
    ) -> None:
        """A description exceeding 500 chars must fail validation."""
        invalid = {**sample_valid_alert, "description": "x" * 501}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=alert_schema)

    def test_additional_properties_fails_schema(
        self, alert_schema: dict, sample_valid_alert: dict
    ) -> None:
        """Extra fields not in the schema must fail (additionalProperties: false)."""
        invalid = {**sample_valid_alert, "extra_field": "not_allowed"}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=invalid, schema=alert_schema)

    def test_all_known_rule_names_pass_schema(
        self, alert_schema: dict, sample_valid_alert: dict
    ) -> None:
        """All three default rule names must pass validation."""
        for rule_name in ["high-value-transaction", "rapid-activity", "unusual-hour"]:
            valid = {**sample_valid_alert, "rule_name": rule_name}
            jsonschema.validate(instance=valid, schema=alert_schema)
