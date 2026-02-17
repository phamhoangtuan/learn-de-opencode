"""T044: Integration test — end-to-end pipeline flow.

Per Constitution v2.0.0 Principle III: Tests written FIRST, must FAIL before implementation.

Tests the full pipeline topology:
  Generator → Kafka (raw-transactions) → Flink → Kafka (alerts, dead-letter) + Iceberg

Requires all Docker services to be running:
  docker compose up -d

These tests are slow (require service startup, message propagation, and processing).
They verify acceptance scenarios from US2 in spec.md.
"""

import json
import time
import uuid

import jsonschema
import pytest


@pytest.mark.integration
class TestPipelineEndToEnd:
    """End-to-end integration tests for the full streaming pipeline.

    Requires: docker compose up -d (all services including Flink)
    """

    @pytest.fixture
    def kafka_bootstrap(self) -> str:
        """Return Kafka bootstrap servers for integration tests."""
        return "localhost:9092"

    @pytest.fixture
    def producer(self, kafka_bootstrap: str):
        """Create a Kafka producer for sending test transactions."""
        from confluent_kafka import Producer

        return Producer({"bootstrap.servers": kafka_bootstrap})

    @pytest.fixture
    def consumer_factory(self, kafka_bootstrap: str):
        """Factory for creating Kafka consumers with unique group IDs."""
        from confluent_kafka import Consumer

        consumers = []

        def _create(topic: str):
            consumer = Consumer({
                "bootstrap.servers": kafka_bootstrap,
                "group.id": f"test-e2e-{uuid.uuid4().hex[:8]}",
                "auto.offset.reset": "latest",
            })
            consumer.subscribe([topic])
            # Do an initial poll to trigger partition assignment
            consumer.poll(1.0)
            consumers.append(consumer)
            return consumer

        yield _create

        for c in consumers:
            c.close()

    def _produce_transaction(self, producer, transaction: dict) -> None:
        """Produce a transaction to the raw-transactions topic."""
        producer.produce(
            "raw-transactions",
            key=transaction["transaction_id"],
            value=json.dumps(transaction).encode("utf-8"),
        )
        producer.flush(timeout=10.0)

    def _consume_messages(self, consumer, count: int, timeout: float = 30.0) -> list:
        """Consume up to `count` messages from the subscribed topic."""
        messages = []
        deadline = time.time() + timeout
        while len(messages) < count and time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                data = json.loads(msg.value().decode("utf-8"))
                messages.append(data)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
        return messages

    def test_high_value_transaction_generates_alert(
        self,
        producer,
        consumer_factory,
        alert_schema: dict,
        sample_high_value_transaction: dict,
    ) -> None:
        """US2-SC1: Transaction exceeding $10,000 generates a high-severity alert within 5 seconds.

        Given a transaction exceeding $10,000 is published,
        When the Flink stream processor evaluates it,
        Then a high-severity alert is generated within 5 seconds.
        """
        # Set up consumer on alerts topic BEFORE producing
        alert_consumer = consumer_factory("alerts")

        # Generate a unique transaction
        tx = {
            **sample_high_value_transaction,
            "transaction_id": str(uuid.uuid4()),
            "amount": 15000.00,
        }

        # Produce to raw-transactions
        self._produce_transaction(producer, tx)

        # Wait for alert on alerts topic
        alerts = self._consume_messages(alert_consumer, count=1, timeout=15.0)

        assert len(alerts) >= 1, (
            f"Expected at least 1 alert for high-value transaction "
            f"(txId={tx['transaction_id']}), got {len(alerts)}"
        )

        # Validate alert against schema
        alert = alerts[0]
        jsonschema.validate(instance=alert, schema=alert_schema)

        # Verify alert references our transaction
        assert alert["transaction_id"] == tx["transaction_id"]
        assert alert["rule_name"] == "high-value-transaction"
        assert alert["severity"] == "high"

    def test_normal_transaction_no_alert(
        self,
        producer,
        consumer_factory,
        sample_valid_transaction: dict,
    ) -> None:
        """US2-SC3: Normal transaction under thresholds generates no alert.

        Given a normal transaction (under thresholds, normal patterns),
        When the Flink stream processor evaluates it,
        Then no alert is generated.
        """
        alert_consumer = consumer_factory("alerts")

        tx = {
            **sample_valid_transaction,
            "transaction_id": str(uuid.uuid4()),
            "amount": 50.00,
            "timestamp": "2026-02-17T12:00:00.000Z",  # Normal hour
        }

        self._produce_transaction(producer, tx)

        # Wait briefly — should NOT receive an alert
        alerts = self._consume_messages(alert_consumer, count=1, timeout=10.0)

        matching_alerts = [a for a in alerts if a.get("transaction_id") == tx["transaction_id"]]
        assert len(matching_alerts) == 0, (
            f"Expected no alerts for normal transaction, got {len(matching_alerts)}"
        )

    def test_malformed_transaction_routes_to_dlq(
        self,
        producer,
        consumer_factory,
        dead_letter_schema: dict,
    ) -> None:
        """US2-EC: Malformed transaction routed to dead-letter topic.

        Given a transaction with invalid fields,
        When the Flink stream processor validates it,
        Then it is routed to the dead-letter topic with error details.
        """
        dlq_consumer = consumer_factory("dead-letter")

        # Produce a malformed transaction (invalid currency)
        tx = {
            "transaction_id": str(uuid.uuid4()),
            "timestamp": "2026-02-17T12:00:00.000Z",
            "account_id": "ACC123456789",
            "amount": 100.00,
            "currency": "INVALID",  # Invalid currency
            "merchant_name": "Test Store",
            "merchant_category": "retail",
            "transaction_type": "purchase",
            "location_country": "US",
            "status": "completed",
        }

        self._produce_transaction(producer, tx)

        # Wait for DLQ record
        dlq_records = self._consume_messages(dlq_consumer, count=1, timeout=15.0)

        assert len(dlq_records) >= 1, (
            f"Expected at least 1 DLQ record for malformed transaction, "
            f"got {len(dlq_records)}"
        )

        # Validate DLQ record against schema
        dlr = dlq_records[0]
        jsonschema.validate(instance=dlr, schema=dead_letter_schema)
        assert dlr["error_field"] == "currency"

    def test_alert_contains_required_fields(
        self,
        producer,
        consumer_factory,
        sample_high_value_transaction: dict,
    ) -> None:
        """US2-SC5: Alert includes all required fields.

        Given an alert is generated,
        Then it includes: transaction_id, rule_name, severity, alert_timestamp, description.
        """
        alert_consumer = consumer_factory("alerts")

        tx = {
            **sample_high_value_transaction,
            "transaction_id": str(uuid.uuid4()),
        }

        self._produce_transaction(producer, tx)
        alerts = self._consume_messages(alert_consumer, count=1, timeout=15.0)

        assert len(alerts) >= 1, "Expected at least 1 alert"

        alert = alerts[0]
        required_fields = ["alert_id", "transaction_id", "rule_name", "severity",
                           "alert_timestamp", "description"]
        for field in required_fields:
            assert field in alert, f"Alert missing required field: {field}"

    def test_unusual_hour_transaction_generates_alert(
        self,
        producer,
        consumer_factory,
        sample_unusual_hour_transaction: dict,
    ) -> None:
        """US2-SC1 variant: Transaction during quiet hours generates low-severity alert.

        Given a transaction at 02:30 UTC (within 01:00-05:00 quiet hours),
        When the Flink stream processor evaluates it,
        Then a low-severity alert is generated for unusual-hour rule.
        """
        alert_consumer = consumer_factory("alerts")

        tx = {
            **sample_unusual_hour_transaction,
            "transaction_id": str(uuid.uuid4()),
        }

        self._produce_transaction(producer, tx)
        alerts = self._consume_messages(alert_consumer, count=1, timeout=15.0)

        # Filter for our specific transaction's alerts
        matching = [a for a in alerts if a.get("transaction_id") == tx["transaction_id"]]

        unusual_hour_alerts = [a for a in matching if a.get("rule_name") == "unusual-hour"]
        assert len(unusual_hour_alerts) >= 1, (
            f"Expected unusual-hour alert for tx at 02:30 UTC, "
            f"got alerts: {matching}"
        )
        assert unusual_hour_alerts[0]["severity"] == "low"

    def test_multiple_rules_trigger_multiple_alerts(
        self,
        producer,
        consumer_factory,
    ) -> None:
        """Edge case: Transaction matching multiple rules generates multiple alerts.

        From spec.md edge case line 105: Both alerts should be generated
        independently, each referencing the same transaction.
        """
        alert_consumer = consumer_factory("alerts")

        # High-value + unusual hour: should trigger 2 rules
        tx = {
            "transaction_id": str(uuid.uuid4()),
            "timestamp": "2026-02-17T03:00:00.000Z",  # Unusual hour
            "account_id": "ACC123456789",
            "amount": 25000.00,  # High value
            "currency": "USD",
            "merchant_name": "Late Night Store",
            "merchant_category": "retail",
            "transaction_type": "purchase",
            "location_country": "US",
            "status": "completed",
        }

        self._produce_transaction(producer, tx)
        alerts = self._consume_messages(alert_consumer, count=2, timeout=15.0)

        matching = [a for a in alerts if a.get("transaction_id") == tx["transaction_id"]]

        assert len(matching) >= 2, (
            f"Expected at least 2 alerts for multi-rule trigger, "
            f"got {len(matching)}: {matching}"
        )

        rule_names = {a["rule_name"] for a in matching}
        assert "high-value-transaction" in rule_names
        assert "unusual-hour" in rule_names
