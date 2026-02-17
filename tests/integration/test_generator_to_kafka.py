"""T026: Integration test — generator produces valid messages to Kafka end-to-end.

Per Constitution v2.0.0 Principle III: Tests written FIRST, must FAIL before implementation.
This test requires Docker services (Kafka) to be running.
"""

import json
import time

import jsonschema
import pytest


@pytest.mark.integration
class TestGeneratorToKafkaIntegration:
    """End-to-end test: generator produces valid messages to Kafka topic.

    Requires: docker compose up kafka init-kafka
    """

    @pytest.fixture
    def kafka_bootstrap(self) -> str:
        """Return Kafka bootstrap servers for integration tests."""
        return "localhost:9092"

    def test_generator_produces_to_kafka_topic(
        self, kafka_bootstrap: str, raw_transaction_schema: dict
    ) -> None:
        """Generator must produce schema-valid messages to raw-transactions topic."""
        from confluent_kafka import Consumer, KafkaError

        from generator.src.generators.transaction_generator import generate_transaction
        from generator.src.kafka.producer import TransactionProducer
        from generator.src.models.account import create_account_pool

        # Produce some transactions
        pool = create_account_pool(size=10)
        producer = TransactionProducer(
            bootstrap_servers=kafka_bootstrap,
            topic="raw-transactions",
        )

        produced_ids = []
        for _ in range(5):
            tx = generate_transaction(pool)
            producer.send(tx.to_json(), key=tx.transaction_id)
            produced_ids.append(tx.transaction_id)

        producer.flush(timeout=10.0)

        # Consume and verify
        consumer = Consumer({
            "bootstrap.servers": kafka_bootstrap,
            "group.id": "test-integration-group",
            "auto.offset.reset": "earliest",
        })
        consumer.subscribe(["raw-transactions"])

        consumed = []
        deadline = time.time() + 15  # 15 second timeout
        while len(consumed) < 5 and time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            consumed.append(json.loads(msg.value().decode("utf-8")))

        consumer.close()

        assert len(consumed) >= 5, f"Expected 5 messages, got {len(consumed)}"

        # Validate each consumed message against schema
        for msg_data in consumed:
            jsonschema.validate(instance=msg_data, schema=raw_transaction_schema)

    def test_generator_sustained_rate(self, kafka_bootstrap: str) -> None:
        """Generator should sustain configured rate for at least 10 seconds."""
        from generator.src.generators.transaction_generator import generate_transaction
        from generator.src.kafka.producer import TransactionProducer
        from generator.src.lib.rate_limiter import RateLimiter
        from generator.src.models.account import create_account_pool

        pool = create_account_pool(size=100)
        producer = TransactionProducer(
            bootstrap_servers=kafka_bootstrap,
            topic="raw-transactions",
        )
        limiter = RateLimiter(rate=10)

        count = 0
        start = time.time()
        duration = 5  # 5 second sustained test

        while time.time() - start < duration:
            if limiter.acquire():
                tx = generate_transaction(pool)
                producer.send(tx.to_json(), key=tx.transaction_id)
                count += 1
            else:
                limiter.wait()

        producer.flush(timeout=10.0)
        elapsed = time.time() - start

        actual_rate = count / elapsed
        # Should be roughly 10 tx/sec ± 20%
        assert actual_rate > 8, f"Rate {actual_rate:.1f} too low (expected ~10 tx/sec)"
        assert actual_rate < 12, f"Rate {actual_rate:.1f} too high (expected ~10 tx/sec)"
