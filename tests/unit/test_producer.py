"""T025: Unit test — Kafka producer wrapper with circuit breaker.

Per Constitution v2.0.0 Principle III: Tests written FIRST, must FAIL before implementation.
Per spec.md FR-011: Retry with exponential backoff on Kafka unavailability.
Per spec.md edge case: Buffer messages and retry when broker unavailable.
"""

from unittest.mock import MagicMock, patch

import pytest


class TestKafkaProducerWrapper:
    """Test Kafka producer wrapper with delivery callbacks and circuit breaker."""

    def test_producer_initializes_with_config(self) -> None:
        """Producer must accept bootstrap_servers and topic configuration."""
        from generator.src.kafka.producer import TransactionProducer

        producer = TransactionProducer(
            bootstrap_servers="localhost:9092",
            topic="raw-transactions",
        )
        assert producer.topic == "raw-transactions"
        assert producer.bootstrap_servers == "localhost:9092"

    def test_producer_send_calls_produce(self) -> None:
        """send() must call the underlying confluent_kafka Producer.produce()."""
        from generator.src.kafka.producer import TransactionProducer

        producer = TransactionProducer(
            bootstrap_servers="localhost:9092",
            topic="raw-transactions",
        )
        # Mock the internal kafka producer
        mock_kafka = MagicMock()
        producer._producer = mock_kafka

        tx_json = '{"transaction_id": "test-123", "amount": 100.0}'
        producer.send(tx_json, key="test-123")

        mock_kafka.produce.assert_called_once()

    def test_producer_send_uses_topic(self) -> None:
        """send() must produce to the configured topic."""
        from generator.src.kafka.producer import TransactionProducer

        producer = TransactionProducer(
            bootstrap_servers="localhost:9092",
            topic="my-topic",
        )
        mock_kafka = MagicMock()
        producer._producer = mock_kafka

        producer.send('{"test": true}', key="k1")

        call_kwargs = mock_kafka.produce.call_args
        assert call_kwargs[1].get("topic") == "my-topic" or call_kwargs[0][0] == "my-topic"


class TestCircuitBreaker:
    """Test circuit breaker pattern for Kafka producer."""

    def test_circuit_breaker_opens_after_consecutive_failures(self) -> None:
        """Circuit breaker must OPEN after 10 consecutive delivery failures."""
        from generator.src.kafka.producer import TransactionProducer

        producer = TransactionProducer(
            bootstrap_servers="localhost:9092",
            topic="raw-transactions",
        )

        # Simulate 10 consecutive failures
        for _ in range(10):
            producer._on_delivery_failure()

        assert producer.circuit_open is True, (
            "Circuit breaker should open after 10 consecutive failures"
        )

    def test_circuit_breaker_resets_on_success(self) -> None:
        """A successful delivery must reset the failure counter."""
        from generator.src.kafka.producer import TransactionProducer

        producer = TransactionProducer(
            bootstrap_servers="localhost:9092",
            topic="raw-transactions",
        )

        # Simulate some failures
        for _ in range(5):
            producer._on_delivery_failure()

        # Then a success
        producer._on_delivery_success()

        assert producer.consecutive_failures == 0
        assert producer.circuit_open is False

    def test_circuit_breaker_buffers_when_open(self) -> None:
        """When circuit is open, send() must buffer messages instead of producing."""
        from generator.src.kafka.producer import TransactionProducer

        producer = TransactionProducer(
            bootstrap_servers="localhost:9092",
            topic="raw-transactions",
        )
        mock_kafka = MagicMock()
        producer._producer = mock_kafka

        # Open the circuit
        for _ in range(10):
            producer._on_delivery_failure()

        assert producer.circuit_open is True

        # Send should buffer, not produce
        producer.send('{"test": true}', key="k1")
        assert producer.buffer_size > 0

    def test_buffer_max_size_10k(self) -> None:
        """Buffer must not exceed 10,000 messages (deque maxlen)."""
        from generator.src.kafka.producer import TransactionProducer

        producer = TransactionProducer(
            bootstrap_servers="localhost:9092",
            topic="raw-transactions",
        )

        # Open circuit
        for _ in range(10):
            producer._on_delivery_failure()

        # Buffer more than 10K messages
        for i in range(11000):
            producer.send(f'{{"id": {i}}}', key=f"k{i}")

        assert producer.buffer_size <= 10000, (
            f"Buffer size {producer.buffer_size} exceeds max 10,000"
        )


class TestDeliveryCallbacks:
    """Test delivery confirmation callbacks."""

    def test_delivery_callback_success_increments_counter(self) -> None:
        """Successful delivery must increment the success counter."""
        from generator.src.kafka.producer import TransactionProducer

        producer = TransactionProducer(
            bootstrap_servers="localhost:9092",
            topic="raw-transactions",
        )
        initial = producer.delivery_success_count
        producer._on_delivery_success()
        assert producer.delivery_success_count == initial + 1

    def test_delivery_callback_failure_increments_counter(self) -> None:
        """Failed delivery must increment the failure counter."""
        from generator.src.kafka.producer import TransactionProducer

        producer = TransactionProducer(
            bootstrap_servers="localhost:9092",
            topic="raw-transactions",
        )
        initial = producer.consecutive_failures
        producer._on_delivery_failure()
        assert producer.consecutive_failures == initial + 1


class TestBufferDrain:
    """Test buffer drain when circuit breaker closes."""

    def test_drain_buffer_sends_buffered_messages(self) -> None:
        """When circuit closes, buffered messages must be sent."""
        from generator.src.kafka.producer import TransactionProducer

        producer = TransactionProducer(
            bootstrap_servers="localhost:9092",
            topic="raw-transactions",
        )
        mock_kafka = MagicMock()
        producer._producer = mock_kafka

        # Open circuit and buffer messages
        for _ in range(10):
            producer._on_delivery_failure()

        producer.send('{"msg": 1}', key="k1")
        producer.send('{"msg": 2}', key="k2")
        assert producer.buffer_size == 2

        # Close circuit and drain
        producer._on_delivery_success()  # Resets failures
        producer.circuit_open = False
        producer.drain_buffer()

        assert producer.buffer_size == 0
        assert mock_kafka.produce.call_count >= 2

    def test_flush_calls_kafka_flush(self) -> None:
        """flush() must call the underlying producer's flush()."""
        from generator.src.kafka.producer import TransactionProducer

        producer = TransactionProducer(
            bootstrap_servers="localhost:9092",
            topic="raw-transactions",
        )
        mock_kafka = MagicMock()
        producer._producer = mock_kafka

        producer.flush(timeout=5.0)
        mock_kafka.flush.assert_called_once_with(timeout=5.0)

    def test_poll_calls_kafka_poll(self) -> None:
        """poll() must call the underlying producer's poll()."""
        from generator.src.kafka.producer import TransactionProducer

        producer = TransactionProducer(
            bootstrap_servers="localhost:9092",
            topic="raw-transactions",
        )
        mock_kafka = MagicMock()
        producer._producer = mock_kafka

        producer.poll(timeout=0.0)
        mock_kafka.poll.assert_called_once_with(0.0)
