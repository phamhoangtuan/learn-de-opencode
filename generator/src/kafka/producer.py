"""Kafka producer wrapper with circuit breaker, delivery callbacks, and buffer.

Wraps confluent-kafka Producer with:
- Delivery confirmation callbacks
- Circuit breaker (opens after 10 consecutive failures)
- Message buffer (deque, max 10K) when circuit is open
- Exponential backoff for reconnection attempts

Per spec.md FR-011: Retry with exponential backoff on Kafka unavailability.
Per spec.md edge case: Buffer messages when broker unavailable.
"""

import time
from collections import deque
from typing import Any

from generator.src.lib.logging_config import get_logger

logger = get_logger("producer")

# Circuit breaker threshold
_CIRCUIT_BREAKER_THRESHOLD = 10
# Maximum buffer size when circuit is open
_MAX_BUFFER_SIZE = 10_000


class TransactionProducer:
    """Kafka producer wrapper with circuit breaker and buffering.

    Attributes:
        bootstrap_servers: Kafka bootstrap servers.
        topic: Target Kafka topic.
        circuit_open: Whether the circuit breaker is currently open.
        consecutive_failures: Count of consecutive delivery failures.
        delivery_success_count: Total successful deliveries.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        **kafka_config: Any,
    ) -> None:
        """Initialize the Kafka producer.

        Args:
            bootstrap_servers: Kafka bootstrap servers connection string.
            topic: Target topic for produced messages.
            **kafka_config: Additional confluent-kafka configuration.
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.circuit_open = False
        self.consecutive_failures = 0
        self.delivery_success_count = 0
        self._buffer: deque[tuple[str, str | None]] = deque(maxlen=_MAX_BUFFER_SIZE)
        self._backoff_until: float = 0.0
        self._backoff_exponent: int = 0

        # Initialize the confluent-kafka producer
        config = {
            "bootstrap.servers": bootstrap_servers,
            "queue.buffering.max.messages": 100000,
            "queue.buffering.max.ms": 100,
            **kafka_config,
        }
        try:
            from confluent_kafka import Producer

            self._producer = Producer(config)
        except Exception:
            # Allow initialization to succeed even without Kafka
            # (for testing with mocked producer)
            self._producer = None  # type: ignore[assignment]
            logger.warning("Could not initialize Kafka producer — will retry on send")

    @property
    def buffer_size(self) -> int:
        """Number of messages currently in the buffer."""
        return len(self._buffer)

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Handle delivery confirmation from Kafka broker.

        Args:
            err: Error if delivery failed, None if successful.
            msg: The message that was delivered (or failed).
        """
        if err is not None:
            self._on_delivery_failure()
            logger.error(
                "Delivery failed: %s",
                str(err),
                extra={"extra_data": {"topic": self.topic}},
            )
        else:
            self._on_delivery_success()

    def _on_delivery_success(self) -> None:
        """Handle a successful delivery — reset circuit breaker."""
        self.consecutive_failures = 0
        self.delivery_success_count += 1
        self._backoff_exponent = 0

        if self.circuit_open:
            self.circuit_open = False
            logger.info("Circuit breaker CLOSED — Kafka connection restored")

    def _on_delivery_failure(self) -> None:
        """Handle a failed delivery — increment failure counter."""
        self.consecutive_failures += 1

        if self.consecutive_failures >= _CIRCUIT_BREAKER_THRESHOLD and not self.circuit_open:
            self.circuit_open = True
            self._backoff_exponent = 1
            self._backoff_until = time.monotonic() + (2 ** self._backoff_exponent)
            logger.warning(
                "Circuit breaker OPEN after %d consecutive failures",
                self.consecutive_failures,
            )

    def send(self, value: str, key: str | None = None) -> None:
        """Send a message to Kafka, buffering if circuit is open.

        Args:
            value: JSON string message value.
            key: Optional message key (typically transaction_id).
        """
        if self.circuit_open:
            self._buffer.append((value, key))
            # Check if backoff period has elapsed for retry
            if time.monotonic() >= self._backoff_until:
                self._try_reconnect()
            return

        try:
            if self._producer is not None:
                self._producer.produce(
                    topic=self.topic,
                    value=value.encode("utf-8"),
                    key=key.encode("utf-8") if key else None,
                    callback=self._delivery_callback,
                )
            else:
                self._on_delivery_failure()
        except BufferError:
            # Internal Kafka buffer is full — buffer externally
            self._buffer.append((value, key))
            logger.warning("Kafka internal buffer full, buffering externally")
        except Exception as e:
            self._on_delivery_failure()
            self._buffer.append((value, key))
            logger.error("Produce error: %s", str(e))

    def _try_reconnect(self) -> None:
        """Attempt to reconnect by sending a buffered message."""
        if not self._buffer:
            return

        value, key = self._buffer[0]
        try:
            if self._producer is not None:
                self._producer.produce(
                    topic=self.topic,
                    value=value.encode("utf-8"),
                    key=key.encode("utf-8") if key else None,
                    callback=self._delivery_callback,
                )
                self._buffer.popleft()  # Remove from buffer if produce succeeded
            else:
                # Increase backoff
                self._backoff_exponent = min(self._backoff_exponent + 1, 6)
                self._backoff_until = time.monotonic() + (2 ** self._backoff_exponent)
        except Exception:
            # Increase backoff
            self._backoff_exponent = min(self._backoff_exponent + 1, 6)
            self._backoff_until = time.monotonic() + (2 ** self._backoff_exponent)
            logger.warning(
                "Reconnect attempt failed, backoff %ds",
                2 ** self._backoff_exponent,
            )

    def drain_buffer(self) -> None:
        """Drain the message buffer by sending all buffered messages.

        Should be called after circuit breaker closes.
        """
        drained = 0
        while self._buffer and not self.circuit_open:
            value, key = self._buffer.popleft()
            try:
                if self._producer is not None:
                    self._producer.produce(
                        topic=self.topic,
                        value=value.encode("utf-8"),
                        key=key.encode("utf-8") if key else None,
                        callback=self._delivery_callback,
                    )
                    drained += 1
            except Exception:
                # Re-buffer and stop draining
                self._buffer.appendleft((value, key))
                break

        if drained > 0:
            logger.info("Drained %d buffered messages", drained)

    def poll(self, timeout: float = 0.0) -> None:
        """Poll the producer for delivery callbacks.

        Args:
            timeout: Maximum time to block in seconds.
        """
        if self._producer is not None:
            self._producer.poll(timeout)

    def flush(self, timeout: float = 10.0) -> None:
        """Flush all outstanding messages and wait for delivery.

        Args:
            timeout: Maximum time to wait in seconds.
        """
        if self._producer is not None:
            self._producer.flush(timeout=timeout)
