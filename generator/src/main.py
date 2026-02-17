"""Main entry point for the financial transaction data generator.

Initializes account pool, loads config, starts rate-limited generation loop.
Handles SIGTERM for graceful shutdown (flush Kafka buffer, log final stats).

Per spec.md FR-002: Configurable rate with hot reload.
Per spec.md US1: Generate and stream financial transactions.
"""

import signal
import sys
import time
import uuid
from pathlib import Path

from generator.src.generators.fraud_injector import maybe_inject_fraud
from generator.src.generators.transaction_generator import generate_transaction
from generator.src.kafka.producer import TransactionProducer
from generator.src.lib.config_loader import ConfigWatcher, load_config
from generator.src.lib.logging_config import setup_logging
from generator.src.lib.rate_limiter import RateLimiter
from generator.src.models.account import create_account_pool

# Default config path (mounted via Docker volume)
_DEFAULT_CONFIG_PATH = Path("/app/config/generator.yaml")
_LOCAL_CONFIG_PATH = Path("config/generator.yaml")

# Session tracking
_SESSION_ID = str(uuid.uuid4())


def _get_config_path() -> Path:
    """Resolve config file path, preferring Docker mount."""
    if _DEFAULT_CONFIG_PATH.exists():
        return _DEFAULT_CONFIG_PATH
    if _LOCAL_CONFIG_PATH.exists():
        return _LOCAL_CONFIG_PATH
    msg = "No config file found at /app/config/generator.yaml or config/generator.yaml"
    raise FileNotFoundError(msg)


def main() -> None:
    """Run the transaction generator main loop."""
    logger = setup_logging(level="INFO")
    logger.info(
        "Starting data generator",
        extra={"session_id": _SESSION_ID},
    )

    # Load configuration
    config_path = _get_config_path()
    config = load_config(config_path)
    watcher = ConfigWatcher(config_path, poll_interval=5.0)

    gen_config = config["generator"]
    kafka_config = config["kafka"]

    # Initialize account pool
    pool_size = gen_config.get("account_pool_size", 1000)
    logger.info("Creating account pool with %d accounts", pool_size)
    account_pool = create_account_pool(size=pool_size)

    # Initialize rate limiter
    rate = gen_config["rate"]
    limiter = RateLimiter(rate=rate)
    logger.info("Rate limiter initialized at %d tx/sec", rate)

    # Initialize Kafka producer
    producer = TransactionProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        topic=kafka_config["topic"],
    )
    logger.info(
        "Kafka producer initialized: %s -> %s",
        kafka_config["bootstrap_servers"],
        kafka_config["topic"],
    )

    # Fraud injection config
    fraud_percentage = gen_config.get("fraud_percentage", 3)

    # Stats tracking
    total_generated = 0
    total_fraudulent = 0
    start_time = time.monotonic()
    last_stats_time = start_time

    # Graceful shutdown handler
    running = True

    def _shutdown_handler(signum: int, frame: object) -> None:
        nonlocal running
        logger.info("Received shutdown signal %d, flushing...", signum)
        running = False

    signal.signal(signal.SIGTERM, _shutdown_handler)
    signal.signal(signal.SIGINT, _shutdown_handler)

    logger.info("Generator started — producing to '%s'", kafka_config["topic"])

    try:
        while running:
            # Check for config changes
            if watcher.has_changed():
                new_config = watcher.get_config()
                new_gen = new_config["generator"]
                new_rate = new_gen["rate"]
                if new_rate != rate:
                    limiter.update_rate(new_rate)
                    rate = new_rate
                    logger.info("Rate updated to %d tx/sec", rate)
                fraud_percentage = new_gen.get("fraud_percentage", fraud_percentage)

            # Rate-limited generation
            if limiter.acquire():
                # Generate transaction
                tx = generate_transaction(account_pool)

                # Maybe inject fraud
                result = maybe_inject_fraud(tx, fraud_percentage=fraud_percentage)
                tx = result.transaction

                # Produce to Kafka
                producer.send(tx.to_json(), key=tx.transaction_id)
                producer.poll(timeout=0.0)

                total_generated += 1
                if result.is_fraudulent:
                    total_fraudulent += 1
            else:
                # No token available — wait briefly
                time.sleep(0.001)

            # Log stats every 30 seconds
            now = time.monotonic()
            if now - last_stats_time >= 30.0:
                elapsed = now - start_time
                actual_rate = total_generated / elapsed if elapsed > 0 else 0
                logger.info(
                    "Stats: %d generated, %d fraudulent, %.1f tx/sec actual, %d buffered",
                    total_generated,
                    total_fraudulent,
                    actual_rate,
                    producer.buffer_size,
                    extra={
                        "session_id": _SESSION_ID,
                        "extra_data": {
                            "total_generated": total_generated,
                            "total_fraudulent": total_fraudulent,
                            "actual_rate": round(actual_rate, 2),
                            "buffer_size": producer.buffer_size,
                            "delivery_success": producer.delivery_success_count,
                        },
                    },
                )
                last_stats_time = now

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    finally:
        # Graceful shutdown
        elapsed = time.monotonic() - start_time
        actual_rate = total_generated / elapsed if elapsed > 0 else 0

        logger.info(
            "Shutting down: %d total generated, %d fraudulent, %.1f tx/sec avg, %.1fs runtime",
            total_generated,
            total_fraudulent,
            actual_rate,
            elapsed,
            extra={
                "session_id": _SESSION_ID,
                "extra_data": {
                    "total_generated": total_generated,
                    "total_fraudulent": total_fraudulent,
                    "actual_rate": round(actual_rate, 2),
                    "runtime_seconds": round(elapsed, 1),
                    "delivery_success": producer.delivery_success_count,
                },
            },
        )
        producer.flush(timeout=10.0)
        logger.info("Generator shutdown complete")


if __name__ == "__main__":
    main()
