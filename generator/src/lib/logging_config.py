"""Structured JSON logging configuration for the data generator.

Provides a consistent logging setup with JSON-formatted output for Docker
container log aggregation and structured log analysis.

Per Constitution v2.0.0 Principle IV (Metadata & Lineage):
- All pipeline operations logged with timestamps and component names
- Correlation IDs via transaction_id where applicable
- JSON format for machine-parseable log entries
"""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


class JSONFormatter(logging.Formatter):
    """Format log records as JSON for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as a JSON string.

        Args:
            record: The log record to format.

        Returns:
            JSON-formatted string with standard fields.
        """
        log_entry: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "component": "generator",
        }

        # Add extra fields if present (e.g., correlation IDs)
        if hasattr(record, "transaction_id"):
            log_entry["transaction_id"] = record.transaction_id
        if hasattr(record, "session_id"):
            log_entry["session_id"] = record.session_id
        if hasattr(record, "extra_data"):
            log_entry["data"] = record.extra_data

        # Add exception info if present
        if record.exc_info and record.exc_info[1]:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]),
            }

        return json.dumps(log_entry, default=str)


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configure structured JSON logging for the generator.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).

    Returns:
        Configured root logger for the generator component.
    """
    logger = logging.getLogger("generator")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()

    # JSON handler to stdout (for Docker log collection)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    logger.addHandler(handler)

    # Prevent propagation to root logger
    logger.propagate = False

    return logger


def get_logger(name: str) -> logging.Logger:
    """Get a child logger under the generator namespace.

    Args:
        name: The module name for the child logger.

    Returns:
        A child logger that inherits the generator's configuration.
    """
    return logging.getLogger(f"generator.{name}")
