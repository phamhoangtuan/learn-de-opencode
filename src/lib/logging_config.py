"""Structured JSON logging configuration for the transaction generator."""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime


@dataclass
class GenerationMetadata:
    """Metadata about a generation run, output as JSON to stdout per FR-009.

    Attributes:
        records_generated: Number of transaction records produced.
        seed: Random seed used for this run.
        start_date: Start of the transaction date range (YYYY-MM-DD).
        end_date: End of the transaction date range (YYYY-MM-DD).
        accounts: Number of unique accounts generated.
        format: Output file format (parquet or csv).
        output_path: Path to the generated output file.
        duration_seconds: Wall-clock time for generation in seconds.
    """

    records_generated: int
    seed: int
    start_date: str
    end_date: str
    accounts: int
    format: str
    output_path: str
    duration_seconds: float
    timestamp: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def to_json(self) -> str:
        """Serialize metadata to a JSON string.

        Returns:
            JSON string representation of the metadata.
        """
        return json.dumps(asdict(self), indent=2)


def setup_logging(*, level: int = logging.INFO) -> logging.Logger:
    """Configure and return a logger for the transaction generator.

    Sets up a stream handler writing to stderr so that stdout remains
    reserved for the JSON metadata output (FR-009).

    Args:
        level: Logging level. Defaults to INFO.

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger("generate_transactions")
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(level)
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
