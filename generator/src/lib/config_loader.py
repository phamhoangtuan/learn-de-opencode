"""YAML config loader with file watching and hot reload.

Loads generator configuration from YAML, validates required fields,
and supports 5-second file-watch-based hot reload.

Per spec.md FR-002: Rate configurable 1-1000 tx/sec.
Per spec.md FR-016: Config changes without pipeline restart.
"""

import os
import time
from pathlib import Path
from typing import Any

import yaml

from generator.src.lib.logging_config import get_logger

logger = get_logger("config_loader")


def load_config(config_path: Path) -> dict[str, Any]:
    """Load and validate configuration from a YAML file.

    Args:
        config_path: Path to the YAML configuration file.

    Returns:
        Parsed configuration dictionary.

    Raises:
        FileNotFoundError: If config file does not exist.
        ValueError: If config values are out of valid range.
    """
    if not config_path.exists():
        msg = f"Config file not found: {config_path}"
        raise FileNotFoundError(msg)

    with open(config_path) as f:
        config = yaml.safe_load(f)

    _validate_config(config)
    return config


def _validate_config(config: dict[str, Any]) -> None:
    """Validate configuration values.

    Args:
        config: Configuration dictionary to validate.

    Raises:
        ValueError: If any config value is invalid.
    """
    gen = config.get("generator", {})

    rate = gen.get("rate", 10)
    if not isinstance(rate, int) or rate < 1 or rate > 1000:
        msg = f"Rate must be between 1 and 1000, got {rate}"
        raise ValueError(msg)

    fraud_pct = gen.get("fraud_percentage", 3)
    if not isinstance(fraud_pct, (int, float)) or fraud_pct < 0 or fraud_pct > 100:
        msg = f"Fraud percentage must be between 0 and 100, got {fraud_pct}"
        raise ValueError(msg)


class ConfigWatcher:
    """Watch a config file for changes and reload when modified.

    Uses file modification time to detect changes, polling at
    a configurable interval (default 5 seconds).

    Attributes:
        path: Path to the config file being watched.
        poll_interval: Seconds between file stat checks.
    """

    def __init__(self, path: Path, poll_interval: float = 5.0) -> None:
        """Initialize the config watcher.

        Args:
            path: Path to the config file to watch.
            poll_interval: Seconds between modification time checks.
        """
        self.path = path
        self.poll_interval = poll_interval
        self._last_mtime: float = self._get_mtime()
        self._cached_config: dict[str, Any] | None = None

    def _get_mtime(self) -> float:
        """Get the file's modification time."""
        try:
            return os.path.getmtime(self.path)
        except OSError:
            return 0.0

    def has_changed(self) -> bool:
        """Check if the config file has been modified since last check.

        Returns:
            True if the file modification time has changed.
        """
        current_mtime = self._get_mtime()
        return current_mtime != self._last_mtime

    def get_config(self) -> dict[str, Any]:
        """Get the current configuration, reloading if file changed.

        Returns:
            Current configuration dictionary (cached if unchanged).
        """
        if self._cached_config is None or self.has_changed():
            self._cached_config = load_config(self.path)
            self._last_mtime = self._get_mtime()
            logger.info("Config reloaded from %s", self.path)

        return self._cached_config
