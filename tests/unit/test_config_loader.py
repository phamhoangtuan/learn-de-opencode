"""T024: Unit test — config_loader YAML loading, watch/reload, validation.

Per Constitution v2.0.0 Principle III: Tests written FIRST, must FAIL before implementation.
Per spec.md FR-002/FR-016: Configurable rate with hot reload.
"""

import tempfile
import time
from pathlib import Path

import pytest
import yaml


class TestConfigLoader:
    """Test YAML config loading and validation."""

    def test_load_config_returns_dict(self, config_dir: Path) -> None:
        """load_config must return a dictionary from generator.yaml."""
        from generator.src.lib.config_loader import load_config

        config = load_config(config_dir / "generator.yaml")
        assert isinstance(config, dict)

    def test_load_config_has_required_sections(self, config_dir: Path) -> None:
        """Config must contain generator, kafka, currencies, merchant_categories, transaction_types."""
        from generator.src.lib.config_loader import load_config

        config = load_config(config_dir / "generator.yaml")
        assert "generator" in config
        assert "kafka" in config
        assert "currencies" in config
        assert "merchant_categories" in config
        assert "transaction_types" in config

    def test_load_config_generator_section(self, config_dir: Path) -> None:
        """generator section must have rate, fraud_percentage, account_pool_size."""
        from generator.src.lib.config_loader import load_config

        config = load_config(config_dir / "generator.yaml")
        gen = config["generator"]
        assert "rate" in gen
        assert "fraud_percentage" in gen
        assert "account_pool_size" in gen
        assert gen["rate"] == 10
        assert gen["fraud_percentage"] == 3
        assert gen["account_pool_size"] == 1000

    def test_load_config_validates_rate_range(self) -> None:
        """Rate must be between 1 and 1000 per spec.md FR-002."""
        from generator.src.lib.config_loader import load_config

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"generator": {"rate": 0, "fraud_percentage": 3, "account_pool_size": 1000}}, f)
            f.flush()
            with pytest.raises(ValueError, match="[Rr]ate"):
                load_config(Path(f.name))

    def test_load_config_validates_fraud_percentage(self) -> None:
        """fraud_percentage must be between 0 and 100."""
        from generator.src.lib.config_loader import load_config

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"generator": {"rate": 10, "fraud_percentage": -1, "account_pool_size": 1000}}, f)
            f.flush()
            with pytest.raises(ValueError, match="[Ff]raud"):
                load_config(Path(f.name))

    def test_load_config_missing_file_raises(self) -> None:
        """Loading a non-existent file must raise FileNotFoundError."""
        from generator.src.lib.config_loader import load_config

        with pytest.raises(FileNotFoundError):
            load_config(Path("/nonexistent/config.yaml"))

    def test_currency_weights_sum_to_one(self, config_dir: Path) -> None:
        """Currency weights must sum to approximately 1.0."""
        from generator.src.lib.config_loader import load_config

        config = load_config(config_dir / "generator.yaml")
        total = sum(config["currencies"].values())
        assert abs(total - 1.0) < 0.01, f"Currency weights sum to {total}, expected ~1.0"

    def test_merchant_category_weights_sum_to_one(self, config_dir: Path) -> None:
        """Merchant category weights must sum to approximately 1.0."""
        from generator.src.lib.config_loader import load_config

        config = load_config(config_dir / "generator.yaml")
        total = sum(config["merchant_categories"].values())
        assert abs(total - 1.0) < 0.01, f"Category weights sum to {total}, expected ~1.0"

    def test_transaction_type_weights_sum_to_one(self, config_dir: Path) -> None:
        """Transaction type weights must sum to approximately 1.0."""
        from generator.src.lib.config_loader import load_config

        config = load_config(config_dir / "generator.yaml")
        total = sum(config["transaction_types"].values())
        assert abs(total - 1.0) < 0.01, f"Type weights sum to {total}, expected ~1.0"


class TestConfigWatcher:
    """Test config file watching and hot reload."""

    def test_config_watcher_detects_change(self) -> None:
        """ConfigWatcher must detect file modification within 5 seconds."""
        from generator.src.lib.config_loader import ConfigWatcher

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"generator": {"rate": 10, "fraud_percentage": 3, "account_pool_size": 1000}}, f)
            f.flush()
            path = Path(f.name)

        watcher = ConfigWatcher(path, poll_interval=0.1)
        assert watcher.has_changed() is False  # No change yet

        # Modify the file
        time.sleep(0.2)
        with open(path, "w") as f:
            yaml.dump({"generator": {"rate": 20, "fraud_percentage": 3, "account_pool_size": 1000}}, f)

        # Should detect the change
        time.sleep(0.2)
        assert watcher.has_changed() is True

    def test_config_watcher_reload_returns_new_config(self) -> None:
        """After file change, reload must return the updated config."""
        from generator.src.lib.config_loader import ConfigWatcher, load_config

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({
                "generator": {"rate": 10, "fraud_percentage": 3, "account_pool_size": 1000},
                "kafka": {"bootstrap_servers": "kafka:29092", "topic": "raw-transactions"},
                "currencies": {"USD": 1.0},
                "merchant_categories": {"retail": 1.0},
                "transaction_types": {"purchase": 1.0},
            }, f)
            f.flush()
            path = Path(f.name)

        watcher = ConfigWatcher(path, poll_interval=0.1)
        config1 = watcher.get_config()
        assert config1["generator"]["rate"] == 10

        # Modify
        time.sleep(0.2)
        with open(path, "w") as f:
            yaml.dump({
                "generator": {"rate": 50, "fraud_percentage": 5, "account_pool_size": 1000},
                "kafka": {"bootstrap_servers": "kafka:29092", "topic": "raw-transactions"},
                "currencies": {"USD": 1.0},
                "merchant_categories": {"retail": 1.0},
                "transaction_types": {"purchase": 1.0},
            }, f)

        time.sleep(0.2)
        assert watcher.has_changed() is True
        config2 = watcher.get_config()
        assert config2["generator"]["rate"] == 50

    def test_config_watcher_no_change_returns_cached(self) -> None:
        """If file hasn't changed, get_config returns cached version."""
        from generator.src.lib.config_loader import ConfigWatcher

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({
                "generator": {"rate": 10, "fraud_percentage": 3, "account_pool_size": 1000},
                "kafka": {"bootstrap_servers": "kafka:29092", "topic": "raw-transactions"},
                "currencies": {"USD": 1.0},
                "merchant_categories": {"retail": 1.0},
                "transaction_types": {"purchase": 1.0},
            }, f)
            f.flush()
            path = Path(f.name)

        watcher = ConfigWatcher(path, poll_interval=0.1)
        config1 = watcher.get_config()
        config2 = watcher.get_config()
        assert config1 is config2  # Same object — cached
