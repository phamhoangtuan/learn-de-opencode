"""Shared test fixtures for the streaming financial pipeline.

Provides reusable fixtures for unit, integration, and contract tests.
Per Constitution v2.0.0 Principle III: Test-First Development.
"""

import json
import os
from pathlib import Path

import pytest


# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent


@pytest.fixture
def project_root() -> Path:
    """Return the project root directory."""
    return PROJECT_ROOT


@pytest.fixture
def contracts_dir() -> Path:
    """Return the path to JSON Schema contract files."""
    return PROJECT_ROOT / "specs" / "001-streaming-financial-pipeline" / "contracts"


@pytest.fixture
def config_dir() -> Path:
    """Return the path to configuration files."""
    return PROJECT_ROOT / "config"


@pytest.fixture
def raw_transaction_schema(contracts_dir: Path) -> dict:
    """Load the raw-transactions JSON Schema contract."""
    schema_path = contracts_dir / "raw-transactions.schema.json"
    with open(schema_path) as f:
        return json.load(f)


@pytest.fixture
def alert_schema(contracts_dir: Path) -> dict:
    """Load the alerts JSON Schema contract."""
    schema_path = contracts_dir / "alerts.schema.json"
    with open(schema_path) as f:
        return json.load(f)


@pytest.fixture
def dead_letter_schema(contracts_dir: Path) -> dict:
    """Load the dead-letter JSON Schema contract."""
    schema_path = contracts_dir / "dead-letter.schema.json"
    with open(schema_path) as f:
        return json.load(f)


@pytest.fixture
def sample_valid_transaction() -> dict:
    """Return a sample valid transaction matching the raw-transactions contract."""
    return {
        "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
        "timestamp": "2026-02-16T14:30:00.000Z",
        "account_id": "ACC123456789",
        "amount": 125.50,
        "currency": "USD",
        "merchant_name": "Amazon",
        "merchant_category": "online",
        "transaction_type": "purchase",
        "location_country": "US",
        "status": "completed",
    }


@pytest.fixture
def sample_high_value_transaction(sample_valid_transaction: dict) -> dict:
    """Return a transaction that should trigger the high-value alert rule."""
    return {**sample_valid_transaction, "amount": 15000.00}


@pytest.fixture
def sample_unusual_hour_transaction(sample_valid_transaction: dict) -> dict:
    """Return a transaction during quiet hours (01:00-05:00 UTC)."""
    return {
        **sample_valid_transaction,
        "timestamp": "2026-02-16T02:30:00.000Z",
    }


@pytest.fixture
def generator_config(config_dir: Path) -> dict:
    """Load the generator configuration YAML."""
    import yaml

    config_path = config_dir / "generator.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)
