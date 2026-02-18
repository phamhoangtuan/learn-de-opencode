"""Shared test fixtures for the synthetic financial data generator."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def default_seed() -> int:
    """Provide a deterministic seed for reproducible tests."""
    return 42


@pytest.fixture
def small_count() -> int:
    """Provide a small record count for fast test execution."""
    return 100


@pytest.fixture
def tmp_output_dir(tmp_path: Path) -> Path:
    """Provide a temporary output directory for generated files.

    Args:
        tmp_path: Pytest built-in temporary directory fixture.

    Returns:
        Path to a temporary 'raw' output directory.
    """
    output_dir = tmp_path / "data" / "raw"
    output_dir.mkdir(parents=True)
    return output_dir
