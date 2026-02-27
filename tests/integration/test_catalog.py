"""Integration tests for the data catalog builder.

Tests end-to-end catalog build against a populated DuckDB warehouse,
including YAML enrichment, idempotency, and zone correctness.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from src.catalog.models import CatalogStatus
from src.catalog.runner import build_catalog


@pytest.fixture
def catalog_warehouse(tmp_path: Path) -> Path:
    """Create a DuckDB warehouse with representative tables for catalog testing.

    Returns:
        Path to the temporary DuckDB file.
    """
    db_path = tmp_path / "warehouse" / "test.duckdb"
    db_path.parent.mkdir(parents=True)
    conn = duckdb.connect(str(db_path))

    now = datetime.now(UTC)

    # Raw: transactions table
    conn.execute("""
        CREATE TABLE transactions (
            transaction_id    VARCHAR,
            "timestamp"       TIMESTAMPTZ,
            transaction_date  DATE,
            amount            DOUBLE,
            currency          VARCHAR,
            account_id        VARCHAR,
            transaction_type  VARCHAR,
            status            VARCHAR,
            source_file       VARCHAR,
            ingested_at       TIMESTAMPTZ,
            run_id            VARCHAR
        )
    """)
    conn.execute(
        """
        INSERT INTO transactions VALUES
            ('tx-001', ?::TIMESTAMPTZ, '2026-02-18', 42.50, 'USD',
             'ACC-001', 'debit', 'completed', 'file1.parquet', ?::TIMESTAMPTZ, 'run-001'),
            ('tx-002', ?::TIMESTAMPTZ, '2026-02-18', 125.00, 'EUR',
             'ACC-002', 'debit', 'completed', 'file1.parquet', ?::TIMESTAMPTZ, 'run-001')
        """,
        [now, now, now, now],
    )

    # Staging: view
    conn.execute("""
        CREATE OR REPLACE VIEW stg_transactions AS
        SELECT
            transaction_id,
            "timestamp" AS transaction_timestamp,
            transaction_date,
            amount,
            currency,
            account_id,
            transaction_type,
            status,
            source_file,
            ingested_at,
            run_id
        FROM transactions
    """)

    # Mart: table
    conn.execute("""
        CREATE TABLE daily_spend_by_category AS
        SELECT
            transaction_date, 'Food' AS category, 'USD' AS currency,
            100.0 AS total_amount, 2 AS transaction_count, 50.0 AS avg_amount
        FROM (SELECT DATE '2026-02-18' AS transaction_date)
    """)

    conn.close()
    return db_path


@pytest.fixture
def catalog_yaml(tmp_path: Path) -> Path:
    """Create a minimal YAML metadata file."""
    yaml_path = tmp_path / "catalog.yaml"
    yaml_path.write_text(
        "tables:\n"
        "  transactions:\n"
        "    zone: raw\n"
        "    description: 'Raw financial transactions from Parquet.'\n"
        "    owner: 'data-engineering'\n"
        "    columns:\n"
        "      transaction_id:\n"
        "        description: 'Primary key UUID'\n"
        "        is_pk: true\n"
        "  stg_transactions:\n"
        "    zone: staging\n"
        "    description: 'Staging view over transactions.'\n"
        "  daily_spend_by_category:\n"
        "    zone: mart\n"
        "    description: 'Daily spend aggregation.'\n"
    )
    return yaml_path


class TestCatalogBuild:
    """End-to-end tests for build_catalog."""

    def test_catalog_tables_populated(
        self, catalog_warehouse: Path, catalog_yaml: Path
    ) -> None:
        result = build_catalog(
            db_path=catalog_warehouse, yaml_path=catalog_yaml
        )
        assert result.status == CatalogStatus.COMPLETED
        # 3 user tables + 2 catalog tables = 5
        assert result.tables_catalogued >= 3

    def test_catalog_columns_populated(
        self, catalog_warehouse: Path, catalog_yaml: Path
    ) -> None:
        result = build_catalog(
            db_path=catalog_warehouse, yaml_path=catalog_yaml
        )
        assert result.columns_catalogued > 0

    def test_all_tables_have_zone(
        self, catalog_warehouse: Path, catalog_yaml: Path
    ) -> None:
        build_catalog(db_path=catalog_warehouse, yaml_path=catalog_yaml)
        conn = duckdb.connect(str(catalog_warehouse))
        try:
            nulls = conn.execute(
                "SELECT COUNT(*) FROM catalog_tables WHERE zone IS NULL OR zone = ''"
            ).fetchone()
            assert nulls is not None
            assert nulls[0] == 0
        finally:
            conn.close()

    def test_views_have_null_row_count(
        self, catalog_warehouse: Path, catalog_yaml: Path
    ) -> None:
        build_catalog(db_path=catalog_warehouse, yaml_path=catalog_yaml)
        conn = duckdb.connect(str(catalog_warehouse))
        try:
            row = conn.execute(
                "SELECT row_count FROM catalog_tables "
                "WHERE table_name = 'stg_transactions'"
            ).fetchone()
            assert row is not None
            assert row[0] is None
        finally:
            conn.close()

    def test_base_tables_have_row_count(
        self, catalog_warehouse: Path, catalog_yaml: Path
    ) -> None:
        build_catalog(db_path=catalog_warehouse, yaml_path=catalog_yaml)
        conn = duckdb.connect(str(catalog_warehouse))
        try:
            row = conn.execute(
                "SELECT row_count FROM catalog_tables "
                "WHERE table_name = 'transactions'"
            ).fetchone()
            assert row is not None
            assert row[0] == 2
        finally:
            conn.close()

    def test_elapsed_under_10_seconds(
        self, catalog_warehouse: Path, catalog_yaml: Path
    ) -> None:
        result = build_catalog(
            db_path=catalog_warehouse, yaml_path=catalog_yaml
        )
        assert result.elapsed_seconds < 10.0

    def test_missing_database(self, tmp_path: Path) -> None:
        result = build_catalog(
            db_path=tmp_path / "nope.duckdb",
            yaml_path=tmp_path / "nope.yaml",
        )
        assert result.status == CatalogStatus.FAILED
        assert "not found" in (result.error_message or "").lower()


class TestCatalogIdempotency:
    """Verify catalog build is idempotent."""

    def test_second_run_same_table_count(
        self, catalog_warehouse: Path, catalog_yaml: Path
    ) -> None:
        r1 = build_catalog(db_path=catalog_warehouse, yaml_path=catalog_yaml)
        r2 = build_catalog(db_path=catalog_warehouse, yaml_path=catalog_yaml)
        assert r1.tables_catalogued == r2.tables_catalogued

    def test_second_run_same_column_count(
        self, catalog_warehouse: Path, catalog_yaml: Path
    ) -> None:
        r1 = build_catalog(db_path=catalog_warehouse, yaml_path=catalog_yaml)
        r2 = build_catalog(db_path=catalog_warehouse, yaml_path=catalog_yaml)
        assert r1.columns_catalogued == r2.columns_catalogued


class TestCatalogWithYaml:
    """Tests for YAML metadata enrichment."""

    def test_yaml_description_in_catalog(
        self, catalog_warehouse: Path, catalog_yaml: Path
    ) -> None:
        build_catalog(db_path=catalog_warehouse, yaml_path=catalog_yaml)
        conn = duckdb.connect(str(catalog_warehouse))
        try:
            row = conn.execute(
                "SELECT description FROM catalog_tables "
                "WHERE table_name = 'transactions'"
            ).fetchone()
            assert row is not None
            assert "Raw financial transactions" in row[0]
        finally:
            conn.close()

    def test_yaml_zone_override(
        self, catalog_warehouse: Path, catalog_yaml: Path
    ) -> None:
        build_catalog(db_path=catalog_warehouse, yaml_path=catalog_yaml)
        conn = duckdb.connect(str(catalog_warehouse))
        try:
            row = conn.execute(
                "SELECT zone FROM catalog_tables "
                "WHERE table_name = 'transactions'"
            ).fetchone()
            assert row is not None
            assert row[0] == "raw"
        finally:
            conn.close()

    def test_yaml_column_is_pk(
        self, catalog_warehouse: Path, catalog_yaml: Path
    ) -> None:
        build_catalog(db_path=catalog_warehouse, yaml_path=catalog_yaml)
        conn = duckdb.connect(str(catalog_warehouse))
        try:
            row = conn.execute(
                "SELECT is_pk FROM catalog_columns "
                "WHERE table_name = 'transactions' AND column_name = 'transaction_id'"
            ).fetchone()
            assert row is not None
            assert row[0] is True
        finally:
            conn.close()

    def test_yaml_column_description(
        self, catalog_warehouse: Path, catalog_yaml: Path
    ) -> None:
        build_catalog(db_path=catalog_warehouse, yaml_path=catalog_yaml)
        conn = duckdb.connect(str(catalog_warehouse))
        try:
            row = conn.execute(
                "SELECT description FROM catalog_columns "
                "WHERE table_name = 'transactions' AND column_name = 'transaction_id'"
            ).fetchone()
            assert row is not None
            assert "Primary key" in row[0]
        finally:
            conn.close()

    def test_yaml_owner_in_catalog(
        self, catalog_warehouse: Path, catalog_yaml: Path
    ) -> None:
        build_catalog(db_path=catalog_warehouse, yaml_path=catalog_yaml)
        conn = duckdb.connect(str(catalog_warehouse))
        try:
            row = conn.execute(
                "SELECT owner FROM catalog_tables "
                "WHERE table_name = 'transactions'"
            ).fetchone()
            assert row is not None
            assert row[0] == "data-engineering"
        finally:
            conn.close()
