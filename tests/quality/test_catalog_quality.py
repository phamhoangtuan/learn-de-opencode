"""Quality tests for the data catalog framework.

Validates the YAML metadata file structure, DDL schema, and catalog
data integrity constraints.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from src.catalog.parser import VALID_ZONES, load_yaml_metadata
from src.catalog.runner import create_catalog_tables

METADATA_YAML = Path("src/metadata/catalog.yaml")


class TestYamlMetadataQuality:
    """Validate the YAML metadata file structure."""

    def test_cat001_metadata_yaml_is_valid(self) -> None:
        """CAT001: catalog.yaml loads without error."""
        result = load_yaml_metadata(METADATA_YAML)
        assert len(result) > 0, "YAML metadata should have at least one table entry"

    def test_cat002_all_yaml_table_keys_are_strings(self) -> None:
        """CAT002: All table keys in YAML are strings."""
        result = load_yaml_metadata(METADATA_YAML)
        for key in result:
            assert isinstance(key, str), f"Table key {key!r} should be a string"

    def test_cat003_all_zones_are_valid(self) -> None:
        """CAT003: All zone values are one of the six canonical zones."""
        result = load_yaml_metadata(METADATA_YAML)
        for table_name, meta in result.items():
            if meta.zone:
                assert meta.zone in VALID_ZONES, (
                    f"Table '{table_name}' has invalid zone '{meta.zone}'. "
                    f"Valid zones: {VALID_ZONES}"
                )

    def test_cat004_yaml_column_descriptions_are_strings(self) -> None:
        """CAT004: All column descriptions are strings."""
        result = load_yaml_metadata(METADATA_YAML)
        for table_name, meta in result.items():
            for col_name, col_meta in meta.columns.items():
                desc = col_meta.get("description", "")
                assert isinstance(desc, str), (
                    f"Column '{col_name}' in '{table_name}' "
                    f"has non-string description: {type(desc)}"
                )


class TestCatalogDdlQuality:
    """Validate catalog DDL creates expected columns."""

    def test_cat005_catalog_tables_has_expected_columns(self) -> None:
        """CAT005: catalog_tables DDL creates all required columns."""
        conn = duckdb.connect(":memory:")
        create_catalog_tables(conn)

        cols = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'catalog_tables'"
            ).fetchall()
        }
        expected = {
            "table_name",
            "table_schema",
            "table_type",
            "zone",
            "description",
            "owner",
            "source_sql_file",
            "depends_on",
            "row_count",
            "column_count",
            "last_built_at",
        }
        assert cols == expected
        conn.close()

    def test_cat006_catalog_columns_has_expected_columns(self) -> None:
        """CAT006: catalog_columns DDL creates all required columns."""
        conn = duckdb.connect(":memory:")
        create_catalog_tables(conn)

        cols = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'catalog_columns'"
            ).fetchall()
        }
        expected = {
            "id",
            "table_name",
            "column_name",
            "ordinal_position",
            "data_type",
            "is_nullable",
            "is_pk",
            "description",
            "sample_values",
        }
        assert cols == expected
        conn.close()

    def test_cat007_yaml_covers_key_tables(self) -> None:
        """CAT007: YAML has entries for all core warehouse tables."""
        result = load_yaml_metadata(METADATA_YAML)
        core_tables = {
            "transactions",
            "quarantine",
            "stg_transactions",
            "dim_accounts",
            "fct_transactions",
            "daily_spend_by_category",
            "monthly_account_summary",
        }
        for table in core_tables:
            assert table in result, f"YAML missing entry for core table: {table}"

    def test_cat008_no_empty_zones_in_yaml(self) -> None:
        """CAT008: No table entries have empty zone values."""
        result = load_yaml_metadata(METADATA_YAML)
        for table_name, meta in result.items():
            assert meta.zone, f"Table '{table_name}' has empty zone"
