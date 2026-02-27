"""Unit tests for catalog parser (YAML loader, SQL header scanner, zone inference)."""

from __future__ import annotations

from pathlib import Path

from src.catalog.parser import infer_zone, load_yaml_metadata, scan_sql_headers


class TestLoadYamlMetadata:
    """Tests for load_yaml_metadata."""

    def test_happy_path(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "catalog.yaml"
        yaml_file.write_text(
            "tables:\n"
            "  transactions:\n"
            "    zone: raw\n"
            "    description: 'Raw transactions'\n"
            "    owner: 'team-a'\n"
            "    columns:\n"
            "      transaction_id:\n"
            "        description: 'Primary key'\n"
            "        is_pk: true\n"
        )
        result = load_yaml_metadata(yaml_file)
        assert "transactions" in result
        meta = result["transactions"]
        assert meta.zone == "raw"
        assert meta.description == "Raw transactions"
        assert meta.owner == "team-a"
        assert "transaction_id" in meta.columns
        assert meta.columns["transaction_id"]["is_pk"] is True

    def test_missing_file(self, tmp_path: Path) -> None:
        result = load_yaml_metadata(tmp_path / "nonexistent.yaml")
        assert result == {}

    def test_empty_file(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "empty.yaml"
        yaml_file.write_text("")
        result = load_yaml_metadata(yaml_file)
        assert result == {}

    def test_no_tables_key(self, tmp_path: Path) -> None:
        """YAML without 'tables' key treats root dict as tables."""
        yaml_file = tmp_path / "catalog.yaml"
        yaml_file.write_text(
            "transactions:\n"
            "  zone: raw\n"
            "  description: 'Direct root'\n"
        )
        result = load_yaml_metadata(yaml_file)
        assert "transactions" in result
        assert result["transactions"].zone == "raw"


class TestScanSqlHeaders:
    """Tests for scan_sql_headers."""

    def test_extracts_model_name(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "staging__stg_transactions.sql"
        sql_file.write_text(
            "-- model: stg_transactions\n"
            "-- depends_on: transactions\n"
            "-- description: Staging view\n"
            "CREATE OR REPLACE VIEW stg_transactions AS SELECT * FROM transactions;\n"
        )
        result = scan_sql_headers([tmp_path])
        assert "stg_transactions" in result
        meta = result["stg_transactions"]
        assert meta.model_name == "stg_transactions"
        assert meta.depends_on == "transactions"
        assert meta.description == "Staging view"
        assert str(tmp_path) in meta.file_path

    def test_extracts_depends_on(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "mart__summary.sql"
        sql_file.write_text(
            "-- model: summary\n"
            "-- depends_on: stg_transactions, dim_accounts\n"
            "SELECT 1;\n"
        )
        result = scan_sql_headers([tmp_path])
        assert result["summary"].depends_on == "stg_transactions, dim_accounts"

    def test_handles_missing_headers(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "mart__raw_model.sql"
        sql_file.write_text("SELECT 1;\n")
        result = scan_sql_headers([tmp_path])
        assert "raw_model" in result
        assert result["raw_model"].depends_on == ""
        assert result["raw_model"].description == ""

    def test_missing_directory(self, tmp_path: Path) -> None:
        result = scan_sql_headers([tmp_path / "nonexistent"])
        assert result == {}


class TestInferZone:
    """Tests for infer_zone naming convention logic."""

    def test_staging(self) -> None:
        assert infer_zone("stg_transactions") == "staging"

    def test_dimension(self) -> None:
        assert infer_zone("dim_accounts") == "dimension"

    def test_fact(self) -> None:
        assert infer_zone("fct_transactions") == "fact"

    def test_raw_transactions(self) -> None:
        assert infer_zone("transactions") == "raw"

    def test_raw_quarantine(self) -> None:
        assert infer_zone("quarantine") == "raw"

    def test_metadata_runs(self) -> None:
        assert infer_zone("check_runs") == "metadata"
        assert infer_zone("ingestion_runs") == "metadata"
        assert infer_zone("transform_runs") == "metadata"
        assert infer_zone("pipeline_runs") == "metadata"

    def test_metadata_results(self) -> None:
        assert infer_zone("check_results") == "metadata"

    def test_metadata_catalog(self) -> None:
        assert infer_zone("catalog_tables") == "metadata"
        assert infer_zone("catalog_columns") == "metadata"

    def test_mart_default(self) -> None:
        assert infer_zone("daily_spend_by_category") == "mart"
        assert infer_zone("monthly_account_summary") == "mart"
