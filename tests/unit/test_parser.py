"""Unit tests for the SQL file parser."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.transformer.parser import derive_model_name, discover_models, parse_sql_file


class TestDeriveModelName:
    """Tests for derive_model_name function."""

    def test_staging_prefix(self, tmp_path: Path) -> None:
        """Layer prefix is stripped, model name retained."""
        assert derive_model_name(Path("staging__stg_transactions.sql")) == "stg_transactions"

    def test_mart_prefix(self) -> None:
        """Mart layer prefix is stripped."""
        result = derive_model_name(Path("mart__daily_spend_by_category.sql"))
        assert result == "daily_spend_by_category"

    def test_no_prefix(self) -> None:
        """Files without layer prefix use full stem as model name."""
        assert derive_model_name(Path("my_model.sql")) == "my_model"

    def test_multiple_underscores_in_name(self) -> None:
        """Only the first __ is used as separator."""
        assert derive_model_name(Path("mart__my__complex__name.sql")) == "my__complex__name"


class TestParseSqlFile:
    """Tests for parse_sql_file function."""

    def test_parse_with_headers(self, tmp_path: Path) -> None:
        """Model name and dependencies extracted from comment headers."""
        sql_file = tmp_path / "staging__stg_transactions.sql"
        sql_file.write_text(
            "-- model: stg_transactions\n"
            "-- depends_on: transactions\n"
            "CREATE OR REPLACE VIEW stg_transactions AS\n"
            "SELECT * FROM transactions;\n"
        )
        model = parse_sql_file(sql_file)
        assert model.name == "stg_transactions"
        assert model.depends_on == ["transactions"]
        assert "CREATE OR REPLACE VIEW" in model.sql

    def test_parse_without_model_header(self, tmp_path: Path) -> None:
        """Model name derived from filename when no -- model: header."""
        sql_file = tmp_path / "mart__daily_spend.sql"
        sql_file.write_text(
            "-- depends_on: stg_transactions\n"
            "CREATE OR REPLACE TABLE daily_spend AS\n"
            "SELECT 1;\n"
        )
        model = parse_sql_file(sql_file)
        assert model.name == "daily_spend"
        assert model.depends_on == ["stg_transactions"]

    def test_parse_multiple_dependencies(self, tmp_path: Path) -> None:
        """Multiple comma-separated dependencies are parsed."""
        sql_file = tmp_path / "mart__combined.sql"
        sql_file.write_text(
            "-- depends_on: stg_transactions, stg_accounts\n"
            "CREATE OR REPLACE TABLE combined AS\n"
            "SELECT 1;\n"
        )
        model = parse_sql_file(sql_file)
        assert model.depends_on == ["stg_transactions", "stg_accounts"]

    def test_parse_no_dependencies(self, tmp_path: Path) -> None:
        """Models with no depends_on header have empty dependency list."""
        sql_file = tmp_path / "staging__raw_view.sql"
        sql_file.write_text(
            "CREATE OR REPLACE VIEW raw_view AS\n"
            "SELECT 1;\n"
        )
        model = parse_sql_file(sql_file)
        assert model.depends_on == []

    def test_parse_empty_file_raises(self, tmp_path: Path) -> None:
        """Empty SQL file raises ValueError."""
        sql_file = tmp_path / "empty.sql"
        sql_file.write_text("")
        with pytest.raises(ValueError, match="empty"):
            parse_sql_file(sql_file)

    def test_parse_headers_only_raises(self, tmp_path: Path) -> None:
        """SQL file with only headers and no SQL content raises ValueError."""
        sql_file = tmp_path / "headers_only.sql"
        sql_file.write_text(
            "-- model: test\n"
            "-- depends_on: something\n"
        )
        with pytest.raises(ValueError, match="no SQL content"):
            parse_sql_file(sql_file)

    def test_parse_nonexistent_file_raises(self, tmp_path: Path) -> None:
        """Non-existent file raises ValueError."""
        sql_file = tmp_path / "nonexistent.sql"
        with pytest.raises(ValueError, match="Cannot read"):
            parse_sql_file(sql_file)


class TestDiscoverModels:
    """Tests for discover_models function."""

    def test_discover_multiple_models(self, tmp_path: Path) -> None:
        """All valid .sql files in directory are discovered."""
        (tmp_path / "staging__a.sql").write_text("CREATE OR REPLACE VIEW a AS SELECT 1;")
        (tmp_path / "mart__b.sql").write_text(
            "-- depends_on: a\nCREATE OR REPLACE TABLE b AS SELECT 1;"
        )
        models = discover_models(tmp_path)
        assert len(models) == 2
        names = {m.name for m in models}
        assert names == {"a", "b"}

    def test_discover_skips_non_sql(self, tmp_path: Path) -> None:
        """Non-.sql files are ignored."""
        (tmp_path / "valid.sql").write_text("CREATE OR REPLACE VIEW v AS SELECT 1;")
        (tmp_path / "readme.md").write_text("# Not SQL")
        models = discover_models(tmp_path)
        assert len(models) == 1

    def test_discover_empty_directory(self, tmp_path: Path) -> None:
        """Empty directory returns empty list."""
        models = discover_models(tmp_path)
        assert models == []

    def test_discover_nonexistent_directory(self, tmp_path: Path) -> None:
        """Non-existent directory raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            discover_models(tmp_path / "nope")

    def test_discover_skips_invalid_files(self, tmp_path: Path) -> None:
        """Invalid SQL files are skipped with warning, valid ones still parsed."""
        (tmp_path / "valid.sql").write_text("CREATE OR REPLACE VIEW valid AS SELECT 1;")
        (tmp_path / "empty.sql").write_text("")
        models = discover_models(tmp_path)
        assert len(models) == 1
        assert models[0].name == "valid"
