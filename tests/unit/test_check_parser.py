"""Unit tests for the check SQL file parser."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.checker.models import CheckSeverity
from src.checker.parser import derive_check_name, discover_checks, parse_check_file


class TestDeriveCheckName:
    """Tests for derive_check_name function."""

    def test_check_prefix(self) -> None:
        """check__ prefix is stripped."""
        result = derive_check_name(Path("check__row_count_staging.sql"))
        assert result == "row_count_staging"

    def test_no_prefix(self) -> None:
        """Files without prefix use full stem."""
        assert derive_check_name(Path("my_check.sql")) == "my_check"

    def test_multiple_underscores(self) -> None:
        """Only the first __ is used as separator."""
        result = derive_check_name(Path("check__a__b__c.sql"))
        assert result == "a__b__c"


class TestParseCheckFile:
    """Tests for parse_check_file function."""

    def test_full_headers(self, tmp_path: Path) -> None:
        """All three headers extracted correctly."""
        f = tmp_path / "check__test.sql"
        f.write_text(
            "-- check: my_test_check\n"
            "-- severity: critical\n"
            "-- description: Ensures no nulls\n"
            "SELECT * FROM t WHERE id IS NULL;\n"
        )
        model = parse_check_file(f)
        assert model.name == "my_test_check"
        assert model.severity == CheckSeverity.CRITICAL
        assert model.description == "Ensures no nulls"
        assert "SELECT * FROM t WHERE id IS NULL" in model.sql

    def test_name_from_filename(self, tmp_path: Path) -> None:
        """Name derived from filename when no -- check: header."""
        f = tmp_path / "check__freshness.sql"
        f.write_text("SELECT 1 WHERE 0;\n")
        model = parse_check_file(f)
        assert model.name == "freshness"

    def test_default_severity_warn(self, tmp_path: Path) -> None:
        """Severity defaults to warn when not specified."""
        f = tmp_path / "check__test.sql"
        f.write_text("SELECT 1 WHERE 0;\n")
        model = parse_check_file(f)
        assert model.severity == CheckSeverity.WARN

    def test_invalid_severity_defaults_warn(
        self, tmp_path: Path
    ) -> None:
        """Invalid severity value falls back to warn."""
        f = tmp_path / "check__test.sql"
        f.write_text(
            "-- severity: blocker\n"
            "SELECT 1 WHERE 0;\n"
        )
        model = parse_check_file(f)
        assert model.severity == CheckSeverity.WARN

    def test_empty_file_raises(self, tmp_path: Path) -> None:
        """Empty file raises ValueError."""
        f = tmp_path / "empty.sql"
        f.write_text("")
        with pytest.raises(ValueError, match="empty"):
            parse_check_file(f)

    def test_headers_only_raises(self, tmp_path: Path) -> None:
        """File with only headers and no SQL raises ValueError."""
        f = tmp_path / "headers.sql"
        f.write_text(
            "-- check: test\n"
            "-- severity: warn\n"
            "-- description: nothing\n"
        )
        with pytest.raises(ValueError, match="no SQL content"):
            parse_check_file(f)

    def test_nonexistent_file_raises(self, tmp_path: Path) -> None:
        """Non-existent file raises ValueError."""
        f = tmp_path / "nope.sql"
        with pytest.raises(ValueError, match="Cannot read"):
            parse_check_file(f)

    def test_empty_description(self, tmp_path: Path) -> None:
        """Missing description header results in empty string."""
        f = tmp_path / "check__test.sql"
        f.write_text("SELECT 1 WHERE 0;\n")
        model = parse_check_file(f)
        assert model.description == ""


class TestDiscoverChecks:
    """Tests for discover_checks function."""

    def test_discovers_multiple(self, tmp_path: Path) -> None:
        """All .sql files discovered and parsed."""
        (tmp_path / "check__a.sql").write_text("SELECT 1 WHERE 0;")
        (tmp_path / "check__b.sql").write_text("SELECT 1 WHERE 0;")
        checks = discover_checks(tmp_path)
        assert len(checks) == 2
        names = {c.name for c in checks}
        assert names == {"a", "b"}

    def test_skips_non_sql(self, tmp_path: Path) -> None:
        """Non-.sql files are ignored."""
        (tmp_path / "check__a.sql").write_text("SELECT 1 WHERE 0;")
        (tmp_path / "readme.md").write_text("# Not SQL")
        checks = discover_checks(tmp_path)
        assert len(checks) == 1

    def test_empty_directory(self, tmp_path: Path) -> None:
        """Empty directory returns empty list."""
        assert discover_checks(tmp_path) == []

    def test_nonexistent_directory(self, tmp_path: Path) -> None:
        """Non-existent directory raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            discover_checks(tmp_path / "nope")

    def test_skips_invalid_files(self, tmp_path: Path) -> None:
        """Invalid SQL files are skipped, valid ones still parsed."""
        (tmp_path / "valid.sql").write_text("SELECT 1 WHERE 0;")
        (tmp_path / "empty.sql").write_text("")
        checks = discover_checks(tmp_path)
        assert len(checks) == 1
        assert checks[0].name == "valid"

    def test_sorted_by_filename(self, tmp_path: Path) -> None:
        """Results are sorted by filename."""
        (tmp_path / "check__z.sql").write_text("SELECT 1 WHERE 0;")
        (tmp_path / "check__a.sql").write_text("SELECT 1 WHERE 0;")
        checks = discover_checks(tmp_path)
        assert checks[0].name == "a"
        assert checks[1].name == "z"
