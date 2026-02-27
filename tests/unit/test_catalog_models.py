"""Unit tests for catalog domain models."""

from __future__ import annotations

from src.catalog.models import (
    CatalogBuildResult,
    CatalogColumnEntry,
    CatalogStatus,
    CatalogTableEntry,
)


class TestCatalogStatus:
    """Tests for CatalogStatus enum."""

    def test_completed_value(self) -> None:
        assert CatalogStatus.COMPLETED.value == "completed"

    def test_failed_value(self) -> None:
        assert CatalogStatus.FAILED.value == "failed"


class TestCatalogTableEntry:
    """Tests for CatalogTableEntry dataclass."""

    def test_defaults(self) -> None:
        entry = CatalogTableEntry(table_name="test_table")
        assert entry.table_name == "test_table"
        assert entry.table_schema == "main"
        assert entry.table_type == "BASE TABLE"
        assert entry.zone == "raw"
        assert entry.description == ""
        assert entry.owner == ""
        assert entry.source_sql_file is None
        assert entry.depends_on is None
        assert entry.row_count is None
        assert entry.column_count == 0
        assert entry.last_built_at == ""


class TestCatalogColumnEntry:
    """Tests for CatalogColumnEntry dataclass."""

    def test_uuid_generated(self) -> None:
        entry1 = CatalogColumnEntry()
        entry2 = CatalogColumnEntry()
        assert entry1.id != entry2.id
        assert len(entry1.id) == 36  # UUID format

    def test_defaults(self) -> None:
        entry = CatalogColumnEntry()
        assert entry.table_name == ""
        assert entry.column_name == ""
        assert entry.ordinal_position == 0
        assert entry.data_type == ""
        assert entry.is_nullable is True
        assert entry.is_pk is False
        assert entry.description == ""
        assert entry.sample_values is None


class TestCatalogBuildResult:
    """Tests for CatalogBuildResult dataclass."""

    def test_defaults(self) -> None:
        result = CatalogBuildResult()
        assert result.tables_catalogued == 0
        assert result.columns_catalogued == 0
        assert result.elapsed_seconds == 0.0
        assert result.status == CatalogStatus.COMPLETED
        assert result.error_message is None

    def test_summary_format(self) -> None:
        result = CatalogBuildResult(
            tables_catalogued=10,
            columns_catalogued=50,
            elapsed_seconds=1.23,
            status=CatalogStatus.COMPLETED,
        )
        summary = result.summary()
        assert "Catalog Build Summary:" in summary
        assert "completed" in summary
        assert "10" in summary
        assert "50" in summary
        assert "1.23s" in summary

    def test_summary_with_error(self) -> None:
        result = CatalogBuildResult(
            status=CatalogStatus.FAILED,
            error_message="Database not found",
        )
        summary = result.summary()
        assert "failed" in summary
        assert "Database not found" in summary
