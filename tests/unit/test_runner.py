"""Unit tests for the transform runner (with mocked/in-memory DB)."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.transformer.models import TransformStatus
from src.transformer.runner import (
    _execute_model,
    _source_table_exists,
    create_transform_runs_table,
    run_transforms,
)


@pytest.fixture()
def mem_conn() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection for testing."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


class TestSourceTableExists:
    """Tests for _source_table_exists."""

    def test_table_exists(self, mem_conn: duckdb.DuckDBPyConnection) -> None:
        """Returns True when transactions table exists."""
        mem_conn.execute("CREATE TABLE transactions (id INTEGER)")
        assert _source_table_exists(mem_conn) is True

    def test_table_not_exists(self, mem_conn: duckdb.DuckDBPyConnection) -> None:
        """Returns False when transactions table does not exist."""
        assert _source_table_exists(mem_conn) is False


class TestCreateTransformRunsTable:
    """Tests for create_transform_runs_table."""

    def test_creates_table(self, mem_conn: duckdb.DuckDBPyConnection) -> None:
        """transform_runs table is created successfully."""
        create_transform_runs_table(mem_conn)
        result = mem_conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'transform_runs'"
        ).fetchone()
        assert result is not None and result[0] == 1

    def test_idempotent_creation(self, mem_conn: duckdb.DuckDBPyConnection) -> None:
        """Calling create_transform_runs_table twice does not error."""
        create_transform_runs_table(mem_conn)
        create_transform_runs_table(mem_conn)  # Should not raise


class TestExecuteModel:
    """Tests for _execute_model."""

    def test_successful_execution(self, mem_conn: duckdb.DuckDBPyConnection) -> None:
        """Valid SQL executes successfully."""
        result = _execute_model(mem_conn, "test_model", "CREATE TABLE test_out (id INTEGER)")
        assert result.success is True
        assert result.error is None
        assert result.elapsed_seconds > 0

    def test_failed_execution(self, mem_conn: duckdb.DuckDBPyConnection) -> None:
        """Invalid SQL returns failure result."""
        result = _execute_model(mem_conn, "bad_model", "THIS IS NOT SQL")
        assert result.success is False
        assert result.error is not None


class TestRunTransforms:
    """Tests for the full run_transforms orchestration."""

    def test_missing_database(self, tmp_path: Path) -> None:
        """Returns failed status when database file does not exist."""
        result = run_transforms(
            db_path=tmp_path / "nonexistent.duckdb",
            transforms_dir=tmp_path,
        )
        assert result.status == TransformStatus.FAILED
        assert "Database not found" in (result.error_message or "")

    def test_missing_source_table(self, tmp_path: Path) -> None:
        """Returns failed status when transactions table does not exist."""
        db_path = tmp_path / "test.duckdb"
        # Create the DB file so connection succeeds
        conn = duckdb.connect(str(db_path))
        conn.close()

        transforms_dir = tmp_path / "transforms"
        transforms_dir.mkdir()
        (transforms_dir / "staging__stg.sql").write_text(
            "CREATE OR REPLACE VIEW stg AS SELECT 1;"
        )

        result = run_transforms(db_path=db_path, transforms_dir=transforms_dir)
        assert result.status == TransformStatus.FAILED
        assert "transactions" in (result.error_message or "")

    def test_no_transform_files(self, tmp_path: Path) -> None:
        """Returns completed status when no SQL files found."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE transactions (id INTEGER)")
        conn.close()

        transforms_dir = tmp_path / "transforms"
        transforms_dir.mkdir()

        result = run_transforms(db_path=db_path, transforms_dir=transforms_dir)
        assert result.status == TransformStatus.COMPLETED
        assert result.models_executed == 0

    def test_successful_execution(self, tmp_path: Path) -> None:
        """Models execute successfully in correct order."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE transactions (id INTEGER, val DOUBLE)")
        conn.execute("INSERT INTO transactions VALUES (1, 10.0), (2, 20.0)")
        conn.close()

        transforms_dir = tmp_path / "transforms"
        transforms_dir.mkdir()
        (transforms_dir / "staging__stg.sql").write_text(
            "CREATE OR REPLACE VIEW stg AS SELECT id, val FROM transactions;"
        )
        (transforms_dir / "mart__agg.sql").write_text(
            "-- depends_on: stg\n"
            "CREATE OR REPLACE TABLE agg AS SELECT SUM(val) AS total FROM stg;"
        )

        result = run_transforms(db_path=db_path, transforms_dir=transforms_dir)
        assert result.status == TransformStatus.COMPLETED
        assert result.models_executed == 2
        assert result.models_failed == 0

        # Verify output
        conn = duckdb.connect(str(db_path))
        total = conn.execute("SELECT total FROM agg").fetchone()
        assert total is not None and total[0] == 30.0
        conn.close()

    def test_partial_failure(self, tmp_path: Path) -> None:
        """One model failing does not prevent others from executing."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE transactions (id INTEGER)")
        conn.close()

        transforms_dir = tmp_path / "transforms"
        transforms_dir.mkdir()
        (transforms_dir / "a__good.sql").write_text(
            "CREATE OR REPLACE VIEW good AS SELECT 1 AS x;"
        )
        (transforms_dir / "a__bad.sql").write_text(
            "THIS IS INVALID SQL;"
        )

        result = run_transforms(db_path=db_path, transforms_dir=transforms_dir)
        assert result.status == TransformStatus.COMPLETED
        assert result.models_executed == 1
        assert result.models_failed == 1
