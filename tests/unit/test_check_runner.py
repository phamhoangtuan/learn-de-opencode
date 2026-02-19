"""Unit tests for the check runner."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from src.checker.models import (
    CheckModel,
    CheckResult,
    CheckRunResult,
    CheckSeverity,
    CheckStatus,
    RunStatus,
)
from src.checker.runner import (
    _execute_check,
    _record_check_result_with_description,
    complete_check_run,
    create_check_run,
    create_metadata_tables,
    run_checks,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def mem_conn() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB connection for fast unit tests."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


def _make_check(
    name: str = "test_check",
    sql: str = "SELECT 1 WHERE 0",
    severity: CheckSeverity = CheckSeverity.WARN,
    description: str = "A test check",
) -> CheckModel:
    """Create a CheckModel for testing."""
    return CheckModel(
        name=name,
        file_path=Path("/fake/check__test.sql"),
        sql=sql,
        severity=severity,
        description=description,
    )


# ---------------------------------------------------------------------------
# _execute_check
# ---------------------------------------------------------------------------


class TestExecuteCheck:
    """Tests for the _execute_check helper."""

    def test_passing_check_returns_pass(
        self, mem_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """Zero rows returned means PASS."""
        check = _make_check(sql="SELECT 1 WHERE 1 = 0")
        result = _execute_check(mem_conn, check)

        assert result.status == CheckStatus.PASS
        assert result.violation_count == 0
        assert result.sample_violations == []
        assert result.elapsed_seconds > 0
        assert result.error is None

    def test_failing_check_returns_fail(
        self, mem_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """One or more rows returned means FAIL."""
        check = _make_check(sql="SELECT 42 AS bad_id")
        result = _execute_check(mem_conn, check)

        assert result.status == CheckStatus.FAIL
        assert result.violation_count == 1
        assert len(result.sample_violations) == 1
        assert result.sample_violations[0]["bad_id"] == 42

    def test_multiple_violations_capped_at_5(
        self, mem_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """Sample violations are capped at MAX_SAMPLE_VIOLATIONS (5)."""
        check = _make_check(
            sql="SELECT unnest(range(10)) AS violation_id"
        )
        result = _execute_check(mem_conn, check)

        assert result.status == CheckStatus.FAIL
        assert result.violation_count == 10
        assert len(result.sample_violations) == 5

    def test_sql_error_returns_error(
        self, mem_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """SQL syntax errors produce ERROR status."""
        check = _make_check(sql="SELECT * FROM nonexistent_table_xyz")
        result = _execute_check(mem_conn, check)

        assert result.status == CheckStatus.ERROR
        assert result.error is not None
        assert result.elapsed_seconds > 0

    def test_severity_preserved(
        self, mem_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """Check severity is carried through to the result."""
        check = _make_check(
            sql="SELECT 1 WHERE 0",
            severity=CheckSeverity.CRITICAL,
        )
        result = _execute_check(mem_conn, check)
        assert result.severity == CheckSeverity.CRITICAL

    def test_name_preserved(
        self, mem_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """Check name is carried through to the result."""
        check = _make_check(name="my_special_check", sql="SELECT 1 WHERE 0")
        result = _execute_check(mem_conn, check)
        assert result.name == "my_special_check"


# ---------------------------------------------------------------------------
# Metadata table DDL
# ---------------------------------------------------------------------------


class TestCreateMetadataTables:
    """Tests for create_metadata_tables."""

    def test_tables_created(
        self, mem_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """check_runs and check_results tables exist after creation."""
        create_metadata_tables(mem_conn)

        tables = {
            row[0]
            for row in mem_conn.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
        assert "check_runs" in tables
        assert "check_results" in tables

    def test_idempotent(
        self, mem_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """Calling create twice does not raise."""
        create_metadata_tables(mem_conn)
        create_metadata_tables(mem_conn)  # No error


# ---------------------------------------------------------------------------
# Metadata recording
# ---------------------------------------------------------------------------


class TestMetadataRecording:
    """Tests for create_check_run, complete_check_run, and result recording."""

    def test_create_and_complete_check_run(
        self, mem_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """A check run can be created and completed."""
        create_metadata_tables(mem_conn)
        run_id = "test-run-001"
        create_check_run(
            mem_conn, run_id=run_id, started_at="2026-02-19T10:00:00+00:00"
        )

        row = mem_conn.execute(
            "SELECT run_id, status FROM check_runs WHERE run_id = ?",
            [run_id],
        ).fetchone()
        assert row is not None
        assert row[0] == run_id
        assert row[1] == "running"

        # Complete the run
        run_result = CheckRunResult(run_id=run_id)
        run_result.status = RunStatus.PASSED
        run_result.total_checks = 2
        run_result.checks_passed = 2
        run_result.elapsed_seconds = 1.5
        complete_check_run(mem_conn, run_result=run_result)

        row = mem_conn.execute(
            "SELECT status, total_checks, checks_passed FROM check_runs "
            "WHERE run_id = ?",
            [run_id],
        ).fetchone()
        assert row is not None
        assert row[0] == "passed"
        assert row[1] == 2
        assert row[2] == 2

    def test_record_check_result_with_description(
        self, mem_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """Per-check results are persisted with description."""
        create_metadata_tables(mem_conn)
        check = _make_check(
            name="row_count",
            description="Rows match source",
            severity=CheckSeverity.CRITICAL,
        )
        result = CheckResult(
            name="row_count",
            severity=CheckSeverity.CRITICAL,
            status=CheckStatus.PASS,
            violation_count=0,
            elapsed_seconds=0.05,
        )
        _record_check_result_with_description(
            mem_conn,
            run_id="test-run-001",
            check=check,
            result=result,
        )

        row = mem_conn.execute(
            "SELECT check_name, severity, status, description "
            "FROM check_results WHERE run_id = 'test-run-001'"
        ).fetchone()
        assert row is not None
        assert row[0] == "row_count"
        assert row[1] == "critical"
        assert row[2] == "pass"
        assert row[3] == "Rows match source"

    def test_record_failed_check_with_samples(
        self, mem_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """Failed check results include JSON sample violations."""
        create_metadata_tables(mem_conn)
        check = _make_check(name="dup_check")
        samples = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
        result = CheckResult(
            name="dup_check",
            severity=CheckSeverity.WARN,
            status=CheckStatus.FAIL,
            violation_count=2,
            sample_violations=samples,
            elapsed_seconds=0.1,
        )
        _record_check_result_with_description(
            mem_conn,
            run_id="test-run-002",
            check=check,
            result=result,
        )

        row = mem_conn.execute(
            "SELECT violation_count, sample_violations "
            "FROM check_results WHERE run_id = 'test-run-002'"
        ).fetchone()
        assert row is not None
        assert row[0] == 2
        parsed = json.loads(row[1])
        assert len(parsed) == 2
        assert parsed[0]["id"] == 1

    def test_record_error_check(
        self, mem_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """Error check results include the error message."""
        create_metadata_tables(mem_conn)
        check = _make_check(name="bad_check")
        result = CheckResult(
            name="bad_check",
            severity=CheckSeverity.WARN,
            status=CheckStatus.ERROR,
            elapsed_seconds=0.01,
            error="Table not found",
        )
        _record_check_result_with_description(
            mem_conn,
            run_id="test-run-003",
            check=check,
            result=result,
        )

        row = mem_conn.execute(
            "SELECT status, error_message "
            "FROM check_results WHERE run_id = 'test-run-003'"
        ).fetchone()
        assert row is not None
        assert row[0] == "error"
        assert row[1] == "Table not found"


# ---------------------------------------------------------------------------
# run_checks (full orchestration)
# ---------------------------------------------------------------------------


class TestRunChecks:
    """Tests for the run_checks orchestrator."""

    def test_missing_database(self, tmp_path: Path) -> None:
        """Missing DB file returns ERROR status."""
        result = run_checks(
            db_path=tmp_path / "nonexistent.duckdb",
            checks_dir=tmp_path,
        )
        assert result.status == RunStatus.ERROR
        assert "not found" in (result.error_message or "").lower()

    def test_missing_checks_directory(self, tmp_path: Path) -> None:
        """Missing checks directory returns ERROR status."""
        db_path = tmp_path / "test.duckdb"
        duckdb.connect(str(db_path)).close()  # create the file

        result = run_checks(
            db_path=db_path,
            checks_dir=tmp_path / "no_such_dir",
        )
        assert result.status == RunStatus.ERROR
        assert "not found" in (result.error_message or "").lower()

    def test_empty_checks_directory(self, tmp_path: Path) -> None:
        """Empty checks directory returns PASSED with 0 total checks."""
        db_path = tmp_path / "test.duckdb"
        duckdb.connect(str(db_path)).close()
        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()

        result = run_checks(db_path=db_path, checks_dir=checks_dir)
        assert result.status == RunStatus.PASSED
        assert result.total_checks == 0

    def test_all_checks_pass(self, tmp_path: Path) -> None:
        """All passing checks produce PASSED status."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.execute("INSERT INTO t VALUES (1), (2), (3)")
        conn.close()

        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()
        (checks_dir / "check__pass1.sql").write_text(
            "-- check: pass1\n"
            "-- severity: critical\n"
            "SELECT * FROM t WHERE id < 0;\n"
        )
        (checks_dir / "check__pass2.sql").write_text(
            "-- check: pass2\n"
            "-- severity: warn\n"
            "SELECT * FROM t WHERE id > 100;\n"
        )

        result = run_checks(db_path=db_path, checks_dir=checks_dir)
        assert result.status == RunStatus.PASSED
        assert result.total_checks == 2
        assert result.checks_passed == 2
        assert result.checks_failed == 0

    def test_critical_fail_produces_failed(self, tmp_path: Path) -> None:
        """A critical-severity failure produces FAILED run status."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
        conn.close()

        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()
        (checks_dir / "check__crit.sql").write_text(
            "-- check: crit_fail\n"
            "-- severity: critical\n"
            "SELECT id FROM t;\n"  # returns rows = fail
        )

        result = run_checks(db_path=db_path, checks_dir=checks_dir)
        assert result.status == RunStatus.FAILED
        assert result.checks_failed == 1

    def test_warn_fail_only_produces_warn(self, tmp_path: Path) -> None:
        """Only warn-severity failures produce WARN run status."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
        conn.close()

        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()
        (checks_dir / "check__warn.sql").write_text(
            "-- check: warn_fail\n"
            "-- severity: warn\n"
            "SELECT id FROM t;\n"
        )

        result = run_checks(db_path=db_path, checks_dir=checks_dir)
        assert result.status == RunStatus.WARN
        assert result.checks_failed == 1

    def test_sql_error_continues_execution(self, tmp_path: Path) -> None:
        """SQL error in one check doesn't stop subsequent checks."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.close()

        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()
        (checks_dir / "check__a_error.sql").write_text(
            "-- check: error_check\n"
            "SELECT * FROM nonexistent_xyz;\n"
        )
        (checks_dir / "check__b_pass.sql").write_text(
            "-- check: pass_check\n"
            "SELECT * FROM t WHERE id < 0;\n"
        )

        result = run_checks(db_path=db_path, checks_dir=checks_dir)
        assert result.total_checks == 2
        assert result.checks_errored == 1
        assert result.checks_passed == 1
        # Error without critical fail => WARN
        assert result.status == RunStatus.WARN

    def test_metadata_tables_populated(self, tmp_path: Path) -> None:
        """check_runs and check_results tables have rows after run."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
        conn.close()

        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()
        (checks_dir / "check__meta.sql").write_text(
            "-- check: meta_check\n"
            "-- severity: warn\n"
            "-- description: Test metadata\n"
            "SELECT * FROM t WHERE id < 0;\n"
        )

        result = run_checks(db_path=db_path, checks_dir=checks_dir)

        conn = duckdb.connect(str(db_path))
        try:
            # check_runs row
            run_row = conn.execute(
                "SELECT run_id, status, total_checks FROM check_runs "
                "WHERE run_id = ?",
                [result.run_id],
            ).fetchone()
            assert run_row is not None
            assert run_row[1] == "passed"
            assert run_row[2] == 1

            # check_results row
            result_row = conn.execute(
                "SELECT check_name, severity, status, description "
                "FROM check_results WHERE run_id = ?",
                [result.run_id],
            ).fetchone()
            assert result_row is not None
            assert result_row[0] == "meta_check"
            assert result_row[1] == "warn"
            assert result_row[2] == "pass"
            assert result_row[3] == "Test metadata"
        finally:
            conn.close()

    def test_sample_violations_in_metadata(self, tmp_path: Path) -> None:
        """Violation samples are persisted as JSON in check_results."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE t (id INTEGER, val VARCHAR)")
        conn.execute("INSERT INTO t VALUES (1, 'bad'), (2, 'worse')")
        conn.close()

        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()
        (checks_dir / "check__samples.sql").write_text(
            "-- check: samples\n"
            "-- severity: critical\n"
            "SELECT id, val FROM t;\n"
        )

        result = run_checks(db_path=db_path, checks_dir=checks_dir)

        conn = duckdb.connect(str(db_path))
        try:
            row = conn.execute(
                "SELECT violation_count, sample_violations "
                "FROM check_results WHERE run_id = ?",
                [result.run_id],
            ).fetchone()
            assert row is not None
            assert row[0] == 2
            samples = json.loads(row[1])
            assert len(samples) == 2
            assert samples[0]["id"] == 1
            assert samples[1]["val"] == "worse"
        finally:
            conn.close()

    def test_elapsed_seconds_positive(self, tmp_path: Path) -> None:
        """Elapsed seconds is a positive value after a run."""
        db_path = tmp_path / "test.duckdb"
        duckdb.connect(str(db_path)).close()
        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()

        result = run_checks(db_path=db_path, checks_dir=checks_dir)
        assert result.elapsed_seconds > 0

    def test_run_id_is_uuid(self, tmp_path: Path) -> None:
        """Run ID is a valid UUID."""
        import uuid

        db_path = tmp_path / "test.duckdb"
        duckdb.connect(str(db_path)).close()
        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()

        result = run_checks(db_path=db_path, checks_dir=checks_dir)
        uuid.UUID(result.run_id)  # Should not raise

    def test_mixed_critical_and_warn_failures(
        self, tmp_path: Path
    ) -> None:
        """Critical failure overrides warn failure in status."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
        conn.close()

        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()
        (checks_dir / "check__a_warn.sql").write_text(
            "-- check: warn_check\n"
            "-- severity: warn\n"
            "SELECT id FROM t;\n"
        )
        (checks_dir / "check__b_crit.sql").write_text(
            "-- check: crit_check\n"
            "-- severity: critical\n"
            "SELECT id FROM t;\n"
        )

        result = run_checks(db_path=db_path, checks_dir=checks_dir)
        assert result.status == RunStatus.FAILED
        assert result.checks_failed == 2
