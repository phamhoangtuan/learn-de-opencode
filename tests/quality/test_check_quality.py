"""Quality tests for the data quality check framework (CHK001-CHK027).

Maps directly to checklist items in
``specs/004-data-quality-checks/checklists/data-quality.md``.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from src.checker.models import CheckSeverity, CheckStatus, RunStatus
from src.checker.parser import discover_checks, parse_check_file
from src.checker.runner import create_metadata_tables, run_checks

CHECKS_DIR = Path("src/checks")

EXPECTED_CHECKS = {
    "check__row_count_staging.sql",
    "check__mart_not_empty.sql",
    "check__freshness.sql",
    "check__unique_daily_spend_grain.sql",
    "check__unique_monthly_summary_grain.sql",
    "check__accepted_values_currency.sql",
}


# ---------------------------------------------------------------------------
# Completeness (CHK001-CHK004)
# ---------------------------------------------------------------------------


class TestCompleteness:
    """CHK001-CHK004: All check files exist and are parseable."""

    def test_chk001_all_six_files_exist(self) -> None:
        """CHK001: All 6 pre-built check SQL files exist."""
        actual = {f.name for f in CHECKS_DIR.glob("*.sql")}
        assert actual == EXPECTED_CHECKS

    def test_chk002_all_files_have_headers(self) -> None:
        """CHK002: Each file has check, severity, and description headers."""
        for sql_file in sorted(CHECKS_DIR.glob("*.sql")):
            model = parse_check_file(sql_file)
            assert model.name, f"{sql_file.name} missing check name"
            assert model.severity in (
                CheckSeverity.CRITICAL,
                CheckSeverity.WARN,
            ), f"{sql_file.name} missing severity"
            assert model.description, (
                f"{sql_file.name} missing description"
            )

    def test_chk003_parser_extracts_metadata(self) -> None:
        """CHK003: Parser extracts all metadata fields correctly."""
        checks = discover_checks(CHECKS_DIR)
        names = {c.name for c in checks}
        expected_names = {
            "row_count_staging",
            "mart_not_empty",
            "freshness",
            "unique_daily_spend_grain",
            "unique_monthly_summary_grain",
            "accepted_values_currency",
        }
        assert names == expected_names

    def test_chk004_runner_discovers_all(self) -> None:
        """CHK004: Runner discovers all check files."""
        checks = discover_checks(CHECKS_DIR)
        assert len(checks) == 6


# ---------------------------------------------------------------------------
# Correctness (CHK011-CHK017)
# ---------------------------------------------------------------------------


class TestCorrectness:
    """CHK011-CHK017: Pass/fail/error logic is correct."""

    def test_chk011_zero_rows_is_pass(self, tmp_path: Path) -> None:
        """CHK011: A check returning zero rows is marked pass."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.close()

        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()
        (checks_dir / "check__pass.sql").write_text(
            "-- check: zero_rows\n"
            "-- severity: critical\n"
            "-- description: Should pass\n"
            "SELECT * FROM t WHERE id < 0;\n"
        )

        result = run_checks(db_path=db_path, checks_dir=checks_dir)
        assert result.check_results[0].status == CheckStatus.PASS

    def test_chk012_rows_returned_is_fail(self, tmp_path: Path) -> None:
        """CHK012: A check returning rows is marked fail with count."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.execute("INSERT INTO t VALUES (1), (2), (3)")
        conn.close()

        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()
        (checks_dir / "check__fail.sql").write_text(
            "-- check: has_rows\n"
            "-- severity: warn\n"
            "-- description: Should fail\n"
            "SELECT id FROM t;\n"
        )

        result = run_checks(db_path=db_path, checks_dir=checks_dir)
        cr = result.check_results[0]
        assert cr.status == CheckStatus.FAIL
        assert cr.violation_count == 3

    def test_chk013_sql_error_is_error(self, tmp_path: Path) -> None:
        """CHK013: SQL error is marked error, not crash."""
        db_path = tmp_path / "test.duckdb"
        duckdb.connect(str(db_path)).close()

        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()
        (checks_dir / "check__bad.sql").write_text(
            "-- check: bad_sql\n"
            "-- severity: warn\n"
            "-- description: Broken SQL\n"
            "SELECT * FROM nonexistent_table_xyz;\n"
        )

        result = run_checks(db_path=db_path, checks_dir=checks_dir)
        cr = result.check_results[0]
        assert cr.status == CheckStatus.ERROR
        assert cr.error is not None

    def test_chk014_critical_fail_exit_code_1(
        self, tmp_path: Path
    ) -> None:
        """CHK014: Critical failure sets run status to FAILED."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
        conn.close()

        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()
        (checks_dir / "check__crit.sql").write_text(
            "-- check: crit\n"
            "-- severity: critical\n"
            "-- description: Critical fail\n"
            "SELECT id FROM t;\n"
        )

        result = run_checks(db_path=db_path, checks_dir=checks_dir)
        assert result.status == RunStatus.FAILED

    def test_chk015_warn_only_exit_code_0(self, tmp_path: Path) -> None:
        """CHK015: Warn-only failure sets run status to WARN."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
        conn.close()

        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()
        (checks_dir / "check__w.sql").write_text(
            "-- check: warn_only\n"
            "-- severity: warn\n"
            "-- description: Warn fail\n"
            "SELECT id FROM t;\n"
        )

        result = run_checks(db_path=db_path, checks_dir=checks_dir)
        assert result.status == RunStatus.WARN

    def test_chk016_all_pass_status_passed(
        self, tmp_path: Path
    ) -> None:
        """CHK016: All-pass sets run status to PASSED."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.close()

        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()
        (checks_dir / "check__ok.sql").write_text(
            "-- check: ok\n"
            "-- severity: critical\n"
            "-- description: Should pass\n"
            "SELECT * FROM t WHERE id < 0;\n"
        )

        result = run_checks(db_path=db_path, checks_dir=checks_dir)
        assert result.status == RunStatus.PASSED

    def test_chk017_sample_violations_up_to_5(
        self, tmp_path: Path
    ) -> None:
        """CHK017: Sample violations capture up to 5 rows."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE t AS SELECT unnest(range(10)) AS id")
        conn.close()

        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()
        (checks_dir / "check__many.sql").write_text(
            "-- check: many\n"
            "-- severity: warn\n"
            "-- description: 10 violations\n"
            "SELECT id FROM t;\n"
        )

        result = run_checks(db_path=db_path, checks_dir=checks_dir)
        cr = result.check_results[0]
        assert cr.violation_count == 10
        assert len(cr.sample_violations) == 5


# ---------------------------------------------------------------------------
# Metadata & Lineage (CHK018-CHK022)
# ---------------------------------------------------------------------------


class TestMetadata:
    """CHK018-CHK022: Metadata tables and linkage."""

    def test_chk018_check_runs_schema(self) -> None:
        """CHK018: check_runs table has correct schema."""
        conn = duckdb.connect(":memory:")
        create_metadata_tables(conn)

        cols = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'check_runs'"
            ).fetchall()
        }
        expected = {
            "run_id",
            "started_at",
            "completed_at",
            "status",
            "total_checks",
            "checks_passed",
            "checks_failed",
            "checks_errored",
            "elapsed_seconds",
            "error_message",
        }
        assert cols == expected
        conn.close()

    def test_chk019_check_results_schema(self) -> None:
        """CHK019: check_results table has correct schema."""
        conn = duckdb.connect(":memory:")
        create_metadata_tables(conn)

        cols = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'check_results'"
            ).fetchall()
        }
        expected = {
            "id",
            "run_id",
            "check_name",
            "severity",
            "description",
            "status",
            "violation_count",
            "sample_violations",
            "elapsed_seconds",
            "error_message",
        }
        assert cols == expected
        conn.close()

    def test_chk020_one_run_row_per_invocation(
        self, tmp_path: Path
    ) -> None:
        """CHK020: Each run creates exactly one check_runs row."""
        db_path = tmp_path / "test.duckdb"
        duckdb.connect(str(db_path)).close()
        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()
        (checks_dir / "check__x.sql").write_text(
            "-- check: x\n-- severity: warn\n-- description: x\n"
            "SELECT 1 WHERE 0;\n"
        )

        result = run_checks(db_path=db_path, checks_dir=checks_dir)

        conn = duckdb.connect(str(db_path))
        count = conn.execute(
            "SELECT COUNT(*) FROM check_runs WHERE run_id = ?",
            [result.run_id],
        ).fetchone()
        assert count is not None
        assert count[0] == 1
        conn.close()

    def test_chk021_one_result_row_per_check(
        self, tmp_path: Path
    ) -> None:
        """CHK021: Each check creates exactly one check_results row."""
        db_path = tmp_path / "test.duckdb"
        duckdb.connect(str(db_path)).close()
        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()
        (checks_dir / "check__a.sql").write_text(
            "-- check: a\n-- severity: warn\n-- description: a\n"
            "SELECT 1 WHERE 0;\n"
        )
        (checks_dir / "check__b.sql").write_text(
            "-- check: b\n-- severity: warn\n-- description: b\n"
            "SELECT 1 WHERE 0;\n"
        )

        result = run_checks(db_path=db_path, checks_dir=checks_dir)

        conn = duckdb.connect(str(db_path))
        count = conn.execute(
            "SELECT COUNT(*) FROM check_results WHERE run_id = ?",
            [result.run_id],
        ).fetchone()
        assert count is not None
        assert count[0] == 2
        conn.close()

    def test_chk022_run_id_links_tables(self, tmp_path: Path) -> None:
        """CHK022: run_id links check_runs to check_results."""
        db_path = tmp_path / "test.duckdb"
        duckdb.connect(str(db_path)).close()
        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()
        (checks_dir / "check__link.sql").write_text(
            "-- check: link\n-- severity: warn\n-- description: link\n"
            "SELECT 1 WHERE 0;\n"
        )

        result = run_checks(db_path=db_path, checks_dir=checks_dir)

        conn = duckdb.connect(str(db_path))
        joined = conn.execute(
            "SELECT cr.run_id, cres.check_name "
            "FROM check_runs cr "
            "JOIN check_results cres ON cr.run_id = cres.run_id "
            "WHERE cr.run_id = ?",
            [result.run_id],
        ).fetchall()
        assert len(joined) == 1
        assert joined[0][0] == result.run_id
        assert joined[0][1] == "link"
        conn.close()


# ---------------------------------------------------------------------------
# Robustness (CHK025-CHK027)
# ---------------------------------------------------------------------------


class TestRobustness:
    """CHK025-CHK027: Edge cases and error handling."""

    def test_chk025_empty_checks_dir(self, tmp_path: Path) -> None:
        """CHK025: Empty checks directory → 0 checks, status passed."""
        db_path = tmp_path / "test.duckdb"
        duckdb.connect(str(db_path)).close()
        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()

        result = run_checks(db_path=db_path, checks_dir=checks_dir)
        assert result.total_checks == 0
        assert result.status == RunStatus.PASSED

    def test_chk026_missing_database(self, tmp_path: Path) -> None:
        """CHK026: Missing database produces clear error."""
        result = run_checks(
            db_path=tmp_path / "nope.duckdb",
            checks_dir=tmp_path,
        )
        assert result.status == RunStatus.ERROR
        assert "not found" in (result.error_message or "").lower()

    def test_chk027_missing_marts_per_check_error(
        self, tmp_path: Path
    ) -> None:
        """CHK027: Missing mart tables produce per-check errors, not crash."""
        db_path = tmp_path / "bare.duckdb"
        conn = duckdb.connect(str(db_path))
        # Only transactions + staging, no marts
        conn.execute("""
            CREATE TABLE transactions (
                transaction_id VARCHAR, "timestamp" TIMESTAMPTZ,
                transaction_date DATE, amount DOUBLE, currency VARCHAR,
                merchant_name VARCHAR, category VARCHAR,
                account_id VARCHAR, transaction_type VARCHAR,
                status VARCHAR, source_file VARCHAR,
                ingested_at TIMESTAMPTZ, run_id VARCHAR
            )
        """)
        conn.execute("""
            INSERT INTO transactions VALUES
                ('tx-1', NOW(), '2026-02-18', 10.0, 'USD', 'Store',
                 'Food', 'ACC-1', 'debit', 'completed', 'f.parquet',
                 NOW(), 'run-1')
        """)
        conn.execute("""
            CREATE OR REPLACE VIEW stg_transactions AS
            SELECT transaction_id, "timestamp" AS transaction_timestamp,
                   transaction_date, amount, currency, merchant_name,
                   category, account_id, transaction_type, status,
                   source_file, ingested_at, run_id
            FROM transactions
        """)
        conn.close()

        result = run_checks(db_path=db_path, checks_dir=CHECKS_DIR)
        # Runner should complete (not crash)
        assert result.error_message is None
        # Checks for missing mart tables should be ERROR
        assert result.checks_errored >= 1
        # Some checks may still pass (row_count, freshness, currency)
        assert result.checks_passed >= 1
