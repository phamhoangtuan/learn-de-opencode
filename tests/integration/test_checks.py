"""Integration tests for the data quality check framework.

Tests T019-T020: End-to-end checks against a populated DuckDB warehouse
with realistic data, including healthy data (all pass) and injected
failures (empty marts, duplicate grains, bad currencies).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from src.checker.models import CheckStatus, RunStatus
from src.checker.runner import run_checks

# Path to the actual pre-built check SQL files
CHECKS_DIR = Path("src/checks")


@pytest.fixture
def healthy_warehouse(tmp_path: Path) -> Path:
    """Create a DuckDB warehouse with healthy data where all checks pass.

    Sets up:
    - transactions table with 5 valid rows (recent ingested_at)
    - stg_transactions view (same row count as source)
    - daily_spend_by_category table (non-empty, unique grains)
    - monthly_account_summary table (non-empty, unique grains)
    - All currencies in accepted set (USD, EUR, GBP, JPY)

    Returns:
        Path to the temporary DuckDB file.
    """
    db_path = tmp_path / "warehouse" / "test.duckdb"
    db_path.parent.mkdir(parents=True)
    conn = duckdb.connect(str(db_path))

    now = datetime.now(UTC)

    # Create the source transactions table matching the ingestion schema
    conn.execute("""
        CREATE TABLE transactions (
            transaction_id    VARCHAR,
            "timestamp"       TIMESTAMPTZ,
            transaction_date  DATE,
            amount            DOUBLE,
            currency          VARCHAR,
            merchant_name     VARCHAR,
            category          VARCHAR,
            account_id        VARCHAR,
            transaction_type  VARCHAR,
            status            VARCHAR,
            source_file       VARCHAR,
            ingested_at       TIMESTAMPTZ,
            run_id            VARCHAR
        )
    """)

    # Insert 5 valid rows with recent timestamps
    conn.execute(
        """
        INSERT INTO transactions VALUES
            ('tx-001', ?::TIMESTAMPTZ, '2026-02-18', 42.50, 'USD',
             'Whole Foods', 'Groceries', 'ACC-001', 'debit', 'completed',
             'file1.parquet', ?::TIMESTAMPTZ, 'run-001'),
            ('tx-002', ?::TIMESTAMPTZ, '2026-02-18', 125.00, 'EUR',
             'Amazon', 'Shopping', 'ACC-002', 'debit', 'completed',
             'file1.parquet', ?::TIMESTAMPTZ, 'run-001'),
            ('tx-003', ?::TIMESTAMPTZ, '2026-02-17', 8.99, 'GBP',
             'Starbucks', 'Dining', 'ACC-001', 'debit', 'completed',
             'file1.parquet', ?::TIMESTAMPTZ, 'run-001'),
            ('tx-004', ?::TIMESTAMPTZ, '2026-02-17', 1500.00, 'JPY',
             'Best Buy', 'Electronics', 'ACC-003', 'debit', 'completed',
             'file1.parquet', ?::TIMESTAMPTZ, 'run-001'),
            ('tx-005', ?::TIMESTAMPTZ, '2026-02-17', 33.75, 'USD',
             'Target', 'Shopping', 'ACC-002', 'credit', 'completed',
             'file1.parquet', ?::TIMESTAMPTZ, 'run-001')
        """,
        [now, now, now, now, now, now, now, now, now, now],
    )

    # Run the transforms (copy the SQL from actual files)
    conn.execute("""
        CREATE OR REPLACE VIEW stg_transactions AS
        SELECT
            transaction_id,
            "timestamp"         AS transaction_timestamp,
            transaction_date,
            amount,
            currency,
            merchant_name,
            category,
            account_id,
            transaction_type,
            status,
            source_file,
            ingested_at,
            run_id
        FROM transactions
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE daily_spend_by_category AS
        SELECT
            transaction_date,
            category,
            currency,
            SUM(amount)            AS total_amount,
            COUNT(*)               AS transaction_count,
            SUM(amount) / COUNT(*) AS avg_amount
        FROM stg_transactions
        WHERE transaction_type = 'debit'
          AND status = 'completed'
        GROUP BY transaction_date, category, currency
        ORDER BY transaction_date, category, currency
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE monthly_account_summary AS
        SELECT
            DATE_TRUNC('month', transaction_date)::DATE AS month,
            account_id,
            currency,
            SUM(CASE WHEN transaction_type = 'debit'  THEN amount ELSE 0 END)
                AS total_debits,
            SUM(CASE WHEN transaction_type = 'credit' THEN amount ELSE 0 END)
                AS total_credits,
            SUM(CASE WHEN transaction_type = 'credit' THEN amount ELSE 0 END)
              - SUM(CASE WHEN transaction_type = 'debit' THEN amount ELSE 0 END)
                AS net_flow,
            COUNT(*) AS transaction_count
        FROM stg_transactions
        WHERE status = 'completed'
        GROUP BY DATE_TRUNC('month', transaction_date)::DATE,
                 account_id, currency
        ORDER BY month, account_id, currency
    """)

    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# T019: All checks pass on healthy data
# ---------------------------------------------------------------------------


class TestHealthyWarehouse:
    """End-to-end tests with healthy data — all checks should pass."""

    def test_all_checks_pass(self, healthy_warehouse: Path) -> None:
        """All 6 pre-built checks pass on a healthy warehouse."""
        result = run_checks(
            db_path=healthy_warehouse, checks_dir=CHECKS_DIR
        )
        assert result.status == RunStatus.PASSED
        assert result.total_checks == 6
        assert result.checks_passed == 6
        assert result.checks_failed == 0
        assert result.checks_errored == 0

    def test_all_individual_checks_pass(
        self, healthy_warehouse: Path
    ) -> None:
        """Each individual check result is PASS."""
        result = run_checks(
            db_path=healthy_warehouse, checks_dir=CHECKS_DIR
        )
        for cr in result.check_results:
            assert cr.status == CheckStatus.PASS, (
                f"Check '{cr.name}' should pass but got {cr.status.value}"
                + (f": {cr.error}" if cr.error else "")
            )

    def test_metadata_tables_created(
        self, healthy_warehouse: Path
    ) -> None:
        """check_runs and check_results tables exist after running."""
        result = run_checks(
            db_path=healthy_warehouse, checks_dir=CHECKS_DIR
        )

        conn = duckdb.connect(str(healthy_warehouse))
        try:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT table_name FROM information_schema.tables"
                ).fetchall()
            }
            assert "check_runs" in tables
            assert "check_results" in tables

            # Verify run row
            run_row = conn.execute(
                "SELECT status, total_checks FROM check_runs "
                "WHERE run_id = ?",
                [result.run_id],
            ).fetchone()
            assert run_row is not None
            assert run_row[0] == "passed"
            assert run_row[1] == 6
        finally:
            conn.close()

    def test_elapsed_under_5_seconds(
        self, healthy_warehouse: Path
    ) -> None:
        """SC-003: All checks complete in under 5 seconds."""
        result = run_checks(
            db_path=healthy_warehouse, checks_dir=CHECKS_DIR
        )
        assert result.elapsed_seconds < 5.0


# ---------------------------------------------------------------------------
# T020: Injected failures detected correctly
# ---------------------------------------------------------------------------


class TestInjectedFailures:
    """Tests that injected data problems are correctly detected."""

    def test_empty_mart_detected(
        self, healthy_warehouse: Path
    ) -> None:
        """Empty mart table causes mart_not_empty check to fail."""
        # Truncate one mart
        conn = duckdb.connect(str(healthy_warehouse))
        conn.execute("DELETE FROM daily_spend_by_category")
        conn.close()

        result = run_checks(
            db_path=healthy_warehouse, checks_dir=CHECKS_DIR
        )

        mart_check = next(
            cr
            for cr in result.check_results
            if cr.name == "mart_not_empty"
        )
        assert mart_check.status == CheckStatus.FAIL
        assert mart_check.violation_count >= 1

        # Critical check failure means overall FAILED
        assert result.status == RunStatus.FAILED

    def test_duplicate_daily_grain_detected(
        self, healthy_warehouse: Path
    ) -> None:
        """Duplicate grain in daily_spend_by_category is detected."""
        conn = duckdb.connect(str(healthy_warehouse))
        # Insert a duplicate grain row
        conn.execute("""
            INSERT INTO daily_spend_by_category
            SELECT * FROM daily_spend_by_category LIMIT 1
        """)
        conn.close()

        result = run_checks(
            db_path=healthy_warehouse, checks_dir=CHECKS_DIR
        )

        grain_check = next(
            cr
            for cr in result.check_results
            if cr.name == "unique_daily_spend_grain"
        )
        assert grain_check.status == CheckStatus.FAIL
        assert grain_check.violation_count >= 1

    def test_duplicate_monthly_grain_detected(
        self, healthy_warehouse: Path
    ) -> None:
        """Duplicate grain in monthly_account_summary is detected."""
        conn = duckdb.connect(str(healthy_warehouse))
        conn.execute("""
            INSERT INTO monthly_account_summary
            SELECT * FROM monthly_account_summary LIMIT 1
        """)
        conn.close()

        result = run_checks(
            db_path=healthy_warehouse, checks_dir=CHECKS_DIR
        )

        grain_check = next(
            cr
            for cr in result.check_results
            if cr.name == "unique_monthly_summary_grain"
        )
        assert grain_check.status == CheckStatus.FAIL

    def test_invalid_currency_detected(
        self, healthy_warehouse: Path
    ) -> None:
        """Currency value outside accepted set is detected."""
        conn = duckdb.connect(str(healthy_warehouse))
        conn.execute("""
            INSERT INTO transactions VALUES
                ('tx-bad', NOW(), '2026-02-18', 10.0, 'XYZ',
                 'Bad Store', 'Other', 'ACC-999', 'debit', 'completed',
                 'file2.parquet', NOW(), 'run-002')
        """)
        conn.close()

        result = run_checks(
            db_path=healthy_warehouse, checks_dir=CHECKS_DIR
        )

        currency_check = next(
            cr
            for cr in result.check_results
            if cr.name == "accepted_values_currency"
        )
        assert currency_check.status == CheckStatus.FAIL
        assert currency_check.violation_count >= 1
        # Should capture the bad row
        assert any(
            s.get("currency") == "XYZ"
            for s in currency_check.sample_violations
        )

    def test_stale_data_detected(self, tmp_path: Path) -> None:
        """Data older than 48 hours triggers freshness check failure."""
        db_path = tmp_path / "stale.duckdb"
        conn = duckdb.connect(str(db_path))

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
        # Insert with ingested_at 72 hours ago
        conn.execute("""
            INSERT INTO transactions VALUES
                ('tx-old', NOW() - INTERVAL '72 hours',
                 '2026-02-16', 50.0, 'USD', 'Store', 'Food',
                 'ACC-001', 'debit', 'completed', 'file.parquet',
                 NOW() - INTERVAL '72 hours', 'run-old')
        """)

        # Create staging and marts
        conn.execute("""
            CREATE OR REPLACE VIEW stg_transactions AS
            SELECT transaction_id, "timestamp" AS transaction_timestamp,
                   transaction_date, amount, currency, merchant_name,
                   category, account_id, transaction_type, status,
                   source_file, ingested_at, run_id
            FROM transactions
        """)
        conn.execute("""
            CREATE OR REPLACE TABLE daily_spend_by_category AS
            SELECT transaction_date, category, currency,
                   SUM(amount) AS total_amount, COUNT(*) AS transaction_count,
                   SUM(amount) / COUNT(*) AS avg_amount
            FROM stg_transactions
            WHERE transaction_type = 'debit' AND status = 'completed'
            GROUP BY transaction_date, category, currency
        """)
        conn.execute("""
            CREATE OR REPLACE TABLE monthly_account_summary AS
            SELECT DATE_TRUNC('month', transaction_date)::DATE AS month,
                   account_id, currency,
                   SUM(CASE WHEN transaction_type='debit' THEN amount ELSE 0 END)
                       AS total_debits,
                   SUM(CASE WHEN transaction_type='credit' THEN amount ELSE 0 END)
                       AS total_credits,
                   SUM(CASE WHEN transaction_type='credit' THEN amount ELSE 0 END)
                     - SUM(CASE WHEN transaction_type='debit' THEN amount ELSE 0 END)
                       AS net_flow,
                   COUNT(*) AS transaction_count
            FROM stg_transactions WHERE status = 'completed'
            GROUP BY DATE_TRUNC('month', transaction_date)::DATE,
                     account_id, currency
        """)
        conn.close()

        result = run_checks(db_path=db_path, checks_dir=CHECKS_DIR)

        freshness_check = next(
            cr for cr in result.check_results if cr.name == "freshness"
        )
        assert freshness_check.status == CheckStatus.FAIL

    def test_row_count_mismatch_detected(
        self, healthy_warehouse: Path
    ) -> None:
        """Row count mismatch between staging and source is detected.

        We break the view to filter out some rows, causing a mismatch.
        """
        conn = duckdb.connect(str(healthy_warehouse))
        # Replace the view with a filtered version
        conn.execute("""
            CREATE OR REPLACE VIEW stg_transactions AS
            SELECT transaction_id, "timestamp" AS transaction_timestamp,
                   transaction_date, amount, currency, merchant_name,
                   category, account_id, transaction_type, status,
                   source_file, ingested_at, run_id
            FROM transactions
            WHERE currency != 'JPY'
        """)
        conn.close()

        result = run_checks(
            db_path=healthy_warehouse, checks_dir=CHECKS_DIR
        )

        row_check = next(
            cr
            for cr in result.check_results
            if cr.name == "row_count_staging"
        )
        assert row_check.status == CheckStatus.FAIL

    def test_missing_mart_tables_error_not_crash(
        self, tmp_path: Path
    ) -> None:
        """Missing mart tables produce per-check errors, not crash."""
        db_path = tmp_path / "bare.duckdb"
        conn = duckdb.connect(str(db_path))
        # Only create transactions + staging, no marts
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
        conn.execute("INSERT INTO transactions VALUES "
                     "('tx-1', NOW(), '2026-02-18', 10.0, 'USD', "
                     "'Store', 'Food', 'ACC-1', 'debit', 'completed', "
                     "'f.parquet', NOW(), 'run-1')")
        conn.execute("""
            CREATE OR REPLACE VIEW stg_transactions AS
            SELECT transaction_id, "timestamp" AS transaction_timestamp,
                   transaction_date, amount, currency, merchant_name,
                   category, account_id, transaction_type, status,
                   source_file, ingested_at, run_id
            FROM transactions
        """)
        conn.close()

        # Should not crash — checks referencing missing tables get ERROR
        result = run_checks(db_path=db_path, checks_dir=CHECKS_DIR)
        assert result.checks_errored >= 1
        # Errors on missing tables should be per-check, not runner-level
        assert result.error_message is None

    def test_sample_violations_in_results_table(
        self, healthy_warehouse: Path
    ) -> None:
        """Violation samples are stored as JSON in check_results."""
        conn = duckdb.connect(str(healthy_warehouse))
        conn.execute("""
            INSERT INTO transactions VALUES
                ('tx-bad-cur', NOW(), '2026-02-18', 5.0, 'BRL',
                 'Bad', 'Other', 'ACC-999', 'debit', 'completed',
                 'f.parquet', NOW(), 'run-2')
        """)
        conn.close()

        result = run_checks(
            db_path=healthy_warehouse, checks_dir=CHECKS_DIR
        )

        conn = duckdb.connect(str(healthy_warehouse))
        try:
            row = conn.execute(
                "SELECT sample_violations FROM check_results "
                "WHERE run_id = ? AND check_name = 'accepted_values_currency'",
                [result.run_id],
            ).fetchone()
            assert row is not None
            assert row[0] is not None
            samples = json.loads(row[0])
            assert len(samples) >= 1
            assert any(s.get("currency") == "BRL" for s in samples)
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


class TestIdempotency:
    """Verify checks are idempotent (CHK023, CHK024)."""

    def test_same_results_on_repeated_runs(
        self, healthy_warehouse: Path
    ) -> None:
        """Running checks twice produces the same pass/fail outcomes."""
        result1 = run_checks(
            db_path=healthy_warehouse, checks_dir=CHECKS_DIR
        )
        result2 = run_checks(
            db_path=healthy_warehouse, checks_dir=CHECKS_DIR
        )

        assert result1.status == result2.status
        assert result1.checks_passed == result2.checks_passed
        assert result1.checks_failed == result2.checks_failed

    def test_separate_run_rows(
        self, healthy_warehouse: Path
    ) -> None:
        """Running checks twice creates two separate check_runs rows."""
        result1 = run_checks(
            db_path=healthy_warehouse, checks_dir=CHECKS_DIR
        )
        result2 = run_checks(
            db_path=healthy_warehouse, checks_dir=CHECKS_DIR
        )

        assert result1.run_id != result2.run_id

        conn = duckdb.connect(str(healthy_warehouse))
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM check_runs"
            ).fetchone()
            assert count is not None
            assert count[0] == 2
        finally:
            conn.close()
