"""Data quality tests for the SQL transformation layer (6Cs validation).

Validates correctness, completeness, consistency, and idempotency of
all SQL transform outputs: stg_transactions, daily_spend_by_category,
and monthly_account_summary.

Covers CHK001-CHK019 from specs/003-sql-transformations/checklists/data-quality.md.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import pytest

from src.transformer.models import TransformStatus
from src.transformer.runner import run_transforms

# Path to the actual SQL transform files
TRANSFORMS_DIR = Path(__file__).resolve().parent.parent.parent / "src" / "transforms"


@pytest.fixture()
def warehouse_db(tmp_path: Path) -> Path:
    """Create a temporary DuckDB database with Feature 002 schema and test data.

    Returns:
        Path to the temporary database file.
    """
    db_path = tmp_path / "quality_warehouse.duckdb"
    conn = duckdb.connect(str(db_path))

    conn.execute("""
        CREATE TABLE transactions (
            transaction_id     VARCHAR PRIMARY KEY,
            "timestamp"        TIMESTAMPTZ NOT NULL,
            amount             DOUBLE NOT NULL,
            currency           VARCHAR NOT NULL,
            merchant_name      VARCHAR NOT NULL,
            category           VARCHAR NOT NULL,
            account_id         VARCHAR NOT NULL,
            transaction_type   VARCHAR NOT NULL,
            status             VARCHAR NOT NULL,
            transaction_date   DATE NOT NULL,
            source_file        VARCHAR NOT NULL,
            ingested_at        TIMESTAMPTZ NOT NULL,
            run_id             VARCHAR NOT NULL
        )
    """)

    # Deliberately diverse data: multiple dates, categories, accounts,
    # currencies, transaction types, and statuses to exercise all filters.
    _ts = "'2026-02-19 00:00:00+00'"
    _f = "'file1.parquet'"
    _r = "'run-001'"
    conn.execute(f"""
        INSERT INTO transactions VALUES
        ('txn-001', '2026-01-15 10:00:00+00', 50.00,  'USD',
         'Walmart',     'Groceries', 'ACC-00001', 'debit',
         'completed', '2026-01-15', {_f}, {_ts}, {_r}),
        ('txn-002', '2026-01-15 11:00:00+00', 25.00,  'USD',
         'Starbucks',   'Dining',    'ACC-00001', 'debit',
         'completed', '2026-01-15', {_f}, {_ts}, {_r}),
        ('txn-003', '2026-01-15 12:00:00+00', 30.00,  'EUR',
         'Carrefour',   'Groceries', 'ACC-00002', 'debit',
         'completed', '2026-01-15', {_f}, {_ts}, {_r}),
        ('txn-004', '2026-01-16 09:00:00+00', 100.00, 'USD',
         'Amazon',      'Shopping',  'ACC-00001', 'debit',
         'completed', '2026-01-16', {_f}, {_ts}, {_r}),
        ('txn-005', '2026-02-01 10:00:00+00', 75.00,  'USD',
         'Target',      'Groceries', 'ACC-00002', 'debit',
         'completed', '2026-02-01', {_f}, {_ts}, {_r}),
        ('txn-006', '2026-01-15 14:00:00+00', 200.00, 'USD',
         'Employer',    'Income',    'ACC-00001', 'credit',
         'completed', '2026-01-15', {_f}, {_ts}, {_r}),
        ('txn-007', '2026-02-01 14:00:00+00', 150.00, 'USD',
         'Employer',    'Income',    'ACC-00002', 'credit',
         'completed', '2026-02-01', {_f}, {_ts}, {_r}),
        ('txn-008', '2026-01-15 15:00:00+00', 999.00, 'USD',
         'Scam LLC',    'Shopping',  'ACC-00001', 'debit',
         'failed',    '2026-01-15', {_f}, {_ts}, {_r}),
        ('txn-009', '2026-01-16 15:00:00+00', 45.00,  'USD',
         'Gas Station', 'Transport', 'ACC-00002', 'debit',
         'pending',   '2026-01-16', {_f}, {_ts}, {_r}),
        ('txn-010', '2026-01-15 16:00:00+00', 500.00, 'EUR',
         'EU Employer', 'Income',    'ACC-00002', 'credit',
         'completed', '2026-01-15', {_f}, {_ts}, {_r})
    """)

    conn.close()
    return db_path


@pytest.fixture()
def transforms_dir(tmp_path: Path) -> Path:
    """Copy actual transform SQL files to a temp directory.

    Returns:
        Path to the temporary transforms directory.
    """
    dest = tmp_path / "transforms"
    shutil.copytree(TRANSFORMS_DIR, dest)
    return dest


@pytest.fixture()
def executed_db(warehouse_db: Path, transforms_dir: Path) -> Path:
    """Run transforms and return the database path for querying.

    Returns:
        Path to the DuckDB database with transforms applied.
    """
    result = run_transforms(db_path=warehouse_db, transforms_dir=transforms_dir)
    assert result.status == TransformStatus.COMPLETED, (
        f"Transforms failed: {result.error_message}"
    )
    return warehouse_db


# ============================================================================
# Staging Layer Quality (CHK001-CHK005)
# ============================================================================


class TestStagingQuality:
    """Quality tests for the stg_transactions staging view."""

    def test_chk001_row_count_matches_source(self, executed_db: Path) -> None:
        """CHK001: stg_transactions row count matches transactions exactly."""
        conn = duckdb.connect(str(executed_db))
        source_count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        staging_count = conn.execute("SELECT COUNT(*) FROM stg_transactions").fetchone()[0]
        conn.close()

        assert staging_count == source_count, (
            f"stg_transactions has {staging_count} rows, expected {source_count}"
        )

    def test_chk002_no_unexpected_nulls(self, executed_db: Path) -> None:
        """CHK002: stg_transactions has no NULL values in NOT NULL source columns."""
        conn = duckdb.connect(str(executed_db))
        not_null_columns = [
            "transaction_id", "transaction_timestamp", "amount", "currency",
            "merchant_name", "category", "account_id", "transaction_type",
            "status", "transaction_date", "source_file", "ingested_at", "run_id",
        ]
        for col in not_null_columns:
            null_count = conn.execute(
                f'SELECT COUNT(*) FROM stg_transactions WHERE "{col}" IS NULL'
            ).fetchone()[0]
            assert null_count == 0, f"Column {col} has {null_count} NULL values"
        conn.close()

    def test_chk003_timestamp_column_renamed(self, executed_db: Path) -> None:
        """CHK003: stg_transactions.transaction_timestamp maps from transactions.timestamp."""
        conn = duckdb.connect(str(executed_db))
        # Verify column exists in staging
        columns = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'stg_transactions'"
        ).fetchall()
        col_names = {row[0] for row in columns}
        conn.close()

        assert "transaction_timestamp" in col_names, (
            "Missing transaction_timestamp column in stg_transactions"
        )
        assert "timestamp" not in col_names, (
            "Raw 'timestamp' column should be renamed to 'transaction_timestamp'"
        )

    def test_chk004_transaction_date_is_valid(self, executed_db: Path) -> None:
        """CHK004: stg_transactions.transaction_date is a valid DATE."""
        conn = duckdb.connect(str(executed_db))
        col_type = conn.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_name = 'stg_transactions' AND column_name = 'transaction_date'"
        ).fetchone()[0]

        # Verify no NULLs and dates are in a reasonable range
        invalid_dates = conn.execute(
            "SELECT COUNT(*) FROM stg_transactions "
            "WHERE transaction_date IS NULL "
            "OR transaction_date < '2020-01-01' "
            "OR transaction_date > '2030-12-31'"
        ).fetchone()[0]
        conn.close()

        assert col_type == "DATE", f"Expected DATE type, got {col_type}"
        assert invalid_dates == 0, f"Found {invalid_dates} invalid transaction dates"

    def test_chk005_lineage_columns_pass_through(self, executed_db: Path) -> None:
        """CHK005: Lineage columns pass through without alteration."""
        conn = duckdb.connect(str(executed_db))
        # Compare lineage columns between source and staging for first record
        source = conn.execute(
            "SELECT source_file, ingested_at, run_id FROM transactions ORDER BY transaction_id"
        ).fetchall()
        staging = conn.execute(
            "SELECT source_file, ingested_at, run_id FROM stg_transactions ORDER BY transaction_id"
        ).fetchall()
        conn.close()

        assert source == staging, (
            "Lineage columns differ between transactions and stg_transactions"
        )


# ============================================================================
# Daily Spend by Category Mart (CHK006-CHK010)
# ============================================================================


class TestDailySpendQuality:
    """Quality tests for the daily_spend_by_category mart table."""

    def test_chk006_only_completed_debits(self, executed_db: Path) -> None:
        """CHK006: daily_spend_by_category only includes completed debit transactions."""
        conn = duckdb.connect(str(executed_db))
        # Count completed debits in source
        expected_count = conn.execute(
            "SELECT COUNT(*) FROM stg_transactions "
            "WHERE transaction_type = 'debit' AND status = 'completed'"
        ).fetchone()[0]

        # Sum of transaction_count in mart should equal the source count
        mart_count = conn.execute(
            "SELECT SUM(transaction_count) FROM daily_spend_by_category"
        ).fetchone()[0]
        conn.close()

        assert mart_count == expected_count, (
            f"Mart has {mart_count} transaction count, expected {expected_count} completed debits"
        )

    def test_chk007_total_amount_matches_manual(self, executed_db: Path) -> None:
        """CHK007: total_amount matches SUM(amount) from manual aggregation."""
        conn = duckdb.connect(str(executed_db))
        # Manual aggregation from staging
        manual = conn.execute("""
            SELECT transaction_date, category, currency, SUM(amount) AS total_amount
            FROM stg_transactions
            WHERE transaction_type = 'debit' AND status = 'completed'
            GROUP BY transaction_date, category, currency
            ORDER BY transaction_date, category, currency
        """).fetchall()

        # Mart values
        mart = conn.execute("""
            SELECT transaction_date, category, currency, total_amount
            FROM daily_spend_by_category
            ORDER BY transaction_date, category, currency
        """).fetchall()
        conn.close()

        assert len(manual) == len(mart), (
            f"Row count mismatch: manual={len(manual)}, mart={len(mart)}"
        )
        for m_row, mart_row in zip(manual, mart, strict=True):
            assert m_row == mart_row, f"Mismatch: manual={m_row}, mart={mart_row}"

    def test_chk008_transaction_count_matches_manual(self, executed_db: Path) -> None:
        """CHK008: transaction_count matches COUNT(*) from manual aggregation."""
        conn = duckdb.connect(str(executed_db))
        manual = conn.execute("""
            SELECT transaction_date, category, currency, COUNT(*) AS cnt
            FROM stg_transactions
            WHERE transaction_type = 'debit' AND status = 'completed'
            GROUP BY transaction_date, category, currency
            ORDER BY transaction_date, category, currency
        """).fetchall()

        mart = conn.execute("""
            SELECT transaction_date, category, currency, transaction_count
            FROM daily_spend_by_category
            ORDER BY transaction_date, category, currency
        """).fetchall()
        conn.close()

        for m_row, mart_row in zip(manual, mart, strict=True):
            assert m_row == mart_row, f"Count mismatch: manual={m_row}, mart={mart_row}"

    def test_chk009_avg_amount_calculation(self, executed_db: Path) -> None:
        """CHK009: avg_amount equals total_amount / transaction_count."""
        conn = duckdb.connect(str(executed_db))
        rows = conn.execute("""
            SELECT total_amount, transaction_count, avg_amount
            FROM daily_spend_by_category
        """).fetchall()
        conn.close()

        for total, count, avg in rows:
            expected_avg = total / count
            assert abs(avg - expected_avg) < 1e-10, (
                f"avg_amount {avg} != total_amount/count {expected_avg}"
            )

    def test_chk010_grain_uniqueness(self, executed_db: Path) -> None:
        """CHK010: No duplicate keys on (transaction_date, category, currency)."""
        conn = duckdb.connect(str(executed_db))
        total = conn.execute("SELECT COUNT(*) FROM daily_spend_by_category").fetchone()[0]
        distinct = conn.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT DISTINCT transaction_date, category, currency "
            "  FROM daily_spend_by_category"
            ")"
        ).fetchone()[0]
        conn.close()

        assert total == distinct, (
            f"Grain violation: {total} rows but only {distinct} distinct keys"
        )


# ============================================================================
# Monthly Account Summary Mart (CHK011-CHK015)
# ============================================================================


class TestMonthlyAccountSummaryQuality:
    """Quality tests for the monthly_account_summary mart table."""

    def test_chk011_only_completed_transactions(self, executed_db: Path) -> None:
        """CHK011: monthly_account_summary only includes completed transactions."""
        conn = duckdb.connect(str(executed_db))
        # Total transactions in mart
        mart_count = conn.execute(
            "SELECT SUM(transaction_count) FROM monthly_account_summary"
        ).fetchone()[0]

        # Completed transactions in source
        expected = conn.execute(
            "SELECT COUNT(*) FROM stg_transactions WHERE status = 'completed'"
        ).fetchone()[0]
        conn.close()

        assert mart_count == expected, (
            f"Mart has {mart_count} txns, expected {expected} completed"
        )

    def test_chk012_total_debits_correct(self, executed_db: Path) -> None:
        """CHK012: total_debits matches filtered SUM(amount) WHERE debit."""
        conn = duckdb.connect(str(executed_db))
        manual = conn.execute("""
            SELECT DATE_TRUNC('month', transaction_date)::DATE AS month,
                   account_id, currency,
                   SUM(CASE WHEN transaction_type = 'debit' THEN amount ELSE 0 END) AS total_debits
            FROM stg_transactions
            WHERE status = 'completed'
            GROUP BY DATE_TRUNC('month', transaction_date)::DATE, account_id, currency
            ORDER BY month, account_id, currency
        """).fetchall()

        mart = conn.execute("""
            SELECT month, account_id, currency, total_debits
            FROM monthly_account_summary
            ORDER BY month, account_id, currency
        """).fetchall()
        conn.close()

        for m_row, mart_row in zip(manual, mart, strict=True):
            assert m_row == mart_row, f"Debit mismatch: manual={m_row}, mart={mart_row}"

    def test_chk013_total_credits_correct(self, executed_db: Path) -> None:
        """CHK013: total_credits matches filtered SUM(amount) WHERE credit."""
        conn = duckdb.connect(str(executed_db))
        manual = conn.execute("""
            SELECT DATE_TRUNC('month', transaction_date)::DATE AS month,
                   account_id, currency,
                   SUM(CASE WHEN transaction_type = 'credit'
                       THEN amount ELSE 0 END) AS total_credits
            FROM stg_transactions
            WHERE status = 'completed'
            GROUP BY DATE_TRUNC('month', transaction_date)::DATE, account_id, currency
            ORDER BY month, account_id, currency
        """).fetchall()

        mart = conn.execute("""
            SELECT month, account_id, currency, total_credits
            FROM monthly_account_summary
            ORDER BY month, account_id, currency
        """).fetchall()
        conn.close()

        for m_row, mart_row in zip(manual, mart, strict=True):
            assert m_row == mart_row, f"Credit mismatch: manual={m_row}, mart={mart_row}"

    def test_chk014_net_flow_calculation(self, executed_db: Path) -> None:
        """CHK014: net_flow equals total_credits - total_debits."""
        conn = duckdb.connect(str(executed_db))
        rows = conn.execute("""
            SELECT total_debits, total_credits, net_flow
            FROM monthly_account_summary
        """).fetchall()
        conn.close()

        for debits, credits, net in rows:
            expected = credits - debits
            assert abs(net - expected) < 1e-10, (
                f"net_flow {net} != credits - debits ({credits} - {debits} = {expected})"
            )

    def test_chk015_grain_uniqueness(self, executed_db: Path) -> None:
        """CHK015: No duplicate keys on (month, account_id, currency)."""
        conn = duckdb.connect(str(executed_db))
        total = conn.execute("SELECT COUNT(*) FROM monthly_account_summary").fetchone()[0]
        distinct = conn.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT DISTINCT month, account_id, currency "
            "  FROM monthly_account_summary"
            ")"
        ).fetchone()[0]
        conn.close()

        assert total == distinct, (
            f"Grain violation: {total} rows but only {distinct} distinct keys"
        )


# ============================================================================
# Idempotency (CHK016-CHK019)
# ============================================================================


class TestIdempotency:
    """Verify that running transforms multiple times produces identical results."""

    def test_chk016_staging_idempotent(
        self, warehouse_db: Path, transforms_dir: Path
    ) -> None:
        """CHK016: Running transforms twice produces identical stg_transactions."""
        # First run
        run_transforms(db_path=warehouse_db, transforms_dir=transforms_dir)
        conn = duckdb.connect(str(warehouse_db))
        stg_1 = conn.execute(
            "SELECT * FROM stg_transactions ORDER BY transaction_id"
        ).fetchall()
        conn.close()

        # Second run
        run_transforms(db_path=warehouse_db, transforms_dir=transforms_dir)
        conn = duckdb.connect(str(warehouse_db))
        stg_2 = conn.execute(
            "SELECT * FROM stg_transactions ORDER BY transaction_id"
        ).fetchall()
        conn.close()

        assert stg_1 == stg_2, "stg_transactions changed between runs"

    def test_chk017_daily_spend_idempotent(
        self, warehouse_db: Path, transforms_dir: Path
    ) -> None:
        """CHK017: Running transforms twice produces identical daily_spend_by_category."""
        run_transforms(db_path=warehouse_db, transforms_dir=transforms_dir)
        conn = duckdb.connect(str(warehouse_db))
        ds_1 = conn.execute(
            "SELECT * FROM daily_spend_by_category ORDER BY transaction_date, category, currency"
        ).fetchall()
        conn.close()

        run_transforms(db_path=warehouse_db, transforms_dir=transforms_dir)
        conn = duckdb.connect(str(warehouse_db))
        ds_2 = conn.execute(
            "SELECT * FROM daily_spend_by_category ORDER BY transaction_date, category, currency"
        ).fetchall()
        conn.close()

        assert ds_1 == ds_2, "daily_spend_by_category changed between runs"

    def test_chk018_monthly_summary_idempotent(
        self, warehouse_db: Path, transforms_dir: Path
    ) -> None:
        """CHK018: Running transforms twice produces identical monthly_account_summary."""
        run_transforms(db_path=warehouse_db, transforms_dir=transforms_dir)
        conn = duckdb.connect(str(warehouse_db))
        ms_1 = conn.execute(
            "SELECT * FROM monthly_account_summary ORDER BY month, account_id, currency"
        ).fetchall()
        conn.close()

        run_transforms(db_path=warehouse_db, transforms_dir=transforms_dir)
        conn = duckdb.connect(str(warehouse_db))
        ms_2 = conn.execute(
            "SELECT * FROM monthly_account_summary ORDER BY month, account_id, currency"
        ).fetchall()
        conn.close()

        assert ms_1 == ms_2, "monthly_account_summary changed between runs"

    def test_chk019_no_orphan_objects(
        self, warehouse_db: Path, transforms_dir: Path
    ) -> None:
        """CHK019: No orphan tables or views after re-execution."""
        run_transforms(db_path=warehouse_db, transforms_dir=transforms_dir)
        conn = duckdb.connect(str(warehouse_db))
        objects_1 = set(
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        )
        conn.close()

        run_transforms(db_path=warehouse_db, transforms_dir=transforms_dir)
        conn = duckdb.connect(str(warehouse_db))
        objects_2 = set(
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        )
        conn.close()

        # Only difference should be extra transform_runs rows (same table)
        assert objects_1 == objects_2, (
            f"Objects changed: added={objects_2 - objects_1}, removed={objects_1 - objects_2}"
        )
