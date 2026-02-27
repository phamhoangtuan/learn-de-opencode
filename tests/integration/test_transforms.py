"""Integration tests for the SQL transform pipeline.

Tests the full end-to-end transform flow using a temporary DuckDB database
populated with realistic test data matching the Feature 002 schema.
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
FACT_TRANSFORMS_DIR = Path(__file__).resolve().parent.parent.parent / "src" / "fact_transforms"


@pytest.fixture()
def warehouse_db(tmp_path: Path) -> Path:
    """Create a temporary DuckDB database with the Feature 002 schema and test data.

    Returns:
        Path to the temporary database file.
    """
    db_path = tmp_path / "test_warehouse.duckdb"
    conn = duckdb.connect(str(db_path))

    # Create the transactions table matching Feature 002 data model
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

    # Create dim_accounts_sk_seq and dim_accounts for mart__account_history transform
    conn.execute("CREATE SEQUENCE IF NOT EXISTS dim_accounts_sk_seq START 1;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_accounts (
            account_sk          BIGINT      PRIMARY KEY DEFAULT nextval('dim_accounts_sk_seq'),
            account_id          VARCHAR     NOT NULL,
            primary_currency    VARCHAR     NOT NULL,
            primary_category    VARCHAR     NOT NULL,
            transaction_count   BIGINT      NOT NULL,
            total_spend         DOUBLE      NOT NULL,
            first_seen          DATE        NOT NULL,
            last_seen           DATE        NOT NULL,
            row_hash            VARCHAR     NOT NULL,
            valid_from          TIMESTAMPTZ NOT NULL,
            valid_to            TIMESTAMPTZ,
            is_current          BOOLEAN     NOT NULL DEFAULT TRUE,
            run_id              VARCHAR     NOT NULL
        )
    """)

    # Insert realistic test data spanning multiple dates, categories, accounts, currencies
    conn.execute("""
        INSERT INTO transactions VALUES
        -- Completed debit transactions (should appear in daily_spend_by_category)
        ('txn-001', '2026-01-15 10:00:00+00', 50.00,  'USD', 'Walmart',
         'Groceries', 'ACC-00001', 'debit', 'completed',
         '2026-01-15', 'test.parquet', '2026-02-19 00:00:00+00', 'run-001'),
        ('txn-002', '2026-01-15 11:00:00+00', 25.00,  'USD', 'Starbucks',
         'Dining', 'ACC-00001', 'debit', 'completed',
         '2026-01-15', 'test.parquet', '2026-02-19 00:00:00+00', 'run-001'),
        ('txn-003', '2026-01-15 12:00:00+00', 30.00,  'EUR', 'Carrefour',
         'Groceries', 'ACC-00002', 'debit', 'completed',
         '2026-01-15', 'test.parquet', '2026-02-19 00:00:00+00', 'run-001'),
        ('txn-004', '2026-01-16 09:00:00+00', 100.00, 'USD', 'Amazon',
         'Shopping', 'ACC-00001', 'debit', 'completed',
         '2026-01-16', 'test.parquet', '2026-02-19 00:00:00+00', 'run-001'),
        ('txn-005', '2026-02-01 10:00:00+00', 75.00,  'USD', 'Target',
         'Groceries', 'ACC-00002', 'debit', 'completed',
         '2026-02-01', 'test.parquet', '2026-02-19 00:00:00+00', 'run-001'),
        -- Completed credit (monthly_account_summary only, NOT daily_spend)
        ('txn-006', '2026-01-15 14:00:00+00', 200.00, 'USD', 'Employer',
         'Income', 'ACC-00001', 'credit', 'completed',
         '2026-01-15', 'test.parquet', '2026-02-19 00:00:00+00', 'run-001'),
        ('txn-007', '2026-02-01 14:00:00+00', 150.00, 'USD', 'Employer',
         'Income', 'ACC-00002', 'credit', 'completed',
         '2026-02-01', 'test.parquet', '2026-02-19 00:00:00+00', 'run-001'),
        -- Pending/failed transactions (excluded from mart tables)
        ('txn-008', '2026-01-15 15:00:00+00', 999.00, 'USD', 'Scam LLC',
         'Shopping', 'ACC-00001', 'debit', 'failed',
         '2026-01-15', 'test.parquet', '2026-02-19 00:00:00+00', 'run-001'),
        ('txn-009', '2026-01-16 15:00:00+00', 45.00,  'USD', 'Gas Station',
         'Transport', 'ACC-00002', 'debit', 'pending',
         '2026-01-16', 'test.parquet', '2026-02-19 00:00:00+00', 'run-001'),
        -- EUR credit for multi-currency testing
        ('txn-010', '2026-01-15 16:00:00+00', 500.00, 'EUR', 'EU Employer',
         'Income', 'ACC-00002', 'credit', 'completed',
         '2026-01-15', 'test.parquet', '2026-02-19 00:00:00+00', 'run-001')
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


class TestFullTransformPipeline:
    """End-to-end integration tests for the transform pipeline."""

    def test_all_outputs_created(
        self, warehouse_db: Path, transforms_dir: Path
    ) -> None:
        """Running transforms creates all expected views and tables."""
        result = run_transforms(db_path=warehouse_db, transforms_dir=transforms_dir)

        assert result.status == TransformStatus.COMPLETED
        assert result.models_executed == 4
        assert result.models_failed == 0

        # Verify all outputs exist
        conn = duckdb.connect(str(warehouse_db))
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        table_names = {row[0] for row in tables}

        assert "stg_transactions" in table_names  # VIEW shows up in tables
        assert "daily_spend_by_category" in table_names
        assert "monthly_account_summary" in table_names
        assert "transform_runs" in table_names
        conn.close()

    def test_idempotent_execution(
        self, warehouse_db: Path, transforms_dir: Path
    ) -> None:
        """Running transforms twice produces identical output."""
        # First run
        result1 = run_transforms(db_path=warehouse_db, transforms_dir=transforms_dir)
        assert result1.status == TransformStatus.COMPLETED

        conn = duckdb.connect(str(warehouse_db))
        spend_count_1 = conn.execute(
            "SELECT COUNT(*) FROM daily_spend_by_category"
        ).fetchone()[0]
        summary_count_1 = conn.execute(
            "SELECT COUNT(*) FROM monthly_account_summary"
        ).fetchone()[0]
        conn.close()

        # Second run
        result2 = run_transforms(db_path=warehouse_db, transforms_dir=transforms_dir)
        assert result2.status == TransformStatus.COMPLETED

        conn = duckdb.connect(str(warehouse_db))
        spend_count_2 = conn.execute(
            "SELECT COUNT(*) FROM daily_spend_by_category"
        ).fetchone()[0]
        summary_count_2 = conn.execute(
            "SELECT COUNT(*) FROM monthly_account_summary"
        ).fetchone()[0]
        conn.close()

        assert spend_count_1 == spend_count_2
        assert summary_count_1 == summary_count_2

    def test_transform_runs_metadata(
        self, warehouse_db: Path, transforms_dir: Path
    ) -> None:
        """Each run creates a transform_runs metadata row."""
        run_transforms(db_path=warehouse_db, transforms_dir=transforms_dir)

        conn = duckdb.connect(str(warehouse_db))
        rows = conn.execute("SELECT * FROM transform_runs").fetchall()
        conn.close()

        assert len(rows) == 1
        row = rows[0]
        # Columns: run_id, started_at, completed_at, status,
        # models_executed, models_failed, elapsed_seconds, error_message
        assert row[3] == "completed"  # status
        assert row[4] == 4            # models_executed
        assert row[5] == 0            # models_failed
        assert row[6] > 0             # elapsed_seconds

    def test_multiple_runs_tracked(
        self, warehouse_db: Path, transforms_dir: Path
    ) -> None:
        """Multiple runs each get a unique row in transform_runs."""
        run_transforms(db_path=warehouse_db, transforms_dir=transforms_dir)
        run_transforms(db_path=warehouse_db, transforms_dir=transforms_dir)

        conn = duckdb.connect(str(warehouse_db))
        count = conn.execute("SELECT COUNT(*) FROM transform_runs").fetchone()[0]
        run_ids = conn.execute("SELECT run_id FROM transform_runs").fetchall()
        conn.close()

        assert count == 2
        assert run_ids[0][0] != run_ids[1][0]  # unique run IDs

    def test_execution_order_respected(
        self, warehouse_db: Path, transforms_dir: Path
    ) -> None:
        """Staging view is created before mart tables (verified by querying marts)."""
        result = run_transforms(db_path=warehouse_db, transforms_dir=transforms_dir)
        assert result.status == TransformStatus.COMPLETED

        # If execution order was wrong, mart queries would fail
        # because they depend on stg_transactions
        conn = duckdb.connect(str(warehouse_db))
        spend_rows = conn.execute(
            "SELECT COUNT(*) FROM daily_spend_by_category"
        ).fetchone()[0]
        summary_rows = conn.execute(
            "SELECT COUNT(*) FROM monthly_account_summary"
        ).fetchone()[0]
        conn.close()

        assert spend_rows > 0
        assert summary_rows > 0


# ---------------------------------------------------------------------------
# Fact transform pipeline tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def fact_transforms_dir(tmp_path: Path) -> Path:
    """Copy actual fact transform SQL files to a temp directory.

    Returns:
        Path to the temporary fact transforms directory.
    """
    dest = tmp_path / "fact_transforms"
    shutil.copytree(FACT_TRANSFORMS_DIR, dest)
    return dest


@pytest.fixture()
def warehouse_with_dims(warehouse_db: Path) -> Path:
    """Extend warehouse_db with staging view and populated dim_accounts.

    Runs the staging transform first, then populates dim_accounts with
    SCD2 rows covering all transaction timestamps.

    Returns:
        Path to the database with staging + dim_accounts ready.
    """
    conn = duckdb.connect(str(warehouse_db))

    # Create staging view (needed before fact build)
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

    # Populate dim_accounts with current rows for test accounts
    conn.execute("""
        INSERT INTO dim_accounts
            (account_id, primary_currency, primary_category,
             transaction_count, total_spend, first_seen, last_seen,
             row_hash, valid_from, valid_to, is_current, run_id)
        VALUES
            ('ACC-00001', 'USD', 'Groceries', 5, 375.00,
             '2026-01-15', '2026-02-01', 'hash001',
             '2026-01-01 00:00:00+00'::TIMESTAMPTZ, NULL, TRUE, 'run-001'),
            ('ACC-00002', 'EUR', 'Groceries', 5, 755.00,
             '2026-01-15', '2026-02-01', 'hash002',
             '2026-01-01 00:00:00+00'::TIMESTAMPTZ, NULL, TRUE, 'run-001')
    """)

    conn.close()
    return warehouse_db


class TestFactTransformPipeline:
    """Integration tests for the fact transform pipeline."""

    def test_fct_transactions_created(
        self, warehouse_with_dims: Path, fact_transforms_dir: Path
    ) -> None:
        """Running fact transforms creates fct_transactions table."""
        result = run_transforms(
            db_path=warehouse_with_dims, transforms_dir=fact_transforms_dir
        )

        assert result.status == TransformStatus.COMPLETED
        assert result.models_executed == 1
        assert result.models_failed == 0

        conn = duckdb.connect(str(warehouse_with_dims))
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()
        table_names = {row[0] for row in tables}
        assert "fct_transactions" in table_names
        conn.close()

    def test_row_count_matches_staging(
        self, warehouse_with_dims: Path, fact_transforms_dir: Path
    ) -> None:
        """fct_transactions has same row count as stg_transactions."""
        run_transforms(
            db_path=warehouse_with_dims, transforms_dir=fact_transforms_dir
        )

        conn = duckdb.connect(str(warehouse_with_dims))
        stg_count = conn.execute(
            "SELECT COUNT(*) FROM stg_transactions"
        ).fetchone()[0]
        fct_count = conn.execute(
            "SELECT COUNT(*) FROM fct_transactions"
        ).fetchone()[0]
        conn.close()

        assert fct_count == stg_count
        assert fct_count == 10  # 10 test transactions

    def test_account_sk_populated(
        self, warehouse_with_dims: Path, fact_transforms_dir: Path
    ) -> None:
        """Matched accounts get valid surrogate keys, unmatched get -1."""
        run_transforms(
            db_path=warehouse_with_dims, transforms_dir=fact_transforms_dir
        )

        conn = duckdb.connect(str(warehouse_with_dims))
        # ACC-00001 and ACC-00002 are in dim_accounts
        matched = conn.execute(
            "SELECT COUNT(*) FROM fct_transactions "
            "WHERE account_sk > 0 AND account_id IN ('ACC-00001', 'ACC-00002')"
        ).fetchone()[0]
        assert matched > 0

        # No NULL account_sk values
        nulls = conn.execute(
            "SELECT COUNT(*) FROM fct_transactions WHERE account_sk IS NULL"
        ).fetchone()[0]
        assert nulls == 0
        conn.close()

    def test_no_duplicate_transaction_id(
        self, warehouse_with_dims: Path, fact_transforms_dir: Path
    ) -> None:
        """No duplicate transaction_id in fct_transactions."""
        run_transforms(
            db_path=warehouse_with_dims, transforms_dir=fact_transforms_dir
        )

        conn = duckdb.connect(str(warehouse_with_dims))
        dupes = conn.execute(
            "SELECT transaction_id, COUNT(*) AS cnt "
            "FROM fct_transactions "
            "GROUP BY transaction_id HAVING COUNT(*) > 1"
        ).fetchall()
        conn.close()

        assert len(dupes) == 0

    def test_idempotent_fact_build(
        self, warehouse_with_dims: Path, fact_transforms_dir: Path
    ) -> None:
        """Running fact transforms twice produces identical output."""
        run_transforms(
            db_path=warehouse_with_dims, transforms_dir=fact_transforms_dir
        )
        conn = duckdb.connect(str(warehouse_with_dims))
        count1 = conn.execute(
            "SELECT COUNT(*) FROM fct_transactions"
        ).fetchone()[0]
        conn.close()

        run_transforms(
            db_path=warehouse_with_dims, transforms_dir=fact_transforms_dir
        )
        conn = duckdb.connect(str(warehouse_with_dims))
        count2 = conn.execute(
            "SELECT COUNT(*) FROM fct_transactions"
        ).fetchone()[0]
        conn.close()

        assert count1 == count2
