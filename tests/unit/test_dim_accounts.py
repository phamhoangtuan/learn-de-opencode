"""Unit tests for Feature 009 SCD Type 2 dim_accounts implementation.

Covers:
    T010 - TestCreateDimTables
    T014 - TestBuildDimAccountsFirstRun
    T019 - TestChangeDetection
    T020 - TestRollbackAtomicity
    T023 - TestMartAccountHistory
    T028 - TestQualityChecks
    T034 - TestPIIClassification
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from src.dimensions.dim_accounts import (
    DimBuildError,
    build_dim_accounts,
    create_dim_tables,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CHECK_DIR = PROJECT_ROOT / "src" / "checks"

# ---------------------------------------------------------------------------
# Shared fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def conn() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB connection with transactions table and stg_transactions view."""
    c = duckdb.connect(":memory:")
    c.execute(
        """CREATE TABLE transactions (
            transaction_id VARCHAR PRIMARY KEY,
            "timestamp" TIMESTAMPTZ NOT NULL,
            amount DOUBLE NOT NULL,
            currency VARCHAR NOT NULL,
            merchant_name VARCHAR NOT NULL,
            category VARCHAR NOT NULL,
            account_id VARCHAR NOT NULL,
            transaction_type VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            transaction_date DATE NOT NULL,
            source_file VARCHAR NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL,
            run_id VARCHAR NOT NULL
        )"""
    )
    c.execute(
        """CREATE OR REPLACE VIEW stg_transactions AS
            SELECT transaction_id, "timestamp" AS transaction_timestamp, transaction_date,
                   amount, currency, merchant_name, category, account_id,
                   transaction_type, status, source_file, ingested_at, run_id
            FROM transactions"""
    )
    create_dim_tables(c)
    yield c
    c.close()


def insert_txn(
    conn: duckdb.DuckDBPyConnection,
    txn_id: str,
    account_id: str,
    currency: str,
    category: str,
    amount: float = 50.0,
    txn_date: str = "2026-01-15",
    run_id: str = "run-001",
) -> None:
    """Insert a single transaction row into the transactions table."""
    conn.execute(
        "INSERT INTO transactions VALUES"
        " (?,CURRENT_TIMESTAMP,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,?)",
        [
            txn_id,
            amount,
            currency,
            "Merchant",
            category,
            account_id,
            "debit",
            "completed",
            txn_date,
            "file.parquet",
            run_id,
        ],
    )


# ---------------------------------------------------------------------------
# T010 - TestCreateDimTables
# ---------------------------------------------------------------------------


class TestCreateDimTables:
    """T010: verify DDL created by create_dim_tables()."""

    def test_creates_dim_accounts_table(self, conn: duckdb.DuckDBPyConnection) -> None:
        """dim_accounts table must exist after create_dim_tables()."""
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
        assert "dim_accounts" in tables

    def test_creates_dim_build_runs_table(self, conn: duckdb.DuckDBPyConnection) -> None:
        """dim_build_runs table must exist after create_dim_tables()."""
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
        assert "dim_build_runs" in tables

    def test_creates_sequence(self, conn: duckdb.DuckDBPyConnection) -> None:
        """dim_accounts_sk_seq sequence must exist after create_dim_tables()."""
        sequences = {
            row[0]
            for row in conn.execute(
                "SELECT sequence_name FROM duckdb_sequences()"
            ).fetchall()
        }
        assert "dim_accounts_sk_seq" in sequences

    def test_idempotent(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Calling create_dim_tables twice must not raise any error."""
        create_dim_tables(conn)  # second call — no error expected

    def test_all_13_columns(self, conn: duckdb.DuckDBPyConnection) -> None:
        """dim_accounts must have exactly the 13 expected columns."""
        expected_columns = {
            "account_sk",
            "account_id",
            "primary_currency",
            "primary_category",
            "transaction_count",
            "total_spend",
            "first_seen",
            "last_seen",
            "row_hash",
            "valid_from",
            "valid_to",
            "is_current",
            "run_id",
        }
        actual_columns = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'dim_accounts'"
            ).fetchall()
        }
        assert actual_columns == expected_columns


# ---------------------------------------------------------------------------
# T014 - TestBuildDimAccountsFirstRun
# ---------------------------------------------------------------------------


class TestBuildDimAccountsFirstRun:
    """T014: first-run build with 5 distinct accounts covering currency/category logic."""

    @pytest.fixture(autouse=True)
    def _setup(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Insert transactions for 5 distinct accounts and run build_dim_accounts."""
        self.conn = conn

        # ACC-001: 3x USD, 2x EUR  → primary_currency=USD (3 > 2)
        insert_txn(conn, "t01", "ACC-001", "USD", "Food", run_id="run-001")
        insert_txn(conn, "t02", "ACC-001", "USD", "Food", run_id="run-001")
        insert_txn(conn, "t03", "ACC-001", "USD", "Food", run_id="run-001")
        insert_txn(conn, "t04", "ACC-001", "EUR", "Travel", run_id="run-001")
        insert_txn(conn, "t05", "ACC-001", "EUR", "Travel", run_id="run-001")

        # ACC-002: 3 EUR, 2 USD  → primary_currency=EUR
        insert_txn(conn, "t06", "ACC-002", "EUR", "Shopping", run_id="run-001")
        insert_txn(conn, "t07", "ACC-002", "EUR", "Shopping", run_id="run-001")
        insert_txn(conn, "t08", "ACC-002", "USD", "Shopping", run_id="run-001")
        insert_txn(conn, "t09", "ACC-002", "USD", "Shopping", run_id="run-001")
        insert_txn(conn, "t10", "ACC-002", "EUR", "Shopping", run_id="run-001")

        # ACC-003: 2x GBP, 1x USD  → primary_currency=GBP
        insert_txn(conn, "t11", "ACC-003", "GBP", "Food", run_id="run-001")
        insert_txn(conn, "t12", "ACC-003", "GBP", "Food", run_id="run-001")
        insert_txn(conn, "t13", "ACC-003", "USD", "Food", run_id="run-001")

        # ACC-004: 3x JPY  → primary_currency=JPY
        insert_txn(conn, "t14", "ACC-004", "JPY", "Entertainment", run_id="run-001")
        insert_txn(conn, "t15", "ACC-004", "JPY", "Entertainment", run_id="run-001")
        insert_txn(conn, "t16", "ACC-004", "JPY", "Entertainment", run_id="run-001")

        # ACC-TIE: exactly 3x USD and 3x EUR  → alpha tie-break → EUR (E < U)
        insert_txn(conn, "t17", "ACC-TIE", "USD", "Food", run_id="run-001")
        insert_txn(conn, "t18", "ACC-TIE", "USD", "Food", run_id="run-001")
        insert_txn(conn, "t19", "ACC-TIE", "USD", "Food", run_id="run-001")
        insert_txn(conn, "t20", "ACC-TIE", "EUR", "Food", run_id="run-001")
        insert_txn(conn, "t21", "ACC-TIE", "EUR", "Food", run_id="run-001")
        insert_txn(conn, "t22", "ACC-TIE", "EUR", "Food", run_id="run-001")

        self.result = build_dim_accounts(conn, run_id="run-001", run_date=date(2026, 1, 16))

    def _rows(self) -> list[dict]:
        """Fetch all dim_accounts rows as a list of dicts keyed by column name."""
        cursor = self.conn.execute("SELECT * FROM dim_accounts")
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row, strict=False)) for row in cursor.fetchall()]

    def _row_for(self, account_id: str) -> dict:
        rows = self._rows()
        matches = [r for r in rows if r["account_id"] == account_id]
        assert len(matches) == 1, f"Expected 1 row for {account_id}, got {len(matches)}"
        return matches[0]

    def test_five_rows_all_current(self) -> None:
        """First run should produce exactly 5 rows, all with is_current=TRUE."""
        rows = self._rows()
        assert len(rows) == 5
        assert all(r["is_current"] is True for r in rows)

    def test_valid_to_null(self) -> None:
        """All rows on first run must have valid_to IS NULL."""
        rows = self._rows()
        assert all(r["valid_to"] is None for r in rows)

    def test_primary_currency_mode(self) -> None:
        """primary_currency must reflect the most-frequent currency per account."""
        assert self._row_for("ACC-001")["primary_currency"] == "USD"
        assert self._row_for("ACC-002")["primary_currency"] == "EUR"
        assert self._row_for("ACC-003")["primary_currency"] == "GBP"
        assert self._row_for("ACC-004")["primary_currency"] == "JPY"

    def test_alpha_tiebreak(self) -> None:
        """On equal currency count, alphabetically first currency wins (EUR < USD)."""
        assert self._row_for("ACC-TIE")["primary_currency"] == "EUR"

    def test_transaction_count(self) -> None:
        """ACC-001 has 5 total transactions across all currencies."""
        assert self._row_for("ACC-001")["transaction_count"] == 5

    def test_run_id_set(self) -> None:
        """All rows must carry the run_id that was passed to build_dim_accounts."""
        rows = self._rows()
        assert all(r["run_id"] == "run-001" for r in rows)

    def test_dim_build_result_first_run(self) -> None:
        """First run: accounts_processed=5, new_versions=5, unchanged=0."""
        assert self.result.accounts_processed == 5
        assert self.result.new_versions == 5
        assert self.result.unchanged == 0

    def test_elapsed_seconds_positive(self) -> None:
        """elapsed_seconds must be a positive float."""
        assert self.result.elapsed_seconds > 0


# ---------------------------------------------------------------------------
# T019 - TestChangeDetection
# ---------------------------------------------------------------------------


class TestChangeDetection:
    """T019: SCD Type 2 change detection across two build runs."""

    @pytest.fixture(autouse=True)
    def _setup(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Prepare initial state with 3 accounts, run once, then add data and run again."""
        self.conn = conn

        # Initial data — ACC-001 starts with 3 USD, ACC-002 and ACC-003 stable
        insert_txn(conn, "t01", "ACC-001", "USD", "Food", run_id="run-001")
        insert_txn(conn, "t02", "ACC-001", "USD", "Food", run_id="run-001")
        insert_txn(conn, "t03", "ACC-001", "USD", "Food", run_id="run-001")
        insert_txn(conn, "t04", "ACC-002", "GBP", "Travel", run_id="run-001")
        insert_txn(conn, "t05", "ACC-002", "GBP", "Travel", run_id="run-001")
        insert_txn(conn, "t06", "ACC-003", "JPY", "Shopping", run_id="run-001")
        insert_txn(conn, "t07", "ACC-003", "JPY", "Shopping", run_id="run-001")

        build_dim_accounts(conn, run_id="run-001", run_date=date(2026, 2, 26))

        # Add more transactions to ACC-001 so primary_currency shifts from USD to EUR
        insert_txn(conn, "t08", "ACC-001", "EUR", "Food", run_id="run-002")
        insert_txn(conn, "t09", "ACC-001", "EUR", "Food", run_id="run-002")
        insert_txn(conn, "t10", "ACC-001", "EUR", "Food", run_id="run-002")
        insert_txn(conn, "t11", "ACC-001", "EUR", "Food", run_id="run-002")

        self.result2 = build_dim_accounts(conn, run_id="run-002", run_date=date(2026, 2, 27))

    def _rows_for(self, account_id: str) -> list[dict]:
        cursor = self.conn.execute(
            "SELECT * FROM dim_accounts WHERE account_id = ? ORDER BY valid_from",
            [account_id],
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row, strict=False)) for row in cursor.fetchall()]

    def test_old_row_expired(self) -> None:
        """ACC-001 old row must have is_current=FALSE and valid_to IS NOT NULL."""
        rows = self._rows_for("ACC-001")
        assert len(rows) == 2
        old_row = rows[0]
        assert old_row["is_current"] is False
        assert old_row["valid_to"] is not None

    def test_new_row_current(self) -> None:
        """ACC-001 new row must have is_current=TRUE and valid_to IS NULL."""
        rows = self._rows_for("ACC-001")
        assert len(rows) == 2
        new_row = rows[1]
        assert new_row["is_current"] is True
        assert new_row["valid_to"] is None

    def test_unchanged_accounts(self) -> None:
        """ACC-002 and ACC-003 must each have exactly 1 row and remain is_current=TRUE."""
        for account_id in ("ACC-002", "ACC-003"):
            rows = self._rows_for(account_id)
            assert len(rows) == 1, f"{account_id} should have 1 row"
            assert rows[0]["is_current"] is True

    def test_dim_build_result_change(self) -> None:
        """Second run: new_versions=1 (ACC-001 changed), unchanged=2."""
        assert self.result2.new_versions == 1
        assert self.result2.unchanged == 2


# ---------------------------------------------------------------------------
# T020 - TestRollbackAtomicity
# ---------------------------------------------------------------------------


class TestRollbackAtomicity:
    """T020: verify that a failed build leaves dim_accounts in its prior state."""

    def test_rollback_on_missing_stg_transactions(self, conn: duckdb.DuckDBPyConnection) -> None:
        """build_dim_accounts must raise DimBuildError and leave dim_accounts untouched."""
        # Pre-populate dim_accounts with 2 rows via direct insert
        conn.execute(
            """INSERT INTO dim_accounts (account_id, primary_currency, primary_category,
                transaction_count, total_spend, first_seen, last_seen, row_hash,
                valid_from, valid_to, is_current, run_id)
               VALUES ('PRE-001','USD','Food',1,100.0,'2026-01-01','2026-01-01','hash1',
                       '2026-01-01T00:00:00+00:00',NULL,TRUE,'pre-run')"""
        )
        conn.execute(
            """INSERT INTO dim_accounts (account_id, primary_currency, primary_category,
                transaction_count, total_spend, first_seen, last_seen, row_hash,
                valid_from, valid_to, is_current, run_id)
               VALUES ('PRE-002','EUR','Travel',2,200.0,'2026-01-01','2026-01-01','hash2',
                       '2026-01-01T00:00:00+00:00',NULL,TRUE,'pre-run')"""
        )

        count_before = conn.execute("SELECT COUNT(*) FROM dim_accounts").fetchone()[0]
        assert count_before == 2

        # Drop stg_transactions view to force a failure during build
        conn.execute("DROP VIEW IF EXISTS stg_transactions")

        with pytest.raises(DimBuildError):
            build_dim_accounts(conn, run_id="bad-run", run_date=date(2026, 2, 26))

        # dim_accounts must still have the original 2 rows
        count_after = conn.execute("SELECT COUNT(*) FROM dim_accounts").fetchone()[0]
        assert count_after == 2

        # Both rows must still be current — no partial state changes
        current_count = conn.execute(
            "SELECT COUNT(*) FROM dim_accounts WHERE is_current = TRUE"
        ).fetchone()[0]
        assert current_count == 2


# ---------------------------------------------------------------------------
# T023 - TestMartAccountHistory
# ---------------------------------------------------------------------------


class TestMartAccountHistory:
    """T023: validate the mart__account_history view computed from a 2-version dim."""

    @pytest.fixture(autouse=True)
    def _setup(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Build two versions for ACC-001 and create the mart view."""
        self.conn = conn

        # Run 1 — ACC-001 starts with USD majority
        insert_txn(conn, "t01", "ACC-001", "USD", "Food", run_id="run-001")
        insert_txn(conn, "t02", "ACC-001", "USD", "Food", run_id="run-001")
        insert_txn(conn, "t03", "ACC-001", "USD", "Food", run_id="run-001")
        insert_txn(conn, "t04", "ACC-002", "GBP", "Travel", run_id="run-001")

        build_dim_accounts(conn, run_id="run-001", run_date=date(2026, 1, 10))

        # Run 2 — add EUR transactions to ACC-001 to trigger a version bump
        insert_txn(conn, "t05", "ACC-001", "EUR", "Food", run_id="run-002")
        insert_txn(conn, "t06", "ACC-001", "EUR", "Food", run_id="run-002")
        insert_txn(conn, "t07", "ACC-001", "EUR", "Food", run_id="run-002")
        insert_txn(conn, "t08", "ACC-001", "EUR", "Food", run_id="run-002")

        build_dim_accounts(conn, run_id="run-002", run_date=date(2026, 1, 15))

        # Create the mart view
        conn.execute(
            """CREATE OR REPLACE VIEW mart__account_history AS
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY account_id ORDER BY valid_from
                    ) AS version_number,
                    CASE WHEN is_current
                        THEN CAST(current_date - CAST(valid_from AS DATE) AS INTEGER)
                        ELSE CAST(CAST(valid_to AS DATE) - CAST(valid_from AS DATE) AS INTEGER)
                    END AS version_duration_days
                FROM dim_accounts
                ORDER BY account_id, valid_from"""
        )

    def _mart_rows_for(self, account_id: str) -> list[dict]:
        cursor = self.conn.execute(
            "SELECT * FROM mart__account_history WHERE account_id = ? ORDER BY valid_from",
            [account_id],
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row, strict=False)) for row in cursor.fetchall()]

    def test_version_number_increments(self) -> None:
        """ACC-001 must have version_number 1 for the old row and 2 for the new row."""
        rows = self._mart_rows_for("ACC-001")
        assert len(rows) == 2
        assert rows[0]["version_number"] == 1
        assert rows[1]["version_number"] == 2

    def test_version_duration_days_current(self) -> None:
        """Current row version_duration_days must be >= 0 (days since valid_from)."""
        rows = self._mart_rows_for("ACC-001")
        current_row = next(r for r in rows if r["is_current"])
        assert current_row["version_duration_days"] >= 0

    def test_version_duration_days_expired(self) -> None:
        """Expired row version_duration_days must equal (valid_to::date - valid_from::date)."""
        rows = self._mart_rows_for("ACC-001")
        expired_row = next(r for r in rows if not r["is_current"])
        vf = expired_row["valid_from"]
        vt = expired_row["valid_to"]
        valid_from_date = vf.date() if hasattr(vf, "date") else vf
        valid_to_date = vt.date() if hasattr(vt, "date") else vt
        expected_duration = (valid_to_date - valid_from_date).days
        assert expired_row["version_duration_days"] == expected_duration

    def test_no_sentinel_in_duration(self) -> None:
        """No version_duration_days should exceed 100000 (would indicate a sentinel date bug)."""
        all_durations = self.conn.execute(
            "SELECT version_duration_days FROM mart__account_history"
        ).fetchall()
        assert all(row[0] <= 100000 for row in all_durations)


# ---------------------------------------------------------------------------
# T028 - TestQualityChecks
# ---------------------------------------------------------------------------


def _read_check_sql(filename: str) -> str:
    """Read a SQL check file from CHECK_DIR and return its content."""
    path = CHECK_DIR / filename
    return path.read_text()


def _run_check_sql(conn: duckdb.DuckDBPyConnection, filename: str) -> list:
    """Execute a check SQL file and return all result rows."""
    sql = _read_check_sql(filename)
    return conn.execute(sql).fetchall()


class TestQualityChecks:
    """T028: quality check SQL files for dim_accounts."""

    @pytest.fixture(autouse=True)
    def _setup(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Build a clean dim_accounts state for pass tests."""
        self.conn = conn

        insert_txn(conn, "t01", "ACC-001", "USD", "Food", run_id="run-001")
        insert_txn(conn, "t02", "ACC-001", "USD", "Food", run_id="run-001")
        insert_txn(conn, "t03", "ACC-002", "GBP", "Travel", run_id="run-001")

        build_dim_accounts(conn, run_id="run-001", run_date=date(2026, 2, 26))

    def test_single_current_pass(self) -> None:
        """Clean dim_accounts must have 0 violations for the single_current check."""
        rows = _run_check_sql(self.conn, "check__dim_accounts_single_current.sql")
        assert len(rows) == 0

    def test_single_current_fail(self) -> None:
        """Inserting two is_current=TRUE rows for same account triggers the check."""
        self.conn.execute(
            """INSERT INTO dim_accounts (account_id, primary_currency, primary_category,
                transaction_count, total_spend, first_seen, last_seen, row_hash,
                valid_from, valid_to, is_current, run_id)
               VALUES ('ACC-DUP','USD','Food',10,500.0,'2026-01-01','2026-01-31','fakehash',
                       '2026-01-01T00:00:00+00:00',NULL,TRUE,'run-bad')"""
        )
        self.conn.execute(
            """INSERT INTO dim_accounts (account_id, primary_currency, primary_category,
                transaction_count, total_spend, first_seen, last_seen, row_hash,
                valid_from, valid_to, is_current, run_id)
               VALUES ('ACC-DUP','EUR','Food',10,500.0,'2026-01-01','2026-01-31','fakehash2',
                       '2026-01-02T00:00:00+00:00',NULL,TRUE,'run-bad')"""
        )
        rows = _run_check_sql(self.conn, "check__dim_accounts_single_current.sql")
        assert len(rows) >= 1

    def test_no_overlapping_ranges_pass(self) -> None:
        """Clean dim_accounts must have 0 violations for the no_overlapping_ranges check."""
        rows = _run_check_sql(self.conn, "check__dim_accounts_no_overlapping_ranges.sql")
        assert len(rows) == 0

    def test_no_null_sk_pass(self) -> None:
        """Clean dim_accounts must have 0 violations for the no_null_sk check."""
        rows = _run_check_sql(self.conn, "check__dim_accounts_no_null_sk.sql")
        assert len(rows) == 0


# ---------------------------------------------------------------------------
# T034 - TestPIIClassification
# ---------------------------------------------------------------------------


class TestPIIClassification:
    """T034: dim_accounts must have exactly 13 columns and contain no PII column names."""

    def test_exactly_13_columns(self, conn: duckdb.DuckDBPyConnection) -> None:
        """dim_accounts must have exactly 13 columns."""
        count = conn.execute(
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_name = 'dim_accounts'"
        ).fetchone()[0]
        assert count == 13

    def test_no_pii_columns(self, conn: duckdb.DuckDBPyConnection) -> None:
        """No column in dim_accounts may contain PII-related name fragments."""
        pii_fragments = ["name", "email", "phone", "ssn", "dob", "birth", "address", "social"]
        column_names = [
            row[0].lower()
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'dim_accounts'"
            ).fetchall()
        ]
        for col in column_names:
            for fragment in pii_fragments:
                assert fragment not in col, (
                    f"Column '{col}' contains PII-related fragment '{fragment}'"
                )
