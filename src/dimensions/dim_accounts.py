"""SCD Type 2 dimension builder for dim_accounts.

Derives behavioral account profiles from the full transaction history and
maintains a slowly-changing dimension table with valid_from/valid_to/is_current
versioning. All SCD merge operations execute inside a single atomic transaction.

Public API:
    create_dim_tables(conn)               -- idempotent DDL
    build_dim_accounts(conn, run_id, run_date) -> DimBuildResult
    DimBuildResult                        -- dataclass returned by build_dim_accounts
    DimBuildError                         -- raised on any failure; rollback guaranteed
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from time import monotonic

import duckdb

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

_DIM_ACCOUNTS_SK_SEQ_DDL: str = """
CREATE SEQUENCE IF NOT EXISTS dim_accounts_sk_seq START 1;
"""

_DIM_ACCOUNTS_DDL: str = """
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
);
"""

_DIM_BUILD_RUNS_DDL: str = """
CREATE TABLE IF NOT EXISTS dim_build_runs (
    run_id              VARCHAR     NOT NULL,
    accounts_processed  INTEGER     NOT NULL,
    new_versions        INTEGER     NOT NULL,
    unchanged           INTEGER     NOT NULL,
    elapsed_seconds     DOUBLE      NOT NULL,
    completed_at        TIMESTAMPTZ NOT NULL
);
"""

# ---------------------------------------------------------------------------
# Attribute derivation SQL
# ---------------------------------------------------------------------------

_COMPUTE_PROFILES_SQL: str = """
WITH currency_counts AS (
    SELECT account_id, currency, COUNT(*) AS cnt
    FROM   stg_transactions
    GROUP  BY account_id, currency
),
primary_currency AS (
    SELECT account_id, currency AS primary_currency
    FROM   currency_counts
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY account_id
        ORDER BY cnt DESC, currency ASC
    ) = 1
),
category_counts AS (
    SELECT account_id, category, COUNT(*) AS cnt
    FROM   stg_transactions
    GROUP  BY account_id, category
),
primary_category AS (
    SELECT account_id, category AS primary_category
    FROM   category_counts
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY account_id
        ORDER BY cnt DESC, category ASC
    ) = 1
),
account_stats AS (
    SELECT
        account_id,
        COUNT(*)              AS transaction_count,
        SUM(amount)           AS total_spend,
        MIN(transaction_date) AS first_seen,
        MAX(transaction_date) AS last_seen
    FROM stg_transactions
    GROUP BY account_id
)
SELECT
    s.account_id,
    c.primary_currency,
    g.primary_category,
    s.transaction_count,
    s.total_spend,
    s.first_seen,
    s.last_seen,
    md5(
        COALESCE(c.primary_currency, '')             || '|' ||
        COALESCE(g.primary_category, '')             || '|' ||
        COALESCE(CAST(s.transaction_count AS VARCHAR), '') || '|' ||
        COALESCE(CAST(s.total_spend      AS VARCHAR), '') || '|' ||
        COALESCE(CAST(s.first_seen       AS VARCHAR), '') || '|' ||
        COALESCE(CAST(s.last_seen        AS VARCHAR), '')
    ) AS row_hash
FROM   account_stats  s
JOIN   primary_currency c USING (account_id)
JOIN   primary_category g USING (account_id)
"""

# ---------------------------------------------------------------------------
# SCD merge SQL
# ---------------------------------------------------------------------------

# Statement A: expire changed current rows
_EXPIRE_SQL: str = """
UPDATE dim_accounts
SET
    is_current = FALSE,
    valid_to   = ?::TIMESTAMPTZ
WHERE is_current = TRUE
  AND account_id IN (
      SELECT p.account_id
      FROM   _incoming_profiles p
      JOIN   dim_accounts d
        ON   d.account_id = p.account_id
       AND   d.is_current = TRUE
      WHERE  p.row_hash <> d.row_hash
  )
"""

# Statement B: insert new current rows (new accounts + changed accounts)
_INSERT_SQL: str = """
INSERT INTO dim_accounts (
    account_id, primary_currency, primary_category,
    transaction_count, total_spend, first_seen, last_seen,
    row_hash, valid_from, valid_to, is_current, run_id
)
SELECT
    p.account_id,
    p.primary_currency,
    p.primary_category,
    p.transaction_count,
    p.total_spend,
    p.first_seen,
    p.last_seen,
    p.row_hash,
    ?::TIMESTAMPTZ AS valid_from,
    NULL           AS valid_to,
    TRUE           AS is_current,
    ?              AS run_id
FROM _incoming_profiles p
WHERE NOT EXISTS (
    SELECT 1
    FROM   dim_accounts d
    WHERE  d.account_id = p.account_id
      AND  d.is_current = TRUE
      AND  d.row_hash   = p.row_hash
)
"""


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass
class DimBuildResult:
    """Metadata returned by a successful dim_accounts build run."""

    accounts_processed: int
    new_versions: int
    unchanged: int
    elapsed_seconds: float


class DimBuildError(Exception):
    """Raised when the dim_accounts build fails.

    The SCD merge transaction is always rolled back before this is raised,
    guaranteeing no partial writes to dim_accounts.
    """


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------


def create_dim_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create dim_accounts sequence, table, and dim_build_runs table if not exist.

    Idempotent — safe to call on every pipeline startup against both fresh
    and existing databases.

    Args:
        conn: An open DuckDB connection.
    """
    conn.execute(_DIM_ACCOUNTS_SK_SEQ_DDL)
    conn.execute(_DIM_ACCOUNTS_DDL)
    conn.execute(_DIM_BUILD_RUNS_DDL)
    logger.info("Dimension tables ready")


def build_dim_accounts(
    conn: duckdb.DuckDBPyConnection,
    run_id: str,
    run_date: date,
) -> DimBuildResult:
    """Build or update the dim_accounts SCD Type 2 dimension.

    Derives account behavioral profiles from the full stg_transactions history,
    compares them against current dimension rows via MD5 row-hash, and applies
    a two-statement atomic SCD merge:
      - Statement A: expire changed current rows (set valid_to, is_current=FALSE)
      - Statement B: insert new current rows for new and changed accounts

    The entire merge executes inside a single transaction. On any failure the
    transaction is rolled back and DimBuildError is raised — no partial writes
    occur.

    Args:
        conn: An open DuckDB connection with dim_accounts created.
        run_id: Pipeline run identifier written to each new dim row for lineage.
        run_date: The calendar date of this pipeline run. Used as valid_from
            for new rows and as valid_to for expired rows (run_date - 1 day).

    Returns:
        DimBuildResult with counts and elapsed time.

    Raises:
        DimBuildError: On any failure after guaranteed rollback.
    """
    start = monotonic()
    # valid_from for new rows = midnight UTC on run_date
    run_ts = datetime(run_date.year, run_date.month, run_date.day, tzinfo=timezone.utc).isoformat()
    # valid_to for expired rows = midnight UTC on run_date - 1 day (non-overlapping)
    expire_ts = (
        datetime(run_date.year, run_date.month, run_date.day, tzinfo=timezone.utc)
        - timedelta(days=1)
    ).isoformat()

    try:
        # Materialise computed profiles into a temp view so both SQL statements
        # share the same computation without re-scanning stg_transactions.
        conn.execute(f"CREATE OR REPLACE TEMP VIEW _incoming_profiles AS {_COMPUTE_PROFILES_SQL}")

        accounts_processed: int = conn.execute(
            "SELECT COUNT(*) FROM _incoming_profiles"
        ).fetchone()[0]

        conn.begin()
        try:
            conn.execute(_EXPIRE_SQL, [expire_ts])
            conn.execute(_INSERT_SQL, [run_ts, run_id])
        except Exception:
            conn.rollback()
            raise
        else:
            conn.commit()

        new_versions: int = conn.execute(
            "SELECT COUNT(*) FROM dim_accounts WHERE run_id = ? AND is_current = TRUE",
            [run_id],
        ).fetchone()[0]
        unchanged = accounts_processed - new_versions

    except DimBuildError:
        raise
    except Exception as exc:
        elapsed = monotonic() - start
        logger.error("dim_accounts build failed after %.3fs: %s", elapsed, exc)
        raise DimBuildError(str(exc)) from exc

    elapsed = monotonic() - start
    logger.info(
        "dim_accounts build complete: processed=%d new=%d unchanged=%d elapsed=%.3fs",
        accounts_processed,
        new_versions,
        unchanged,
        elapsed,
    )

    result = DimBuildResult(
        accounts_processed=accounts_processed,
        new_versions=new_versions,
        unchanged=unchanged,
        elapsed_seconds=elapsed,
    )

    # T032: persist dim build metadata
    completed_at = datetime.now(tz=timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO dim_build_runs (run_id, accounts_processed, new_versions, unchanged,
                                    elapsed_seconds, completed_at)
        VALUES (?, ?, ?, ?, ?, ?::TIMESTAMPTZ)
        """,
        [run_id, accounts_processed, new_versions, unchanged, elapsed, completed_at],
    )

    return result
