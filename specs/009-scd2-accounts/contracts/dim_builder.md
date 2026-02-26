# Module Contract: src/dimensions/dim_accounts.py

**Feature**: 009-scd2-accounts
**Date**: 2026-02-26

---

## Overview

`src/dimensions/dim_accounts.py` implements the SCD Type 2 dimension build logic for the `dim_accounts` table. It is a dedicated Python module — separate from the SQL transform runner — that handles change detection, row expiry, and version insertion using DuckDB operations directly inside a single atomic transaction.

This module is invoked by the pipeline orchestrator after SQL transforms and before data quality checks (FR-009).

---

## Public Interface

### `build_dim_accounts(conn, run_id, run_date) -> DimBuildResult`

Execute the full SCD Type 2 merge for the accounts dimension.

**Signature**

```python
def build_dim_accounts(
    conn: duckdb.DuckDBPyConnection,
    run_id: str,
    run_date: date,
) -> DimBuildResult:
```

**Parameters**

| Parameter | Type | Description |
|-----------|------|-------------|
| `conn` | `duckdb.DuckDBPyConnection` | An open DuckDB connection. Tables must already be created (via `create_dim_tables`). |
| `run_id` | `str` | Unique identifier for the current pipeline run. Recorded in metadata but does not affect SCD merge logic. |
| `run_date` | `date` | The effective date for this pipeline run. Used as `valid_from` on new version rows and as the expiry boundary (`valid_to = run_date - 1 day`) on closed rows. |

**Returns**

`DimBuildResult` — see dataclass definition below.

**Pre-conditions**

- `conn` is open and not inside an active transaction.
- The `transactions` table exists and is populated (Feature 002).
- The `stg_transactions` view exists (Feature 003).
- `create_dim_tables(conn)` has been called at least once on this connection/database — i.e., `dim_accounts` and `dim_accounts_sk_seq` exist.
- `run_date` is not None and is a valid `datetime.date`.

**Post-conditions (on success)**

- For every `account_id` present in `transactions`: exactly one row in `dim_accounts` has `is_current = TRUE` for that `account_id`.
- For `account_id` values whose tracked attributes changed since the last run: the previously current row has been closed (`is_current = FALSE`, `valid_to = run_date - 1 day`) and a new row has been inserted (`is_current = TRUE`, `valid_from = run_date`, `valid_to = 9999-12-31`).
- For `account_id` values whose tracked attributes are unchanged: no new row is inserted; the existing current row is untouched.
- For `account_id` values appearing for the first time: a new row is inserted with `valid_from = run_date`, `valid_to = 9999-12-31`, `is_current = TRUE`.
- All writes (row closures + insertions) are committed atomically in a single transaction — no partial state is possible.
- Historical rows (non-current) are never deleted or modified.

**Error contract**

- On any failure during the SCD merge, `conn.rollback()` is called before raising.
- `DimBuildError` is raised wrapping the original exception.
- After a `DimBuildError`, the `dim_accounts` table is guaranteed to be in its pre-call state — no rows are closed or inserted.
- The caller (pipeline orchestrator) is responsible for recording the failure in `ingestion_runs`. Manifest/run-tracking writes must occur outside this function.

**Example**

```python
from datetime import date
from src.dimensions.dim_accounts import build_dim_accounts, create_dim_tables

conn = duckdb.connect("data/warehouse/transactions.duckdb")
create_dim_tables(conn)

result = build_dim_accounts(conn, run_id="abc123", run_date=date(2026, 2, 26))
print(result.accounts_processed)   # e.g. 42
print(result.new_versions)         # e.g. 3
print(result.unchanged)            # e.g. 39
print(result.elapsed_seconds)      # e.g. 0.14
```

---

### `create_dim_tables(conn) -> None`

Create the `dim_accounts` table and `dim_accounts_sk_seq` sequence if they do not already exist.

**Signature**

```python
def create_dim_tables(conn: duckdb.DuckDBPyConnection) -> None:
```

**Parameters**

| Parameter | Type | Description |
|-----------|------|-------------|
| `conn` | `duckdb.DuckDBPyConnection` | An open DuckDB connection. |

**Returns**

`None`

**Behavior**

- Executes `CREATE SEQUENCE IF NOT EXISTS dim_accounts_sk_seq START 1`.
- Executes `CREATE TABLE IF NOT EXISTS dim_accounts (...)` with the full schema (see table definition below).
- Idempotent: safe to call multiple times on the same database. Repeated calls produce no side effects.
- Does not modify existing rows or reset the sequence.

**Pre-conditions**

- `conn` is open.

**Post-conditions**

- `dim_accounts` table exists with the correct schema.
- `dim_accounts_sk_seq` sequence exists.

---

## Data Classes

### `DimBuildResult`

Aggregate statistics for one `build_dim_accounts` invocation.

```python
@dataclass
class DimBuildResult:
    accounts_processed: int   # Total distinct account_ids evaluated
    new_versions: int         # Rows inserted (new accounts + changed accounts)
    unchanged: int            # Accounts with no attribute change (no row inserted)
    elapsed_seconds: float    # Wall-clock time for the entire SCD merge
```

| Field | Type | Description |
|-------|------|-------------|
| `accounts_processed` | `int` | Total distinct `account_id` values recomputed from the full transaction history. Equals `new_versions + unchanged`. |
| `new_versions` | `int` | Number of new `dim_accounts` rows inserted. Includes first-time accounts and changed accounts. |
| `unchanged` | `int` | Number of accounts where recomputed attributes matched the current row; no row was inserted. |
| `elapsed_seconds` | `float` | Wall-clock seconds from function entry to return (includes transaction overhead). |

### `DimBuildError`

Raised on any failure inside `build_dim_accounts`. Always indicates a full rollback has been performed.

```python
class DimBuildError(Exception):
    """Raised when the SCD2 dimension build fails.

    The underlying dim_accounts table is guaranteed to be unchanged
    (full rollback performed before this exception is raised).
    """
```

---

## Table Schema: `dim_accounts`

Managed by `create_dim_tables`. Reference only — do not CREATE outside this module.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `account_sk` | `BIGINT` | PRIMARY KEY, `nextval('dim_accounts_sk_seq')` | Surrogate key. Monotonically increasing. Assigned at INSERT. |
| `account_id` | `VARCHAR` | NOT NULL | Natural key. Multiple rows per account for historical versions. |
| `primary_currency` | `VARCHAR` | NOT NULL | Mode of `currency` across all transactions for this account. Tie-breaks alphabetically. |
| `primary_category` | `VARCHAR` | NOT NULL | Mode of `category` across all transactions for this account. Tie-breaks alphabetically. |
| `transaction_count` | `BIGINT` | NOT NULL | Total number of transactions for this account. |
| `total_spend` | `DOUBLE` | NOT NULL | Sum of `amount` across all transactions for this account. |
| `first_seen` | `TIMESTAMPTZ` | NOT NULL | Earliest `timestamp` in `transactions` for this account. |
| `last_seen` | `TIMESTAMPTZ` | NOT NULL | Latest `timestamp` in `transactions` for this account. |
| `valid_from` | `DATE` | NOT NULL | First date this version is effective. Set to `run_date` at INSERT. |
| `valid_to` | `DATE` | NOT NULL | Last date this version is effective. `9999-12-31` for current rows; `run_date - 1 day` when closed. |
| `is_current` | `BOOLEAN` | NOT NULL | `TRUE` for the active version. Exactly one `TRUE` per `account_id` at all times. |
| `row_hash` | `VARCHAR` | NOT NULL | MD5 fingerprint of tracked attribute columns. Used for change detection. Format: `md5(primary_currency \|\| '\|' \|\| primary_category \|\| '\|' \|\| transaction_count \|\| '\|' \|\| total_spend \|\| '\|' \|\| first_seen \|\| '\|' \|\| last_seen)` with NULLs coalesced to `''`. |
| `run_id` | `VARCHAR` | NOT NULL | Pipeline run ID that created this row. For lineage. |

---

## Internal SCD Merge Pattern

The merge executes two SQL statements inside a single transaction (per research decision R-002 and R-004):

```python
conn.begin()
try:
    conn.execute(EXPIRE_SQL, params)   # Statement A: close changed current rows
    conn.execute(INSERT_SQL, params)   # Statement B: insert new current rows
except Exception:
    conn.rollback()
    raise DimBuildError(...) from exc
else:
    conn.commit()
```

**Statement A (EXPIRE)** — closes rows where the recomputed `row_hash` differs from the stored `row_hash`:

```sql
UPDATE dim_accounts
SET is_current = FALSE,
    valid_to   = ?::DATE  -- run_date - 1 day
WHERE is_current = TRUE
  AND account_id IN (
      SELECT account_id FROM <computed_attrs>
      WHERE row_hash != dim_accounts.row_hash
  )
```

**Statement B (INSERT)** — inserts new current rows for new or changed accounts:

```sql
INSERT INTO dim_accounts (
    account_id, primary_currency, primary_category,
    transaction_count, total_spend, first_seen, last_seen,
    valid_from, valid_to, is_current, row_hash, run_id
)
SELECT
    account_id, primary_currency, primary_category,
    transaction_count, total_spend, first_seen, last_seen,
    ?::DATE,         -- valid_from = run_date
    '9999-12-31',    -- valid_to sentinel
    TRUE,
    row_hash,
    ?                -- run_id
FROM <computed_attrs> AS new
WHERE NOT EXISTS (
    SELECT 1 FROM dim_accounts d
    WHERE d.account_id = new.account_id
      AND d.is_current = TRUE
      AND d.row_hash   = new.row_hash
)
```

---

## Dependencies

| Dependency | Feature | Notes |
|------------|---------|-------|
| `transactions` table | Feature 002 | Must exist and be populated before calling `build_dim_accounts`. |
| `stg_transactions` view | Feature 003 | Used as the source for attribute recomputation. |
| `dim_accounts_sk_seq` | This module | Created by `create_dim_tables`. |
| `duckdb` | — | DuckDB Python client. |
