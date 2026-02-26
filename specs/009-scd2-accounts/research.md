# Research: SCD Type 2 — Accounts Dimension

**Feature**: 009-scd2-accounts
**Date**: 2026-02-26

---

## R-001: Surrogate Key Strategy

**Decision:** Use `DuckDB SEQUENCE` (`nextval('dim_accounts_sk_seq')`) as a `BIGINT` surrogate key, generated SQL-side at INSERT time.

**Rationale:** The SCD merge logic lives in Python with direct DuckDB SQL execution (per clarification FR-012). Generating the surrogate in SQL via `nextval()` inside the INSERT statement is the cleanest approach — no round-trip, no Python-side UUID generation needed, and the sequence is monotonically increasing which provides an implicit creation-order audit trail. The existing codebase already uses a `quarantine_id_seq` sequence (loader.py:35) with exactly this pattern.

**Alternatives considered:**
- UUID (Python-side `uuid.uuid4().hex`) — Viable and idempotent across full refreshes, but adds no benefit here since dim_accounts is an append-only table during normal operation and the dimension is not shared across distributed writers.
- Hash-based surrogate (`MD5` of natural_key + valid_from) — Deterministic, good for reproducibility, but requires casting and concatenation overhead; overkill for a single-node DuckDB pipeline.
- Python-side integer counter — Fragile; race-prone if ever parallelised.

---

## R-002: Change Detection Strategy

**Decision:** MD5 row-fingerprint hashing stored as a `row_hash VARCHAR` column in `dim_accounts`, with a two-statement DuckDB MERGE pattern (expire-then-insert) inside a single transaction.

**Rationale:** Comparing a single MD5 hash column is cleaner and more maintainable than six explicit column-by-column `<>` predicates. DuckDB's `md5()` function is native (no extension). The two-statement pattern is required because DuckDB MERGE does not support `WHEN MATCHED THEN INSERT` (a new version row for the same natural key).

**Pattern:**
1. Statement A — MERGE to expire changed current rows (`UPDATE SET is_current = FALSE, valid_to = run_date - 1 day`)
2. Statement B — INSERT new current rows where hash changed or account is new (`NOT EXISTS` guard prevents duplicate inserts for unchanged accounts)

**Hash construction:** `md5(primary_currency || '|' || primary_category || '|' || transaction_count || '|' || total_spend || '|' || first_seen || '|' || last_seen)` — always COALESCE nulls to `''` before concatenation; use `'|'` delimiter to prevent `'a'||'bc'` == `'ab'||'c'` collisions.

**Alternatives considered:**
- Column-by-column `<>` predicates — Functional but fragile; breaks every time an attribute is added.
- Python-side Polars diff (join + filter) — Two round-trips (read dim into memory, compute diff, write back); less efficient than pure SQL.
- CDC / log-based — Overkill; source is batch-ingested transactions, not a streaming WAL.

---

## R-003: Mode Computation with Deterministic Tie-Breaking

**Decision:** Use `ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY cnt DESC, value ASC) QUALIFY rn = 1` pattern for computing `primary_currency` and `primary_category`.

**Rationale:** DuckDB's built-in `mode()` aggregate is explicitly documented as producing indeterminate results on ties. The `ROW_NUMBER` + `QUALIFY` pattern is explicit, portable, and gives a deterministic tie-break: highest frequency wins; alphabetically first value wins on ties.

**Pattern:**
```sql
WITH currency_counts AS (
    SELECT account_id, currency, COUNT(*) AS cnt
    FROM transactions
    GROUP BY account_id, currency
),
ranked AS (
    SELECT account_id, currency,
           ROW_NUMBER() OVER (
               PARTITION BY account_id
               ORDER BY cnt DESC, currency ASC
           ) AS rn
    FROM currency_counts
    QUALIFY rn = 1
)
SELECT account_id, currency AS primary_currency FROM ranked
```

**Alternatives considered:**
- `mode(currency)` bare — Non-deterministic on ties; rejected.
- `mode(currency ORDER BY currency ASC)` — Tie-breaking via ORDER BY inside mode() is underdocumented and not a reliable specification.
- `MIN_BY(currency, (-freq, currency))` — Tuple sort key in MIN_BY may not be stable across DuckDB versions.

---

## R-004: Atomic Transaction Pattern

**Decision:** Use `conn.begin()` / `conn.commit()` / `conn.rollback()` Python method calls wrapping the two-statement SCD merge.

**Rationale:** DuckDB provides full ACID atomicity. The Python method form is idiomatic and avoids string-based SQL control flow. The `else` clause on the `try` block ensures `commit()` only runs when no exception was raised — prevents the anti-pattern of committing after a rollback.

**Pattern:**
```python
conn.begin()
try:
    conn.execute(EXPIRE_SQL, params)   # Statement A: close old rows
    conn.execute(INSERT_SQL, params)   # Statement B: insert new rows
except Exception:
    conn.rollback()
    raise
else:
    conn.commit()
```

**Key constraints:**
- `conn.begin()` must not be called if a transaction is already open on the connection.
- Manifest writes (`mark_success`, `mark_failed`) must remain **outside** this transaction — they track bookkeeping state that should persist even if the SCD merge fails.
- DuckDB does not support `with conn.transaction():` context manager (unimplemented as of 2026).

**Alternatives considered:**
- `conn.execute("BEGIN TRANSACTION")` SQL strings — Functionally equivalent but string-based; less idiomatic.
- Savepoints — Unnecessary complexity for a two-statement operation.
- Auto-commit (no explicit transaction) — Leaves a split-brain window where expire commits but insert fails. Rejected.

---

## Summary of Key Design Decisions

| # | Decision | Choice |
|---|----------|--------|
| R-001 | Surrogate key | DuckDB SEQUENCE (`BIGINT`) |
| R-002 | Change detection | MD5 row hash + two-statement MERGE |
| R-003 | Mode tie-breaking | ROW_NUMBER + QUALIFY, ORDER BY cnt DESC, value ASC |
| R-004 | Transaction atomicity | `conn.begin()` / `conn.commit()` / `conn.rollback()` |
