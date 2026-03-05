# Data Engineering Learning Guide: `learn-de-opencode`

A complete, end-to-end walkthrough of this project — written for data engineers who want to understand every moving part, from raw data generation to interactive dashboards.

---

## Table of Contents

1. [What Is This Project?](#1-what-is-this-project)
2. [Tech Stack at a Glance](#2-tech-stack-at-a-glance)
3. [Repository Layout](#3-repository-layout)
4. [The Big Picture: End-to-End Data Flow](#4-the-big-picture-end-to-end-data-flow)
5. [Step 1 — Generate Synthetic Data](#5-step-1--generate-synthetic-data)
6. [Step 2 — Ingest into DuckDB](#6-step-2--ingest-into-duckdb)
7. [Step 3 — SQL Transforms (Staging & Marts)](#7-step-3--sql-transforms-staging--marts)
8. [Step 4 — Build SCD Type 2 Dimensions](#8-step-4--build-scd-type-2-dimensions)
9. [Step 5 — Build the Fact Table](#9-step-5--build-the-fact-table)
10. [Step 6 — Data Quality Checks](#10-step-6--data-quality-checks)
11. [Step 7 — Data Catalog](#11-step-7--data-catalog)
12. [Step 8 — Dashboard](#12-step-8--dashboard)
13. [Data Model Reference](#13-data-model-reference)
14. [The Orchestrator: Tying It All Together](#14-the-orchestrator-tying-it-all-together)
15. [Testing Strategy](#15-testing-strategy)
16. [CI/CD Pipeline](#16-cicd-pipeline)
17. [Key Design Patterns Explained](#17-key-design-patterns-explained)
18. [Running It Yourself](#18-running-it-yourself)
19. [Concept Glossary](#19-concept-glossary)

---

## 1. What Is This Project?

`learn-de-opencode` is a **production-style data engineering tutorial project**. It simulates the kind of pipeline you'd build at a real company — generating financial transaction data, loading it into a warehouse, transforming it into analytics-ready tables, enforcing data quality, and serving it through an interactive dashboard.

**Learning goals:**
- Understand how raw data becomes a queryable warehouse
- See dimensional modeling (star schema, SCD Type 2) in practice
- Learn idempotent, lineage-tracked pipeline design
- Understand data quality as code (SQL checks)
- See a metadata catalog built automatically from introspection

**What it is NOT:**
- It does not use a cloud warehouse (uses DuckDB, an embedded in-process database)
- It does not use Airflow or a managed scheduler (uses a simple Python orchestrator)
- It does not use dbt (SQL transforms are custom Python runners)

This makes it ideal for learning fundamentals without cloud costs or tooling complexity.

---

## 2. Tech Stack at a Glance

| Layer | Tool | Why |
|---|---|---|
| Data generation | Python + Faker + NumPy | Realistic synthetic financial data |
| DataFrame processing | Polars | Fast vectorized operations, native Parquet I/O |
| Data format (files) | Parquet | Columnar, compressed, schema-preserving |
| Warehouse | DuckDB | Embedded SQL analytical engine, serverless |
| Arrow interchange | PyArrow | Zero-copy data transfer between Polars and DuckDB |
| Business metadata | YAML | Human-readable catalog overlay |
| Dashboard | Evidence.dev | SQL-driven, Git-friendly dashboards |
| Package manager | uv | Ultra-fast Python package manager |
| Linting | Ruff | Fast Python linter and formatter |
| Testing | pytest | Unit and integration tests |
| CI/CD | GitHub Actions | Lint → Test → E2E pipeline |

---

## 3. Repository Layout

```
learn-de-opencode/
│
├── src/                          # All pipeline source code
│   ├── generate_transactions.py  # CLI: Step 1 — generate raw data
│   ├── ingest_transactions.py    # CLI: Step 2 — ingest into DuckDB
│   ├── run_transforms.py         # CLI: Step 3 & 5 — run SQL transforms
│   ├── run_dim_build.py          # CLI: Step 4 — build SCD2 dimensions
│   ├── run_checks.py             # CLI: Step 6 — run data quality checks
│   ├── run_catalog.py            # CLI: Step 7 — build the data catalog
│   ├── run_pipeline.py           # CLI: Orchestrator — runs all steps
│   │
│   ├── ingestion/                # Ingestion module
│   │   ├── pipeline.py           # Main ingestion orchestration logic
│   │   ├── validator.py          # Schema & record validation rules
│   │   └── manifest.py           # Tracks which files were already ingested
│   │
│   ├── transformer/              # Transform module
│   │   ├── runner.py             # Discovers SQL files, executes in DAG order
│   │   ├── graph.py              # Topological sort (Kahn's algorithm)
│   │   └── parser.py             # Extracts model name/depends_on from SQL headers
│   │
│   ├── dimensions/               # Dimension builder module
│   │   └── dim_accounts.py       # SCD Type 2 logic for dim_accounts
│   │
│   ├── checker/                  # Data quality module
│   │   └── runner.py             # Discovers SQL checks, runs them, captures violations
│   │
│   ├── catalog/                  # Catalog module
│   │   └── runner.py             # Introspects warehouse + merges YAML metadata
│   │
│   ├── orchestrator/             # Pipeline orchestrator module
│   │   └── runner.py             # Step sequencing, filtering, subprocess execution
│   │
│   ├── lib/                      # Shared utilities
│   │   └── db.py                 # DuckDB connection helpers
│   │
│   ├── models/                   # Python data models (dataclasses/TypedDicts)
│   │
│   ├── transforms/               # SQL files for staging and mart transforms
│   │   ├── staging__stg_transactions.sql
│   │   ├── mart__daily_spend_by_category.sql
│   │   └── mart__monthly_account_summary.sql
│   │
│   ├── fact_transforms/          # SQL files for fact table builds
│   │   └── fact__fct_transactions.sql
│   │
│   ├── checks/                   # SQL data quality check files
│   │   ├── check__row_count_staging.sql
│   │   ├── check__freshness.sql
│   │   ├── check__fct_row_count_matches_staging.sql
│   │   └── ... (13 total checks)
│   │
│   └── metadata/
│       └── catalog.yaml          # Business metadata overlay for the catalog
│
├── tests/                        # Test suite
│   ├── unit/                     # Unit tests per module
│   ├── integration/              # End-to-end pipeline tests
│   └── quality/                  # Data quality validation (6Cs framework)
│
├── dashboard/                    # Evidence.dev dashboard
│   ├── pages/                    # Dashboard pages (markdown + SQL)
│   └── sources/warehouse/        # DuckDB data source config
│
├── specs/                        # Feature specifications (9 features)
├── .github/workflows/ci.yml      # GitHub Actions CI/CD
├── pyproject.toml                # Python project config + dependencies
├── CLAUDE.md                     # AI assistant instructions
├── AGENTS.md                     # Development guidelines
└── README.md                     # Quick start documentation
```

**Key insight:** Every pipeline step has a corresponding CLI script (`src/run_*.py`) that calls a module in `src/<module>/`. This separation keeps CLI concerns (argument parsing, exit codes) separate from business logic.

---

## 4. The Big Picture: End-to-End Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│  STEP 1: Generate                                               │
│  Python + Faker + NumPy → Parquet files → data/raw/            │
└───────────────────────────────┬─────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────┐
│  STEP 2: Ingest                                                 │
│  Read Parquet → Validate → Quarantine bad rows →                │
│  Deduplicate → Enrich with lineage → Load into DuckDB           │
│  Tables: transactions, quarantine, ingestion_runs               │
└───────────────────────────────┬─────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────┐
│  STEP 3: Transforms                                             │
│  Run SQL in DAG order (Kahn's topological sort)                 │
│  Tables: stg_transactions (VIEW), daily_spend_by_category,      │
│          monthly_account_summary                                │
└───────────────────────────────┬─────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────┐
│  STEP 4: Dim Build                                              │
│  Derive account attributes from transaction history →           │
│  SCD Type 2 merge (expire old rows, insert new current row)     │
│  Table: dim_accounts                                            │
└───────────────────────────────┬─────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────┐
│  STEP 5: Fact Build                                             │
│  LEFT JOIN stg_transactions to dim_accounts (point-in-time)     │
│  Table: fct_transactions                                        │
└───────────────────────────────┬─────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────┐
│  STEP 6: Checks                                                 │
│  Run SQL checks (violations = failures)                         │
│  Tables: check_runs, check_results                              │
└───────────────────────────────┬─────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────┐
│  STEP 7: Catalog                                                │
│  Introspect warehouse + merge YAML metadata                     │
│  Tables: catalog_tables, catalog_columns                        │
└───────────────────────────────┬─────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────┐
│  STEP 8: Dashboard                                              │
│  Evidence.dev reads DuckDB → Interactive web dashboards         │
│  URL: http://localhost:3000                                     │
└─────────────────────────────────────────────────────────────────┘
```

**Zone concept:** The warehouse is logically divided into zones:
- **Raw** — data as-loaded, minimal transformation
- **Staging** — cleaned, standardized views of raw data
- **Dimension** — slowly changing dimensions (SCD2)
- **Fact** — grain-level measurable events joined to dimensions
- **Mart** — pre-aggregated summaries for consumption
- **Metadata** — pipeline run logs, quality results, catalog

---

## 5. Step 1 — Generate Synthetic Data

**Files:** `src/generate_transactions.py`

**What it does:** Creates realistic-looking financial transaction records and saves them as Parquet files.

### How synthetic data is generated

```python
# Simplified view of what the generator does:

# 1. Create realistic account IDs
account_ids = [f"ACC{i:05d}" for i in range(num_accounts)]  # e.g. ACC00001

# 2. For each transaction:
#   - Pick a random account
#   - Pick transaction type: debit (70%) or credit (30%)  ← skewed distribution
#   - Generate amount using log-normal distribution (realistic spend curves)
#   - Pick currency (USD 80%, EUR 10%, GBP 5%, CAD 5%)
#   - Pick merchant using Faker
#   - Pick category (Groceries, Gas, etc.)
#   - Pick status: completed (85%), pending (10%), failed (5%)
#   - Generate timestamp within the date range

# 3. Save as Parquet partitioned by date:
#    data/raw/transactions_20240101_XXXXXX.parquet
```

### Key CLI options

```bash
# Generate 10,000 transactions (default)
uv run src/generate_transactions.py

# Generate 1 million transactions with a fixed seed (reproducible)
uv run src/generate_transactions.py --count 1000000 --seed 42

# Specify date range
uv run src/generate_transactions.py --start-date 2024-01-01 --end-date 2024-06-30

# Generate multiple account populations
uv run src/generate_transactions.py --accounts 500
```

### Why Parquet?

Parquet is a columnar format — it stores data column-by-column rather than row-by-row. This is ideal for analytics because:
- Reading a single column (e.g., `amount`) doesn't read unneeded columns
- It compresses well (similar values grouped together)
- It preserves data types (no type inference issues like CSV)
- Polars and DuckDB both have native Parquet support

### Large dataset handling

For datasets > 1M records, the generator uses **chunked generation** to avoid memory exhaustion — it generates and writes batches of rows rather than building one giant DataFrame.

---

## 6. Step 2 — Ingest into DuckDB

**Files:** `src/ingest_transactions.py`, `src/ingestion/pipeline.py`, `src/ingestion/validator.py`, `src/ingestion/manifest.py`

This is the most complex step. It takes raw Parquet files and loads them into DuckDB with validation, deduplication, and lineage tracking.

### Ingestion pipeline phases (per file)

```
┌─────────────────┐
│  1. Discover    │  Find all *.parquet files in data/raw/
│     Files       │  Check manifest — skip files already ingested
└────────┬────────┘
         │
┌────────▼────────┐
│  2. Read        │  Polars reads Parquet → DataFrame
│     Parquet     │
└────────┬────────┘
         │
┌────────▼────────────────────────────────────────┐
│  3. File-Level  │  Check: correct columns?       │
│     Validation  │  Check: correct data types?    │
│                 │  → Reject entire file if fails  │
└────────┬────────┘
         │
┌────────▼────────────────────────────────────────┐
│  4. Record-Level│  For each row check:           │
│     Validation  │  - No null transaction_id      │
│                 │  - amount > 0                  │
│                 │  - currency in {USD,EUR,GBP,CAD}│
│                 │  - status in {completed,pending,failed}│
│                 │  → Valid rows: continue         │
│                 │  → Invalid rows: quarantine     │
└────────┬────────┘
         │
┌────────▼────────┐
│  5. Quarantine  │  Write rejected rows to `quarantine` table
│     Bad Rows    │  with rejection_reason column
└────────┬────────┘
         │
┌────────▼────────────────────────────────────────┐
│  6. Deduplicate │  Within-file: remove duplicate  │
│                 │  transaction_ids in this batch  │
│                 │  Cross-file: check against      │
│                 │  existing warehouse records     │
└────────┬────────┘
         │
┌────────▼────────────────────────────────────────┐
│  7. Enrich      │  Add metadata columns:         │
│     Lineage     │  - source_file (which Parquet) │
│                 │  - ingested_at (UTC timestamp)  │
│                 │  - run_id (UUID for this run)   │
│                 │  - transaction_date (derived)   │
└────────┬────────┘
         │
┌────────▼────────┐
│  8. Load into   │  INSERT INTO transactions
│     DuckDB      │  using PyArrow as bridge
└────────┬────────┘
         │
┌────────▼────────┐
│  9. Update      │  Mark file as processed in
│     Manifest    │  manifest (for incremental runs)
└─────────────────┘
```

### Manifest-based incremental ingestion

The manifest is a simple tracking table that records which files have been successfully ingested. On subsequent runs:

```python
# Pseudocode of manifest logic
all_files = discover_parquet_files("data/raw/")
already_processed = manifest.get_processed_files()
files_to_process = all_files - already_processed  # Set difference

for file in files_to_process:
    ingest(file)
    manifest.mark_as_processed(file)  # Only if successful
```

This makes ingestion **idempotent** — re-running it won't create duplicates.

### Validation rules (`src/ingestion/validator.py`)

```python
EXPECTED_COLUMNS = {
    "transaction_id": pl.Utf8,
    "timestamp": pl.Datetime,
    "amount": pl.Float64,
    "currency": pl.Utf8,
    "merchant_name": pl.Utf8,
    "category": pl.Utf8,
    "account_id": pl.Utf8,
    "transaction_type": pl.Utf8,
    "status": pl.Utf8,
}

VALID_CURRENCIES = {"USD", "EUR", "GBP", "CAD"}
VALID_TRANSACTION_TYPES = {"debit", "credit"}
VALID_STATUSES = {"completed", "pending", "failed"}
```

### Quarantine table

Rejected records are not silently dropped — they land in the `quarantine` table with a `rejection_reason` so engineers can investigate:

```sql
SELECT rejection_reason, COUNT(*) as count
FROM quarantine
GROUP BY rejection_reason;
-- Example output:
-- "amount must be positive" → 12 rows
-- "invalid currency: XYZ" → 3 rows
```

### Why Polars → PyArrow → DuckDB?

Rather than using SQL INSERT statements row-by-row, this pipeline uses a zero-copy path:

```
Polars DataFrame → .to_arrow() → PyArrow Table → DuckDB.register() → INSERT SELECT
```

This is dramatically faster because:
1. No serialization/deserialization (Arrow is a shared memory format)
2. DuckDB can operate directly on Arrow memory
3. Bulk inserts instead of row-by-row

---

## 7. Step 3 — SQL Transforms (Staging & Marts)

**Files:** `src/run_transforms.py`, `src/transformer/runner.py`, `src/transformer/graph.py`, `src/transforms/*.sql`

### How SQL models work

Each SQL file is a "model" with a header declaring its name and dependencies:

```sql
-- model: daily_spend_by_category
-- depends_on: stg_transactions

CREATE OR REPLACE TABLE daily_spend_by_category AS
SELECT
    transaction_date,
    category,
    currency,
    SUM(amount)        AS total_amount,
    COUNT(*)           AS transaction_count,
    SUM(amount) / COUNT(*) AS avg_amount
FROM stg_transactions
WHERE transaction_type = 'debit'
  AND status = 'completed'
GROUP BY transaction_date, category, currency
ORDER BY transaction_date, category, currency;
```

The Python runner:
1. Discovers all `.sql` files in `src/transforms/`
2. Parses the `-- model:` and `-- depends_on:` headers
3. Builds a dependency graph
4. Runs topological sort (Kahn's algorithm) to determine execution order
5. Executes each SQL file via `CREATE OR REPLACE` (idempotent)

### DAG resolution with Kahn's algorithm

Consider three models with these dependencies:

```
transactions → stg_transactions → daily_spend_by_category
                               ↘ monthly_account_summary
```

Kahn's algorithm:
1. Start with nodes that have no dependencies (in-degree = 0): `transactions`
2. "Remove" `transactions` from the graph, decrement in-degrees of dependents
3. `stg_transactions` now has in-degree = 0 → add to queue
4. Continue until all nodes processed

Result: a valid execution order that respects all dependencies.

**Cycle detection:** If a cycle exists (A depends on B, B depends on A), Kahn's algorithm detects it because some nodes never reach in-degree = 0.

### The transforms created

**`stg_transactions` (VIEW)** — staging layer:
```sql
CREATE OR REPLACE VIEW stg_transactions AS
SELECT
    transaction_id,
    "timestamp" AS transaction_timestamp,  -- renamed (timestamp is reserved)
    transaction_date,
    amount, currency, merchant_name, category,
    account_id, transaction_type, status,
    source_file, ingested_at, run_id
FROM transactions;
```

Why a VIEW? A view doesn't copy data — it's just a saved query. This means staging "transformation" is zero-cost and always reflects the latest raw data.

**`daily_spend_by_category` (TABLE)** — mart layer:
Pre-aggregated daily spend summaries. Useful for time-series charts. Grain: one row per (date, category, currency).

**`monthly_account_summary` (TABLE)** — mart layer:
Monthly debit/credit/net flow per account and currency. Grain: one row per (month, account_id, currency).

---

## 8. Step 4 — Build SCD Type 2 Dimensions

**Files:** `src/run_dim_build.py`, `src/dimensions/dim_accounts.py`

### What is a Slowly Changing Dimension (SCD Type 2)?

A dimension is a table of attributes about an entity — in this case, accounts. Those attributes can change over time (e.g., an account's primary category shifts from "Groceries" to "Gas"). SCD Type 2 tracks the **full history** of changes rather than overwriting old values.

Each version of an account gets its own row with:
- `valid_from` — when this version became active
- `valid_to` — when this version was superseded (NULL = currently active)
- `is_current` — boolean flag for the latest version
- `account_sk` — surrogate key (auto-incrementing integer, unique per row)
- `account_id` — natural key (the actual account ID, can have multiple rows)

### How SCD2 is implemented here

```
STEP 1: Compute current profiles
        From transaction history, derive each account's attributes:
        - primary_currency (mode of currencies used)
        - primary_category (mode of categories used)
        - transaction_count, total_spend, first_seen, last_seen

STEP 2: Compare to existing dim_accounts
        Compute row_hash = MD5(primary_currency || primary_category || ...)
        If row_hash changed → this account's attributes changed

STEP 3: Expire old rows (atomic transaction)
        UPDATE dim_accounts
        SET is_current = FALSE,
            valid_to = NOW()
        WHERE account_id IN (changed_accounts)
          AND is_current = TRUE

STEP 4: Insert new current rows (same transaction)
        INSERT INTO dim_accounts (account_id, primary_currency, ...,
                                  valid_from, valid_to, is_current)
        VALUES (new profile data, NOW(), NULL, TRUE)
```

Both STEP 3 and STEP 4 happen inside a single SQL transaction, ensuring atomicity — you never see a state where old rows are expired but new rows haven't been inserted.

### Example SCD2 scenario

**Initial load (Jan 1):**
| account_sk | account_id | primary_category | valid_from | valid_to | is_current |
|---|---|---|---|---|---|
| 1 | ACC00001 | Groceries | 2024-01-01 | NULL | TRUE |

**After profile change (Feb 1, account shifted to Gas spending):**
| account_sk | account_id | primary_category | valid_from | valid_to | is_current |
|---|---|---|---|---|---|
| 1 | ACC00001 | Groceries | 2024-01-01 | 2024-02-01 | FALSE |
| 2 | ACC00001 | Gas | 2024-02-01 | NULL | TRUE |

This allows you to ask: "What category was this account in on January 15?" — just filter `WHERE valid_from <= '2024-01-15' AND valid_to > '2024-01-15'`.

---

## 9. Step 5 — Build the Fact Table

**Files:** `src/fact_transforms/fact__fct_transactions.sql` (run via `run_transforms.py --transforms-dir src/fact_transforms`)

### What is a fact table?

A fact table sits at the center of a star schema. It records discrete business events (transactions) at the grain level (one row per transaction). It joins to dimension tables via foreign keys.

### Point-in-time join

The critical challenge with a SCD2 dimension is joining correctly across time. A naive join on `account_id` would match ALL rows for that account (past and present). Instead, we use a **point-in-time join**:

```sql
CREATE OR REPLACE TABLE fct_transactions AS
SELECT
    stg.transaction_id,
    stg.transaction_timestamp,
    stg.transaction_date,
    COALESCE(d.account_sk, -1) AS account_sk,  -- -1 = "unknown" member
    stg.account_id,
    stg.amount,
    stg.currency,
    stg.merchant_name,
    stg.category,
    stg.transaction_type,
    stg.status,
    stg.source_file,
    stg.ingested_at,
    stg.run_id
FROM stg_transactions stg
LEFT JOIN dim_accounts d
    ON  stg.account_id = d.account_id
    AND stg.transaction_timestamp >= d.valid_from
    AND stg.transaction_timestamp <  COALESCE(d.valid_to, '9999-12-31 23:59:59+00'::TIMESTAMPTZ);
```

**Key details:**
- `LEFT JOIN` — keeps all transactions even if no dimension row matches
- `COALESCE(d.account_sk, -1)` — when no dimension match, use the "unknown member" surrogate key -1 (standard warehouse pattern)
- `COALESCE(d.valid_to, '9999-12-31...')` — the current (active) row has `valid_to = NULL`, so we substitute a far-future date

---

## 10. Step 6 — Data Quality Checks

**Files:** `src/run_checks.py`, `src/checker/runner.py`, `src/checks/*.sql`

### The violation-based convention

Every check SQL file follows one rule: **if the query returns rows, there are violations**. This means checks are naturally expressive:

```sql
-- check: unique_transaction_ids
-- severity: critical
-- description: transaction_id must be unique in transactions table

SELECT transaction_id, COUNT(*) as occurrences
FROM transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;
-- Returns rows only if duplicates exist
```

If the query returns 0 rows → check passes. If it returns rows → check fails, and the returned rows become sample violations.

### Check metadata header format

```sql
-- check: row_count_staging
-- severity: critical
-- description: Staging view row count must match source transactions table
```

Three severity levels:
- **`critical`** — pipeline fails (exit code 1) if this check has violations
- **`warn`** — violations are reported but pipeline continues (exit code 0)

### Checks included

| Check | Severity | What it validates |
|---|---|---|
| `row_count_staging` | critical | stg_transactions count = transactions count |
| `freshness` | warn | Most recent record ingested within 48 hours |
| `fct_row_count_matches_staging` | critical | fct_transactions count = stg_transactions count |
| `unique_daily_spend_grain` | critical | No duplicate (date, category, currency) in daily mart |
| `no_null_transaction_ids` | critical | No NULL transaction_id values |
| `accepted_currencies` | warn | All currency values in allowed set |
| `accepted_statuses` | warn | All status values in allowed set |
| `positive_amounts` | critical | All amounts > 0 |
| ...and more | | |

### Result storage

After checks run, results persist in the warehouse:

```sql
-- check_runs: one row per run
SELECT run_id, total_checks, checks_passed, checks_failed, status
FROM check_runs
ORDER BY started_at DESC LIMIT 5;

-- check_results: one row per check per run
SELECT check_name, severity, status, violation_count, sample_violations
FROM check_results
WHERE run_id = 'latest-run-uuid'
ORDER BY severity, check_name;
```

---

## 11. Step 7 — Data Catalog

**Files:** `src/run_catalog.py`, `src/catalog/runner.py`, `src/metadata/catalog.yaml`

### Why a data catalog?

A data catalog answers "what tables exist, what do they contain, and who owns them?" — essential for any team working with a shared warehouse. This project builds one automatically by combining:

1. **Introspection** — querying DuckDB's `information_schema` to discover all tables, views, columns, and data types
2. **Business metadata** — YAML file maintained by humans with descriptions, zones, owners, and column documentation

### The YAML metadata structure (`src/metadata/catalog.yaml`)

```yaml
tables:
  transactions:
    zone: raw
    description: "Raw financial transactions loaded from Parquet files"
    owner: "data-engineering"
    columns:
      transaction_id:
        is_pk: true
        description: "Unique identifier for each transaction"
      amount:
        description: "Transaction amount in the specified currency"

  dim_accounts:
    zone: dimension
    description: "Slowly changing dimension (Type 2) for account attributes"
    owner: "data-engineering"
    ...
```

### What the catalog builds

Two tables in the warehouse:

**`catalog_tables`** — one row per table/view:
| Column | Example |
|---|---|
| table_name | `fct_transactions` |
| zone | `fact` |
| description | `"Star schema fact table for..."` |
| owner | `data-engineering` |
| row_count | `10000` |
| column_count | `14` |
| depends_on | `stg_transactions, dim_accounts` |

**`catalog_columns`** — one row per column:
| Column | Example |
|---|---|
| table_name | `fct_transactions` |
| column_name | `account_sk` |
| data_type | `INTEGER` |
| is_pk | `false` |
| is_nullable | `false` |
| description | `"Surrogate key to dim_accounts"` |
| sample_values | `[1, 2, 3, -1, 4]` (JSON) |

The catalog is **fully idempotent**: it DELETE + INSERTs on every run, so it always reflects the current warehouse state.

---

## 12. Step 8 — Dashboard

**Files:** `dashboard/` directory (Evidence.dev project)

### What is Evidence.dev?

Evidence.dev is a framework for building data dashboards using Markdown files with embedded SQL queries. Unlike BI tools with point-and-click interfaces, Evidence dashboards are:
- **Code** — stored in Git, version-controlled, reviewable
- **SQL-native** — queries are written directly in the dashboard files
- **Static** — compiled to a static site (no server needed for production)

### Dashboard pages

| Page | URL | Content |
|---|---|---|
| Pipeline Overview | `/` | Latest run stats for all steps, combined timeline |
| Financial Analytics | `/financial-analytics` | Daily spend charts, top merchants, currency breakdowns |
| Ingestion Health | `/ingestion-health` | Per-run metrics, file history, record trends |
| Data Quality | `/data-quality` | Check results with severity, violations over time |
| Data Catalog | `/data-catalog` | Browsable registry of all tables and columns |

### How Evidence queries work

Each dashboard page is a `.md` file with SQL fenced code blocks:

````markdown
## Daily Spend by Category

```sql daily_spend
SELECT transaction_date, category, SUM(total_amount) as spend
FROM daily_spend_by_category
GROUP BY 1, 2
ORDER BY 1, 2
```

<LineChart data={daily_spend} x="transaction_date" y="spend" series="category" />
````

The SQL results become variables (`daily_spend`), which are passed directly to chart components.

### Running the dashboard

```bash
cd dashboard
npm install       # First time only
npm run dev       # Starts dev server at http://localhost:3000
npm run build     # Builds static site for production
```

---

## 13. Data Model Reference

### Zone map

```
RAW ZONE             STAGING ZONE         DIMENSION ZONE
─────────────        ────────────         ──────────────
transactions    ───► stg_transactions ──► dim_accounts
quarantine                           │   (SCD2)
ingestion_runs                       │
                                     │   FACT ZONE
                                     ├──► fct_transactions
                                     │
                                     │   MART ZONE
                                     ├──► daily_spend_by_category
                                     └──► monthly_account_summary

METADATA ZONE
─────────────
ingestion_runs     transform_runs     check_runs
check_results      dim_build_runs     pipeline_runs
catalog_tables     catalog_columns
```

### Table schemas

#### `transactions` (raw zone)
| Column | Type | Notes |
|---|---|---|
| transaction_id | VARCHAR | Primary key |
| timestamp | TIMESTAMPTZ | Original event timestamp |
| transaction_date | DATE | Derived from timestamp |
| amount | DOUBLE | Positive value |
| currency | VARCHAR | USD / EUR / GBP / CAD |
| merchant_name | VARCHAR | Faker-generated |
| category | VARCHAR | Groceries, Gas, etc. |
| account_id | VARCHAR | e.g. ACC00001 |
| transaction_type | VARCHAR | debit / credit |
| status | VARCHAR | completed / pending / failed |
| source_file | VARCHAR | Origin Parquet filename (lineage) |
| ingested_at | TIMESTAMPTZ | When loaded into DuckDB (lineage) |
| run_id | VARCHAR | UUID for the ingestion run (lineage) |

#### `stg_transactions` (staging zone, VIEW)
Same as `transactions` but:
- `timestamp` renamed to `transaction_timestamp` (avoids SQL reserved word)

#### `dim_accounts` (dimension zone, SCD2)
| Column | Type | Notes |
|---|---|---|
| account_sk | INTEGER | Surrogate key (auto-increment) |
| account_id | VARCHAR | Natural key |
| primary_currency | VARCHAR | Most-used currency |
| primary_category | VARCHAR | Most-used category |
| transaction_count | BIGINT | Total transactions seen |
| total_spend | DOUBLE | Total debit amount |
| first_seen | DATE | Earliest transaction date |
| last_seen | DATE | Most recent transaction date |
| row_hash | VARCHAR | MD5 of changing attributes |
| valid_from | TIMESTAMPTZ | Row effective start |
| valid_to | TIMESTAMPTZ | Row effective end (NULL if current) |
| is_current | BOOLEAN | TRUE for active version |
| run_id | VARCHAR | Which dim build run created this |

#### `fct_transactions` (fact zone)
| Column | Type | Notes |
|---|---|---|
| transaction_id | VARCHAR | Primary key |
| transaction_timestamp | TIMESTAMPTZ | |
| transaction_date | DATE | |
| account_sk | INTEGER | FK to dim_accounts (-1 = unknown) |
| account_id | VARCHAR | Denormalized for convenience |
| amount | DOUBLE | |
| currency | VARCHAR | |
| merchant_name | VARCHAR | |
| category | VARCHAR | |
| transaction_type | VARCHAR | |
| status | VARCHAR | |
| source_file | VARCHAR | Lineage |
| ingested_at | TIMESTAMPTZ | Lineage |
| run_id | VARCHAR | Lineage |

#### `daily_spend_by_category` (mart zone)
| Column | Type | Grain |
|---|---|---|
| transaction_date | DATE | Part of grain |
| category | VARCHAR | Part of grain |
| currency | VARCHAR | Part of grain |
| total_amount | DOUBLE | Measure |
| transaction_count | BIGINT | Measure |
| avg_amount | DOUBLE | Measure |

#### `monthly_account_summary` (mart zone)
| Column | Type | Grain |
|---|---|---|
| month | DATE | Part of grain (truncated to month) |
| account_id | VARCHAR | Part of grain |
| currency | VARCHAR | Part of grain |
| total_debits | DOUBLE | Measure |
| total_credits | DOUBLE | Measure |
| net_flow | DOUBLE | Measure (credits - debits) |
| transaction_count | BIGINT | Measure |

---

## 14. The Orchestrator: Tying It All Together

**Files:** `src/run_pipeline.py`, `src/orchestrator/runner.py`

The orchestrator runs all 8 steps in sequence as subprocesses. Running as subprocesses (rather than function calls) provides:
- Clean process isolation (a step crash doesn't kill the orchestrator)
- Real-time log streaming
- Easy step filtering

### Step sequence

```python
PIPELINE_STEPS = [
    "generate",      # generate_transactions.py
    "ingest",        # ingest_transactions.py
    "transforms",    # run_transforms.py
    "dim-build",     # run_dim_build.py
    "fact-build",    # run_transforms.py --transforms-dir src/fact_transforms
    "checks",        # run_checks.py
    "catalog",       # run_catalog.py
    "dashboard",     # npm run sources (in dashboard/)
]
```

### Flexible step control

```bash
# Run all steps
uv run src/run_pipeline.py

# Run only specific steps
uv run src/run_pipeline.py --step ingest --step transforms

# Start from a specific step (resume a partially-completed pipeline)
uv run src/run_pipeline.py --from-step checks

# Skip steps (e.g., skip dashboard in CI)
uv run src/run_pipeline.py --skip-steps dashboard
```

### Metadata tracking

Every pipeline execution creates a row in `pipeline_runs` with:
- Start/end timestamps
- Status (success / failed / partial)
- Which steps ran, which were skipped
- Overall elapsed time

---

## 15. Testing Strategy

**Directory:** `tests/`

The test suite has three categories:

### Unit tests (`tests/unit/`)

Test individual functions in isolation. Example:

```python
# tests/unit/test_validator.py
def test_validate_records_rejects_negative_amount():
    df = pl.DataFrame({"amount": [-5.0], "currency": ["USD"], ...})
    valid, invalid = validate_records(df)
    assert len(invalid) == 1
    assert "amount" in invalid["rejection_reason"][0]
```

### Integration tests (`tests/integration/`)

Test full pipeline stages end-to-end with a real (temporary) DuckDB database. Example:

```python
# tests/integration/test_ingestion.py
def test_full_ingestion_pipeline(tmp_path):
    # Generate test Parquet files
    generate_test_data(tmp_path / "raw")

    # Run ingestion
    run_pipeline(source_dir=tmp_path / "raw", db_path=tmp_path / "test.duckdb")

    # Assert results
    conn = duckdb.connect(tmp_path / "test.duckdb")
    count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    assert count > 0
```

### Quality tests (`tests/quality/`)

Validate data against the 6Cs framework:
- **Completeness** — no unexpected NULLs
- **Consistency** — referential integrity (all fact rows have valid dimension keys)
- **Correctness** — values within expected ranges
- **Currency** — data is fresh (not stale)
- **Conformity** — values match allowed enumerations
- **Cardinality** — row counts meet expectations

### Running tests

```bash
# All tests
uv run pytest

# With coverage report
uv run pytest --cov=src --cov-report=term-missing

# Specific test file
uv run pytest tests/unit/test_validator.py -v

# Specific test
uv run pytest tests/unit/test_validator.py::test_validate_records_rejects_negative_amount -v
```

---

## 16. CI/CD Pipeline

**File:** `.github/workflows/ci.yml`

Three jobs run in sequence on every push or pull request to `main`:

```
┌──────────┐    ┌──────────┐    ┌──────────────────────┐
│   Lint   │───►│   Test   │───►│   Pipeline (E2E)     │
│          │    │          │    │                      │
│ ruff     │    │ pytest   │    │ uv run run_pipeline  │
│ check .  │    │ (with    │    │ --skip-steps         │
│          │    │  report) │    │   dashboard          │
└──────────┘    └──────────┘    └──────────────────────┘
```

**Lint job:** Runs `ruff check .` — fails on any linting error. Ruff enforces PEP 8, catches unused imports, enforces modern Python patterns.

**Test job:** Runs `pytest` and publishes the markdown report to the GitHub Actions step summary (visible directly in the PR).

**Pipeline job:** Runs the entire pipeline end-to-end (minus dashboard which requires npm). This catches integration issues that unit tests miss — e.g., SQL syntax errors, wrong table names, missing columns.

---

## 17. Key Design Patterns Explained

### Pattern 1: Idempotency

Every step is designed to be safely re-run without creating duplicates or errors:
- Ingestion uses a manifest to skip already-processed files
- Cross-file deduplication skips records already in `transactions`
- SQL transforms use `CREATE OR REPLACE`
- Dim build uses hash comparison to avoid unnecessary updates
- Catalog uses DELETE + INSERT

**Why it matters:** In real pipelines, steps fail and need to be retried. Idempotency means you can safely retry without fear of side effects.

### Pattern 2: Lineage tracking

Every record in the warehouse knows where it came from:
- `source_file` — which Parquet file it came from
- `ingested_at` — when it entered the warehouse
- `run_id` — which pipeline run created it

**Why it matters:** When a data issue is discovered, lineage lets you trace "which file introduced this bad record?" and "when was it loaded?"

### Pattern 3: Metadata as data

Instead of logs in files, pipeline metadata is stored in the warehouse itself:
- `ingestion_runs` — tracks every ingestion execution
- `transform_runs` — tracks every transform execution
- `check_results` — stores quality check outcomes with sample violations
- `pipeline_runs` — tracks overall pipeline executions

**Why it matters:** Metadata stored in the warehouse is queryable, chartable, and retained across runs. The dashboard's "Data Quality" and "Ingestion Health" pages are powered by these metadata tables.

### Pattern 4: SQL transforms as files

SQL files are first-class artifacts, not embedded strings in Python code. This means:
- SQL is readable and reviewable without understanding Python
- Engineers can add new transforms by just adding a `.sql` file
- Dependency declaration is in the SQL header itself
- CI/CD can lint SQL files independently

### Pattern 5: Exit codes for pipeline control

Quality checks use standard Unix exit codes:
- `0` — success (including warn-only failures)
- `1` — failure (critical check violations)

The orchestrator respects exit codes — a critical check failure stops the pipeline. This is the standard pattern for composing CLI tools into pipelines.

### Pattern 6: Unknown member in dimensions (-1)

When a transaction's `account_id` has no matching row in `dim_accounts`, the fact table uses `account_sk = -1` (the "unknown member"). This is preferable to NULL because:
- Queries like `WHERE account_sk = -1` explicitly find unknowns
- Aggregations that GROUP BY `account_sk` don't silently merge unknowns (NULL groups together with NULL)
- It signals "this is intentionally unmatched" rather than "join failed silently"

---

## 18. Running It Yourself

### Prerequisites

- Python 3.11+
- `uv` — install with `curl -LsSf https://astral.sh/uv/install.sh | sh`
- Node.js 18+ + npm (only needed for dashboard step)

### Setup

```bash
# Clone the repo
git clone <repo-url>
cd learn-de-opencode

# Install Python dependencies
uv sync

# Install dashboard dependencies (optional)
cd dashboard && npm install && cd ..
```

### Run the full pipeline

```bash
# Run everything (skipping dashboard if npm not installed)
uv run src/run_pipeline.py --skip-steps dashboard

# Or run each step manually for learning purposes:
uv run src/generate_transactions.py --count 5000 --seed 42
uv run src/ingest_transactions.py
uv run src/run_transforms.py
uv run src/run_dim_build.py
uv run src/run_transforms.py --transforms-dir src/fact_transforms
uv run src/run_checks.py
uv run src/run_catalog.py

# Start the dashboard
cd dashboard && npm run dev
# Open http://localhost:3000
```

### Query the warehouse directly

```bash
# Open DuckDB CLI
uv run python -c "import duckdb; duckdb.connect('data/warehouse.duckdb').execute('.tables')"

# Or use the DuckDB CLI if installed
duckdb data/warehouse.duckdb
```

```sql
-- Explore what's in the warehouse
SHOW TABLES;

-- Check row counts
SELECT table_name, row_count FROM catalog_tables ORDER BY row_count DESC;

-- Latest ingestion run
SELECT * FROM ingestion_runs ORDER BY started_at DESC LIMIT 1;

-- Latest quality check results
SELECT check_name, severity, status, violation_count
FROM check_results
WHERE run_id = (SELECT run_id FROM check_runs ORDER BY started_at DESC LIMIT 1);

-- Top spending categories
SELECT category, SUM(total_amount) AS spend
FROM daily_spend_by_category
GROUP BY category
ORDER BY spend DESC;
```

---

## 19. Concept Glossary

| Term | Definition |
|---|---|
| **DuckDB** | An embedded analytical database — like SQLite but optimized for analytics. Stores data in a single `.duckdb` file. No server required. |
| **Polars** | A DataFrame library for Python (similar to pandas but faster, using Arrow memory format). |
| **Parquet** | A columnar file format ideal for analytics. Stores data column-by-column, compresses well, preserves types. |
| **PyArrow** | Apache Arrow implementation for Python. Provides a common memory format enabling zero-copy data sharing between libraries. |
| **Idempotent** | A pipeline step that can be run multiple times and produces the same result as running it once. |
| **Lineage** | The ability to trace a piece of data back to its source — which file it came from, when it was loaded, by which pipeline run. |
| **SCD Type 2** | Slowly Changing Dimension Type 2 — tracks historical changes to dimension attributes by adding new rows with effective date ranges instead of overwriting. |
| **Surrogate key** | An artificial primary key (usually an auto-incrementing integer) used in dimension tables instead of the natural business key. |
| **Natural key** | The real-world identifier for an entity (e.g., `account_id = "ACC00001"`). |
| **Unknown member** | A special dimension row (account_sk = -1) representing "no matching dimension record found". |
| **Point-in-time join** | A join that matches fact records to the dimension row that was active at the time of the fact event. |
| **Star schema** | A warehouse design with a central fact table surrounded by dimension tables — shaped like a star. |
| **DAG** | Directed Acyclic Graph — a graph where edges have direction and no cycles. Used to represent transform dependencies. |
| **Topological sort** | An ordering of nodes in a DAG such that every node appears before all nodes it points to. Used to determine SQL execution order. |
| **Quarantine** | A holding area for records that fail validation — isolates bad data without losing it. |
| **Manifest** | A tracking table that records which files have been processed, enabling incremental ingestion. |
| **Grain** | The level of detail in a table — e.g., "one row per transaction" or "one row per day/category/currency". |
| **Mart** | Pre-aggregated tables built for specific consumption patterns (reporting, dashboards). |
| **Evidence.dev** | A dashboard framework where pages are Markdown files with embedded SQL queries. Git-native, SQL-driven. |
| **Ruff** | A fast Python linter and code formatter, written in Rust. Enforces code style and catches common errors. |
| **uv** | An ultra-fast Python package manager and project tool, written in Rust. Replacement for pip + virtualenv. |
| **6Cs framework** | A data quality framework: Completeness, Consistency, Correctness, Currency, Conformity, Cardinality. |

---

*This guide was generated for the `learn-de-opencode` repository. For questions or improvements, open an issue or pull request.*
