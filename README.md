# learn_de - Data Engineering Learning Project

A production-style data engineering project built with Python, Polars, and DuckDB. Generates realistic synthetic financial transaction data, ingests it into a local analytical warehouse with schema validation, deduplication, and lineage tracking, then transforms it into curated staging views and analytical mart tables using a lightweight dbt-inspired SQL runner.

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) package manager

## Setup

```bash
# Clone the repository
git clone https://github.com/phamhoangtuan/learn-de-opencode.git
cd learn-de-opencode

# Install dependencies
uv sync --all-extras
```

## Quick Start

### 1. Generate Synthetic Data

```bash
uv run src/generate_transactions.py --count 10000 --seed 42
```

This creates a Parquet file at `data/raw/transactions_YYYYMMDD_HHMMSS.parquet` with 10,000 realistic financial transactions.

**Generator options:**

| Option | Default | Description |
|--------|---------|-------------|
| `--count` | 10000 | Number of transactions to generate |
| `--seed` | random | Random seed for reproducibility |
| `--start-date` | 90 days ago | Start date (YYYY-MM-DD) |
| `--end-date` | today | End date (YYYY-MM-DD) |
| `--accounts` | 100 | Number of unique accounts |
| `--format` | parquet | Output format: `parquet` or `csv` |
| `--output-dir` | data/raw | Output directory |

### 2. Ingest Data into DuckDB

```bash
uv run src/ingest_transactions.py
```

This reads all Parquet files from `data/raw/`, validates, deduplicates, and loads them into a DuckDB warehouse at `data/warehouse/transactions.duckdb`.

**Ingestion options:**

| Option | Default | Description |
|--------|---------|-------------|
| `--source-dir` | data/raw | Source directory for Parquet files |
| `--db-path` | data/warehouse/transactions.duckdb | Path to DuckDB database |

The pipeline outputs a run summary:

```
Pipeline Run Summary (abc123...):
  Status:              completed
  Files processed:     1
  Records loaded:      10000
  Records quarantined: 0
  Duplicates skipped:  0
  Elapsed time:        0.85s
```

Re-running the pipeline is safe -- it is fully idempotent. Duplicate records are detected and skipped automatically.

### 3. Run SQL Transformations

```bash
uv run src/run_transforms.py
```

This executes all SQL transform files in `src/transforms/` against the DuckDB warehouse in dependency order, creating:
- **`stg_transactions`** -- a staging VIEW that standardizes column names (e.g., `timestamp` becomes `transaction_timestamp`)
- **`daily_spend_by_category`** -- a materialized TABLE aggregating completed debit transactions by date, category, and currency
- **`monthly_account_summary`** -- a materialized TABLE aggregating account-level debits, credits, and net flow by month and currency

**Transform options:**

| Option | Default | Description |
|--------|---------|-------------|
| `--db-path` | data/warehouse/transactions.duckdb | Path to DuckDB database |
| `--transforms-dir` | src/transforms | Directory containing .sql transform files |
| `--verbose` | off | Enable debug-level logging |

The runner outputs a summary:

```
Transform Run Summary (abc123...):
  Status:           completed
  Models executed:  3
  Models failed:    0
  Elapsed time:     0.05s
```

All transforms use `CREATE OR REPLACE` patterns, making re-runs fully idempotent. Each execution is tracked in a `transform_runs` metadata table.

### 4. Query the Warehouse

```python
import duckdb

conn = duckdb.connect("data/warehouse/transactions.duckdb")

# Count loaded records
conn.sql("SELECT COUNT(*) FROM transactions").show()

# Check lineage (source file and run traceability)
conn.sql("SELECT source_file, run_id, COUNT(*) FROM transactions GROUP BY 1, 2").show()

# Query by date range
conn.sql("""
    SELECT transaction_date, COUNT(*) AS cnt, ROUND(SUM(amount), 2) AS total
    FROM transactions
    GROUP BY transaction_date
    ORDER BY transaction_date
""").show()

# Query the staging view (standardized column names)
conn.sql("SELECT * FROM stg_transactions LIMIT 5").show()

# Daily spending by category
conn.sql("SELECT * FROM daily_spend_by_category ORDER BY transaction_date, category").show()

# Monthly account summary with net flow
conn.sql("SELECT * FROM monthly_account_summary ORDER BY month, account_id").show()

# Review quarantined records
conn.sql("SELECT * FROM quarantine").show()

# Review ingestion run history
conn.sql("SELECT * FROM ingestion_runs ORDER BY started_at DESC").show()

# Review transform run history
conn.sql("SELECT * FROM transform_runs ORDER BY started_at DESC").show()

conn.close()
```

## Pipeline Features

- **Schema validation** -- two-tier checks: file-level (columns, types) and record-level (nulls, ranges, enums)
- **Quarantine** -- invalid records routed to a `quarantine` table with rejection reason and lineage
- **Deduplication** -- within-file and cross-file dedup by `transaction_id`; fully idempotent re-ingestion
- **Lineage tracking** -- every record tagged with `source_file`, `ingested_at`, and `run_id`
- **Date partitioning** -- derived `transaction_date` column for efficient date-range queries
- **Run tracking** -- each pipeline execution logged in `ingestion_runs` with full statistics
- **SQL transformations** -- dbt-inspired staging views and mart tables with DAG-based dependency resolution
- **Idempotent transforms** -- all SQL uses `CREATE OR REPLACE` for safe re-runs
- **Transform metadata** -- every transform execution tracked in `transform_runs` table

## Data Model

The DuckDB warehouse contains the following tables and views:

### Raw Zone

**`transactions`** -- 13 columns
| Column | Type | Description |
|--------|------|-------------|
| transaction_id | VARCHAR (PK) | Unique transaction identifier |
| timestamp | TIMESTAMPTZ | Transaction timestamp (UTC) |
| amount | DOUBLE | Transaction amount (positive) |
| currency | VARCHAR | USD, EUR, GBP, or JPY |
| merchant_name | VARCHAR | Merchant name |
| category | VARCHAR | Merchant category |
| account_id | VARCHAR | Account identifier (ACC-XXXXX) |
| transaction_type | VARCHAR | debit or credit |
| status | VARCHAR | completed, pending, or failed |
| transaction_date | DATE | Derived from timestamp |
| source_file | VARCHAR | Source Parquet filename |
| ingested_at | TIMESTAMPTZ | When the record was ingested |
| run_id | VARCHAR | Pipeline run identifier |

**`quarantine`** -- rejected records with rejection reason and lineage

### Staging Zone

**`stg_transactions`** (VIEW) -- standardized version of `transactions`
- Renames `timestamp` to `transaction_timestamp` (avoids SQL reserved word)
- Passes through all columns including lineage

### Marts Zone

**`daily_spend_by_category`** (TABLE)
| Column | Type | Description |
|--------|------|-------------|
| transaction_date | DATE | Spending date |
| category | VARCHAR | Merchant category |
| currency | VARCHAR | Transaction currency |
| total_amount | DOUBLE | Sum of debit amounts |
| transaction_count | INTEGER | Number of transactions |
| avg_amount | DOUBLE | Average transaction amount |

Only includes completed debit transactions. Grain: (transaction_date, category, currency).

**`monthly_account_summary`** (TABLE)
| Column | Type | Description |
|--------|------|-------------|
| month | DATE | First day of month |
| account_id | VARCHAR | Account identifier |
| currency | VARCHAR | Transaction currency |
| total_debits | DOUBLE | Sum of debit amounts |
| total_credits | DOUBLE | Sum of credit amounts |
| net_flow | DOUBLE | credits - debits |
| transaction_count | INTEGER | Number of transactions |

Only includes completed transactions. Grain: (month, account_id, currency).

### Metadata Tables

**`ingestion_runs`** -- pipeline execution history with statistics

**`transform_runs`** -- transform execution history with model counts and timing

## Running Tests

```bash
# Run full test suite
uv run pytest

# Run with coverage
uv run pytest --cov=src --cov-report=term-missing

# Run specific test tiers
uv run pytest tests/unit/           # Unit tests
uv run pytest tests/integration/    # Integration tests
uv run pytest tests/quality/        # Data quality tests (6Cs)
```

## Linting

```bash
uv run ruff check .
```

## Project Structure

```
src/
  generate_transactions.py        # CLI: synthetic data generator
  ingest_transactions.py          # CLI: ingestion pipeline
  run_transforms.py               # CLI: SQL transform runner
  ingestion/
    pipeline.py                   # Pipeline orchestration
    validator.py                  # Schema + value validation
    loader.py                     # DuckDB write operations
    dedup.py                      # Deduplication logic
    models.py                     # Domain models (RunResult, etc.)
  transformer/
    runner.py                     # Transform orchestration + metadata
    parser.py                     # SQL file parser (model/depends_on headers)
    graph.py                      # DAG resolution (Kahn's algorithm)
    models.py                     # Domain models (TransformModel, etc.)
  transforms/
    staging__stg_transactions.sql           # Staging view
    mart__daily_spend_by_category.sql       # Daily spend mart
    mart__monthly_account_summary.sql       # Monthly summary mart
  models/                         # Transaction schema (Feature 001)
  lib/                            # Shared utilities

tests/
  unit/                           # Unit tests
  integration/                    # End-to-end pipeline tests
  quality/                        # Data quality validation (6Cs)

specs/                            # Feature specifications
data/
  raw/                            # Source Parquet files (gitignored)
  warehouse/                      # DuckDB database (gitignored)
```

## Technologies

- **Python 3.11+** -- runtime
- **Polars** -- DataFrame processing and Parquet I/O
- **DuckDB** -- embedded analytical warehouse
- **PyArrow** -- zero-copy Arrow interchange between Polars and DuckDB
- **NumPy** -- statistical distributions for data generation
- **Faker** -- realistic names and merchants
- **pytest** -- testing framework
- **ruff** -- linting and formatting
- **uv** -- dependency management
