# Data Model: Evidence.dev Dashboard

**Feature**: 005-evidence-dashboard
**Date**: 2026-02-23

## Data Architecture

This feature creates **no new DuckDB tables**. The dashboard reads from existing tables/views created by Features 001-004. Data flows from DuckDB through Evidence.dev's source extraction layer into page-level queries.

```
DuckDB Warehouse                 Evidence.dev
(transactions.duckdb)            (dashboard/)
                                 
  transactions ──────────────┐
  quarantine ────────────────┤   sources/warehouse/*.sql
  ingestion_runs ────────────┤        │
  stg_transactions ──────────┤        ▼
  daily_spend_by_category ───┤   .evidence/template/
  monthly_account_summary ───┤   (extracted Parquet)
  transform_runs ────────────┤        │
  check_runs ────────────────┤        ▼
  check_results ─────────────┘   pages/*.md
                                 (inline SQL + components)
```

## Source Tables (read-only)

### transactions (Feature 002)

| Column | Type | Dashboard Usage |
|--------|------|----------------|
| transaction_id | VARCHAR | Count for metrics |
| timestamp | TIMESTAMPTZ | Not directly used (use transaction_date) |
| amount | DOUBLE | Aggregation for top merchants, currency breakdown |
| currency | VARCHAR | Currency breakdown grouping |
| merchant_name | VARCHAR | Top merchants ranking |
| category | VARCHAR | Spend by category (alternative to mart) |
| account_id | VARCHAR | Account summary (alternative to mart) |
| transaction_type | VARCHAR | Debit/credit classification |
| status | VARCHAR | Not used in dashboard |
| transaction_date | DATE | Time axis for financial charts |
| source_file | VARCHAR | Not used in dashboard |
| ingested_at | TIMESTAMPTZ | Not used in dashboard |
| run_id | VARCHAR | Not used in dashboard |

### quarantine (Feature 002)

| Column | Type | Dashboard Usage |
|--------|------|----------------|
| quarantine_id | INTEGER | Count for metrics |
| source_file | VARCHAR | Not used in dashboard |
| record_data | VARCHAR | Not used in dashboard |
| rejection_reason | VARCHAR | Quarantine breakdown by reason |
| rejected_at | TIMESTAMPTZ | Not used in dashboard |
| run_id | VARCHAR | Join to ingestion_runs |

### ingestion_runs (Feature 002)

| Column | Type | Dashboard Usage |
|--------|------|----------------|
| run_id | VARCHAR | Row identity |
| started_at | TIMESTAMPTZ | Time axis for trend charts, timeline |
| completed_at | TIMESTAMPTZ | Not used in dashboard |
| status | VARCHAR | Status indicator, overview card |
| files_processed | INTEGER | Run detail |
| records_loaded | INTEGER | BigValue metric, trend chart |
| records_quarantined | INTEGER | BigValue metric, trend chart |
| duplicates_skipped | INTEGER | BigValue metric, trend chart |
| elapsed_seconds | DOUBLE | Run detail |

### daily_spend_by_category (Feature 003, mart)

| Column | Type | Dashboard Usage |
|--------|------|----------------|
| transaction_date | DATE | X-axis for spend chart |
| category | VARCHAR | Series grouping |
| currency | VARCHAR | Filter/grouping |
| total_amount | DOUBLE | Y-axis value |
| transaction_count | BIGINT | Secondary metric |
| avg_amount | DOUBLE | Not used in dashboard |

### monthly_account_summary (Feature 003, mart)

| Column | Type | Dashboard Usage |
|--------|------|----------------|
| month | DATE | X-axis / row grouping |
| account_id | VARCHAR | Row grouping |
| currency | VARCHAR | Row grouping |
| total_debits | DOUBLE | Table/chart value |
| total_credits | DOUBLE | Table/chart value |
| net_flow | DOUBLE | Table/chart value |
| transaction_count | BIGINT | Secondary metric |

### transform_runs (Feature 003)

| Column | Type | Dashboard Usage |
|--------|------|----------------|
| run_id | VARCHAR | Row identity |
| started_at | TIMESTAMPTZ | Time axis, timeline |
| completed_at | TIMESTAMPTZ | Not used in dashboard |
| status | VARCHAR | Status indicator, overview card |
| models_executed | INTEGER | Run detail |
| models_failed | INTEGER | Run detail |
| elapsed_seconds | DOUBLE | Run detail |
| error_message | VARCHAR | Not used in dashboard |

### check_runs (Feature 004)

| Column | Type | Dashboard Usage |
|--------|------|----------------|
| run_id | VARCHAR | Row identity, join key |
| started_at | TIMESTAMPTZ | Time axis, timeline |
| completed_at | TIMESTAMPTZ | Not used in dashboard |
| status | VARCHAR | Status indicator, overview card |
| total_checks | INTEGER | BigValue metric |
| checks_passed | INTEGER | BigValue metric, trend chart |
| checks_failed | INTEGER | BigValue metric, trend chart |
| checks_errored | INTEGER | BigValue metric, trend chart |
| elapsed_seconds | DOUBLE | Run detail |
| error_message | VARCHAR | Not used in dashboard |

### check_results (Feature 004)

| Column | Type | Dashboard Usage |
|--------|------|----------------|
| id | VARCHAR | Row identity |
| run_id | VARCHAR | Join to check_runs |
| check_name | VARCHAR | Per-check grouping |
| severity | VARCHAR | Severity indicator |
| description | VARCHAR | Detail display |
| status | VARCHAR | Pass/fail/error indicator |
| violation_count | INTEGER | Metric display |
| sample_violations | VARCHAR | Not used in dashboard |
| elapsed_seconds | DOUBLE | Not used in dashboard |
| error_message | VARCHAR | Error detail |

## Source Extraction Queries

SQL files placed in `dashboard/sources/warehouse/` that extract data from DuckDB into Evidence's internal Parquet format. Each file creates a table accessible as `warehouse.<filename>`.

| Source Query File | Target Table | SQL Summary |
|-------------------|-------------|-------------|
| `transactions.sql` | `warehouse.transactions` | `SELECT * FROM transactions` |
| `quarantine.sql` | `warehouse.quarantine` | `SELECT * FROM quarantine` |
| `ingestion_runs.sql` | `warehouse.ingestion_runs` | `SELECT * FROM ingestion_runs` |
| `daily_spend_by_category.sql` | `warehouse.daily_spend_by_category` | `SELECT * FROM daily_spend_by_category` |
| `monthly_account_summary.sql` | `warehouse.monthly_account_summary` | `SELECT * FROM monthly_account_summary` |
| `transform_runs.sql` | `warehouse.transform_runs` | `SELECT * FROM transform_runs` |
| `check_runs.sql` | `warehouse.check_runs` | `SELECT * FROM check_runs` |
| `check_results.sql` | `warehouse.check_results` | `SELECT * FROM check_results` |

Note: Source queries use `SELECT *` to extract full table contents. Evidence extracts these into Parquet at dev-server start and when `npm run sources` is run. Page-level inline queries then aggregate/filter as needed.

## Evidence.dev Configuration

### evidence.plugins.yaml

```yaml
datasources:
  evidence-connector-duckdb:
    package: "@evidence-dev/duckdb"
```

### sources/warehouse/connection.yaml

```yaml
name: warehouse
type: duckdb
options:
  filename: ../../data/warehouse/transactions.duckdb
  access_mode: read_only
```

## No New Domain Models

This feature does not introduce any new Python domain models, enums, or dataclasses. It is a pure presentation layer built entirely with Evidence.dev's markdown + SQL + component system.
