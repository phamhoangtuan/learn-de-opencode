# Implementation Plan: Evidence.dev Dashboard

**Branch**: `005-evidence-dashboard` | **Date**: 2026-02-23 | **Spec**: `specs/005-evidence-dashboard/spec.md`

## Summary

Build an Evidence.dev dashboard project in `dashboard/` that connects to the existing DuckDB warehouse (`data/warehouse/transactions.duckdb`) in read-only mode and provides four interactive pages: a pipeline overview home page, financial analytics, ingestion health monitoring, and data quality check results. The dashboard uses Evidence.dev's DuckDB source plugin for data extraction, inline SQL queries in markdown for page-level aggregation, and built-in chart components (BigValue, BarChart, LineChart, DataTable) for visualization. Runnable via `npm run dev` from the `dashboard/` directory.

## Technical Context

**Language/Version**: Node.js (Evidence.dev framework, Svelte-based)
**Primary Dependencies**: Evidence.dev (latest stable), `@evidence-dev/duckdb` plugin
**Storage**: Read-only connection to `data/warehouse/transactions.duckdb`
**Testing**: Manual verification -- Evidence.dev pages render without errors, charts display correct data
**Target Platform**: Local development (macOS/Linux), `npm run dev`
**Project Type**: Standalone dashboard (separate from Python src/)
**Performance Goals**: Dev server starts and renders home page within 30 seconds (SC-001)
**Constraints**: Read-only database access (FR-009), no date range filters (clarification Q4), dev mode only (clarification Q2)
**Scale/Scope**: 4 pages, 8 source extraction queries, ~15 inline queries across pages

## Constitution Check

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Data Quality First | N/A | Dashboard is read-only presentation; does not produce data |
| II. Metadata & Lineage | Pass | Displays pipeline metadata (ingestion/transform/check runs); does not create new metadata |
| III. Security & Privacy | Pass | Read-only connection; no PII exposure beyond what's in source tables; no auth needed for local dev |
| IV. Integration & Interop | Pass | Uses standard DuckDB connector; Evidence.dev is open-source; no vendor lock-in for data |
| V. Architecture Integrity | Pass | Clean separation: `dashboard/` is a standalone project reading from the shared warehouse |

## Project Structure

### Documentation

```text
specs/005-evidence-dashboard/
  spec.md
  plan.md
  research.md
  data-model.md
  checklists/
  tasks.md
```

### Source Code

```text
dashboard/
  package.json
  evidence.plugins.yaml
  .gitignore
  sources/
    warehouse/
      connection.yaml
      transactions.sql
      quarantine.sql
      ingestion_runs.sql
      daily_spend_by_category.sql
      monthly_account_summary.sql
      transform_runs.sql
      check_runs.sql
      check_results.sql
  pages/
    index.md                    # Overview / home page (P4)
    financial-analytics.md      # Financial analytics (P1)
    ingestion-health.md         # Ingestion health (P2)
    data-quality.md             # Data quality checks (P3)
```

**Structure Decision**: Standalone `dashboard/` directory at project root, separate from the Python `src/` codebase. This is a Node.js project (Evidence.dev) that reads from the shared DuckDB warehouse. No Python code involved. The `dashboard/node_modules/` and `dashboard/.evidence/` directories are gitignored.

## Page Designs

### Home / Overview (`pages/index.md`)

**Purpose**: At-a-glance pipeline health summary (User Story 4).

**Content**:
1. **Header**: "Pipeline Overview"
2. **Summary cards** (BigValue): Latest ingestion run status + records loaded; Latest transform run status + models executed; Latest check run status + checks passed
3. **Pipeline timeline** (DataTable): Combined chronological list of all run types (ingestion, transform, check) sorted by started_at descending, showing run_type, started_at, status, and key metric

### Financial Analytics (`pages/financial-analytics.md`)

**Purpose**: Spending analysis across categories, accounts, merchants, and currencies (User Story 1).

**Content**:
1. **Daily Spend by Category** (BarChart, stacked): X = transaction_date, Y = total_amount, series = category. From `daily_spend_by_category` mart.
2. **Monthly Account Summary** (DataTable): Columns: month, account_id, currency, total_debits, total_credits, net_flow, transaction_count. From `monthly_account_summary` mart.
3. **Top Merchants by Spend** (BarChart, horizontal): Top 10 merchants ranked by total absolute amount. From `transactions` table aggregation.
4. **Currency Breakdown** (BarChart): Transaction count and total amount per currency. From `transactions` table aggregation.

### Ingestion Health (`pages/ingestion-health.md`)

**Purpose**: Monitor ingestion pipeline runs and quarantine activity (User Story 2).

**Content**:
1. **Latest Run Summary** (BigValue cards): Records loaded, records quarantined, duplicates skipped from most recent run.
2. **Ingestion Runs History** (DataTable): All runs with run_id, started_at, status, records_loaded, records_quarantined, duplicates_skipped, elapsed_seconds.
3. **Records Trend** (LineChart): X = started_at, Y = records_loaded / records_quarantined / duplicates_skipped over time.
4. **Quarantine Breakdown by Reason** (BarChart): rejection_reason counts from quarantine table.

### Data Quality (`pages/data-quality.md`)

**Purpose**: Track check results and quality trends (User Story 3).

**Content**:
1. **Latest Run Summary** (BigValue cards): Checks passed, checks failed, checks errored from most recent check run.
2. **Check Results History** (DataTable): Per-check results from latest run: check_name, severity, status, violation_count, description.
3. **Check Outcomes Over Time** (BarChart, stacked): X = started_at (from check_runs), Y = count, series = status (pass/fail/error). Shows trend per run.
4. **Check Results Detail** (DataTable): Full check_results table with run_id, check_name, severity, status, violation_count.

## Complexity Tracking

No constitution violations. This feature is a read-only presentation layer with no data mutation.
