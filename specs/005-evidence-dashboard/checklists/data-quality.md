# Data Quality Checklist: Evidence.dev Dashboard

**Feature**: 005-evidence-dashboard
**Date**: 2026-02-23
**Purpose**: Verify the dashboard accurately presents data from the DuckDB warehouse, maintains read-only access, and follows DAMA-DMBOK data quality standards for reporting and visualization

## Accuracy

- [ ] CHK001: Financial analytics charts display values matching direct DuckDB `daily_spend_by_category` query results
- [ ] CHK002: Monthly account summary table values (debits, credits, net_flow) match direct DuckDB `monthly_account_summary` query results
- [ ] CHK003: Top merchants ranking matches a direct `SELECT merchant_name, SUM(ABS(amount)) FROM transactions GROUP BY merchant_name ORDER BY 2 DESC LIMIT 10` query
- [ ] CHK004: Currency breakdown chart values match direct aggregation on `transactions` by currency
- [ ] CHK005: Ingestion health BigValue cards (records_loaded, records_quarantined, duplicates_skipped) match the most recent `ingestion_runs` row
- [ ] CHK006: Quarantine breakdown by reason matches direct `SELECT rejection_reason, COUNT(*) FROM quarantine GROUP BY 1`
- [ ] CHK007: Data quality BigValue cards (checks_passed, checks_failed, checks_errored) match the most recent `check_runs` row
- [ ] CHK008: Check results detail table matches direct query of `check_results` joined with `check_runs`
- [ ] CHK009: Pipeline overview summary cards reflect the latest row from each of `ingestion_runs`, `transform_runs`, and `check_runs`

## Completeness

- [ ] CHK010: All 8 source extraction SQL files exist in `dashboard/sources/warehouse/` (transactions, quarantine, ingestion_runs, daily_spend_by_category, monthly_account_summary, transform_runs, check_runs, check_results)
- [ ] CHK011: All 4 dashboard pages exist (index.md, financial-analytics.md, ingestion-health.md, data-quality.md)
- [ ] CHK012: Financial analytics page includes all 4 sections: daily spend by category, monthly account summary, top merchants, currency breakdown
- [ ] CHK013: Ingestion health page includes all 4 sections: latest run summary, run history, records trend, quarantine breakdown
- [ ] CHK014: Data quality page includes all 4 sections: latest run summary, check results history, outcomes over time, results detail
- [ ] CHK015: Overview page includes summary cards for ingestion, transform, and check runs plus a combined pipeline timeline

## Consistency

- [ ] CHK016: Source extraction queries use `SELECT *` to extract full tables without transformation (data transformation happens in inline page queries only)
- [ ] CHK017: All pages reference `warehouse.*` as the source prefix consistently
- [ ] CHK018: Chart component types are used consistently for similar data (e.g., BigValue for summary metrics, DataTable for tabular detail, BarChart/LineChart for trends)
- [ ] CHK019: Column naming in inline queries is consistent across pages (e.g., same alias for status, started_at, run_id)

## Read-Only Integrity (FR-009)

- [ ] CHK020: `connection.yaml` specifies `access_mode: read_only`
- [ ] CHK021: No SQL queries in source files or pages contain INSERT, UPDATE, DELETE, CREATE, ALTER, or DROP statements
- [ ] CHK022: Dashboard operation does not modify the DuckDB warehouse file (file checksum unchanged after dev server start and page navigation)
- [ ] CHK023: Dashboard can run while another process holds a write connection to the DuckDB file (no locking conflict)

## Configuration & Connectivity

- [ ] CHK024: `evidence.plugins.yaml` correctly declares the `@evidence-dev/duckdb` datasource plugin
- [ ] CHK025: `connection.yaml` uses relative path `../../data/warehouse/transactions.duckdb` to reference the warehouse
- [ ] CHK026: `npm run dev` starts the Evidence dev server without errors when the DuckDB warehouse exists and contains data
- [ ] CHK027: Dashboard serves the home/overview page as the landing page at `/`

## Empty/Edge State Handling

- [ ] CHK028: Dashboard pages render without JavaScript errors when the DuckDB warehouse contains tables but no rows
- [ ] CHK029: Charts display gracefully (empty chart or "no data" state) when source tables have zero rows
- [ ] CHK030: Pages that depend on `check_runs` / `check_results` render when those tables exist but are empty
- [ ] CHK031: Pages that depend on `transform_runs` render when that table exists but is empty

## Metadata & Lineage Visibility (DAMA-DMBOK Area 10)

- [ ] CHK032: Ingestion health page displays operational metadata: run_id, started_at, status, elapsed_seconds
- [ ] CHK033: Data quality page displays check metadata: check_name, severity, description, violation_count
- [ ] CHK034: Overview page shows pipeline lineage by presenting ingestion -> transform -> check run sequence chronologically
- [ ] CHK035: All run history tables include timestamp columns allowing temporal analysis

## Notes

- Check items off as completed: `[x]`
- Accuracy checks (CHK001-CHK009) require comparing dashboard visual output against direct DuckDB SQL queries
- Read-only checks (CHK020-CHK023) are critical for FR-009 compliance
- Edge state checks (CHK028-CHK031) verify graceful degradation per spec edge cases
