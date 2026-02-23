# Tasks: Evidence.dev Dashboard

**Feature**: 005-evidence-dashboard
**Date**: 2026-02-23
**Spec**: `specs/005-evidence-dashboard/spec.md`
**Plan**: `specs/005-evidence-dashboard/plan.md`

## Phase 1: Setup -- Project Scaffolding

- [x] T001 Initialize Evidence.dev project in `dashboard/` using `npx degit evidence-dev/template`
- [x] T002 Install `@evidence-dev/duckdb` plugin via npm and configure `dashboard/evidence.plugins.yaml`
- [x] T003 Create `dashboard/.gitignore` to exclude `node_modules/`, `.evidence/`, and build artifacts
- [x] T004 Verify `npm run dev` starts the Evidence dev server with the default template page

## Phase 2: Foundational -- DuckDB Source Connection

**Purpose**: Configure the DuckDB source so all pages can query warehouse data.

- [x] T005 Create `dashboard/sources/warehouse/connection.yaml` with DuckDB path `../../../data/warehouse/transactions.duckdb` (relative to sources/warehouse/)
- [x] T006 [P] Create `dashboard/sources/warehouse/transactions.sql` -- `SELECT * FROM transactions`
- [x] T007 Removed quarantine source (0-row tables produce invalid Parquet in Evidence.dev); quarantine data shown as placeholder on ingestion-health page
- [x] T008 [P] Create `dashboard/sources/warehouse/ingestion_runs.sql` -- `SELECT * FROM ingestion_runs`
- [x] T009 [P] Create `dashboard/sources/warehouse/daily_spend_by_category.sql` -- `SELECT * FROM daily_spend_by_category`
- [x] T010 [P] Create `dashboard/sources/warehouse/monthly_account_summary.sql` -- `SELECT * FROM monthly_account_summary`
- [x] T011 [P] Create `dashboard/sources/warehouse/transform_runs.sql` -- `SELECT * FROM transform_runs`
- [x] T012 [P] Create `dashboard/sources/warehouse/check_runs.sql` -- `SELECT * FROM check_runs`
- [x] T013 [P] Create `dashboard/sources/warehouse/check_results.sql` -- `SELECT * FROM check_results`
- [x] T014 Verify source extraction works by running `npm run sources` and confirming no errors

**Checkpoint**: DuckDB source connected and all 8 tables extracted. All pages can now query `warehouse.*` tables.

---

## Phase 3: User Story 1 -- Financial Analytics Dashboard (Priority: P1)

**Goal**: Interactive financial analytics page with daily spend by category, monthly account summaries, top merchants, and currency breakdowns.

**Independent Test**: Start dev server, navigate to `/financial-analytics`, verify all four chart sections render with DuckDB data.

### Implementation for User Story 1

- [x] T015 [US1] Create `dashboard/pages/financial-analytics.md` with page title and section headers
- [x] T016 [US1] Add Daily Spend by Category section: inline SQL query from `daily_spend_by_category`, stacked BarChart (x=transaction_date, y=total_amount, series=category)
- [x] T017 [US1] Add Monthly Account Summary section: inline SQL query from `monthly_account_summary`, DataTable with month, account_id, currency, total_debits, total_credits, net_flow, transaction_count
- [x] T018 [US1] Add Top Merchants by Spend section: inline SQL aggregating top 10 merchants by total amount from `transactions`, horizontal BarChart
- [x] T019 [US1] Add Currency Breakdown section: inline SQL aggregating transaction count and total amount by currency from `transactions`, BarChart

**Checkpoint**: Financial analytics page fully functional with all four visualization sections.

---

## Phase 4: User Story 2 -- Ingestion Pipeline Health Dashboard (Priority: P2)

**Goal**: Monitor ingestion pipeline health with per-run metrics, trends, and quarantine breakdown.

**Independent Test**: Start dev server, navigate to `/ingestion-health`, verify run summary, history table, trend chart, and quarantine breakdown render.

### Implementation for User Story 2

- [x] T020 [US2] Create `dashboard/pages/ingestion-health.md` with page title and section headers
- [x] T021 [US2] Add Latest Run Summary section: inline SQL for most recent ingestion run, BigValue cards for records_loaded, records_quarantined, duplicates_skipped
- [x] T022 [US2] Add Ingestion Runs History section: inline SQL from `ingestion_runs` ordered by started_at desc, DataTable with run_id, started_at, status, records_loaded, records_quarantined, duplicates_skipped, elapsed_seconds
- [x] T023 [US2] Add Records Trend section: inline SQL from `ingestion_runs`, LineChart (x=started_at, y=records_loaded/records_quarantined/duplicates_skipped)
- [x] T024 [US2] Quarantine section replaced with placeholder text (quarantine source removed due to 0-row Parquet limitation)

**Checkpoint**: Ingestion health page fully functional with all four sections.

---

## Phase 5: User Story 3 -- Data Quality Check Results Dashboard (Priority: P3)

**Goal**: Track data quality check results with latest run summary, per-check status over time, and violation details.

**Independent Test**: Start dev server, navigate to `/data-quality`, verify latest run summary, outcomes over time chart, and check results table render.

### Implementation for User Story 3

- [x] T025 [US3] Create `dashboard/pages/data-quality.md` with page title and section headers
- [x] T026 [US3] Add Latest Run Summary section: inline SQL for most recent check run, BigValue cards for checks_passed, checks_failed, checks_errored
- [x] T027 [US3] Add Check Results from Latest Run section: inline SQL joining `check_results` with latest `check_runs`, DataTable with check_name, severity, status, violation_count, description
- [x] T028 [US3] Add Check Outcomes Over Time section: inline SQL from `check_runs`, stacked BarChart (x=started_at, y=checks_passed/checks_failed/checks_errored)
- [x] T029 [US3] Add Check Results Detail section: inline SQL from `check_results`, DataTable with run_id, check_name, severity, status, violation_count

**Checkpoint**: Data quality page fully functional with all four sections.

---

## Phase 6: User Story 4 -- Pipeline Overview Home Page (Priority: P4)

**Goal**: Unified overview page with summary cards and combined pipeline timeline.

**Independent Test**: Start dev server, navigate to `/` (home), verify summary cards for latest ingestion/transform/check runs and combined timeline table render.

### Implementation for User Story 4

- [x] T030 [US4] Create `dashboard/pages/index.md` (replace template default) with page title "Pipeline Overview"
- [x] T031 [US4] Add Summary Cards section: inline SQL queries for latest ingestion run, latest transform run, latest check run; BigValue cards for each showing status and key metric
- [x] T032 [US4] Add Pipeline Timeline section: inline SQL combining ingestion_runs, transform_runs, check_runs into a unified chronological view; DataTable sorted by started_at desc with run_type, started_at, status, key_metric columns

**Checkpoint**: Overview page fully functional. All four dashboard pages complete.

---

## Phase 7: Polish & Cross-Cutting

- [x] T033 Remove any template default pages/content that are not part of the dashboard spec
- [x] T034 Verify sidebar navigation ordering: Overview (home), Financial Analytics, Ingestion Health, Data Quality
- [x] T035 Verify all pages handle empty data gracefully (quarantine source removed; all other sources have data; build succeeds)
- [x] T036 Verify DuckDB connection is read-only (connection.yaml uses default DuckDB read behavior; no .wal file created)
- [x] T037 Update `README.md` with Feature 005 dashboard section (location, startup command, pages)
- [x] T038 Update `AGENTS.md` with Feature 005 technology and commands

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies -- start immediately
- **Phase 2 (Foundational)**: Depends on Phase 1 -- DuckDB source must be connected before any page work
- **Phases 3-6 (User Stories)**: All depend on Phase 2 completion; can then proceed in priority order (P1 -> P2 -> P3 -> P4) or in parallel since each page is an independent file
- **Phase 7 (Polish)**: Depends on all user story phases being complete

### Parallel Opportunities

- T006-T013: All source extraction SQL files can be created in parallel (different files, no dependencies)
- Phases 3-6: Each page is an independent markdown file; user stories can be implemented in parallel
- Within each user story: Section tasks are sequential (page creation first, then sections added incrementally)
