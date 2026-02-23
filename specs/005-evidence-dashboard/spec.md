# Feature Specification: Evidence.dev Dashboard for DuckDB Analytical Warehouse

**Feature Branch**: `005-evidence-dashboard`  
**Created**: 2026-02-23  
**Status**: Draft  
**Input**: User description: "Evidence.dev dashboard for the DuckDB analytical warehouse. Build an Evidence.dev project that connects to the existing DuckDB warehouse (data/warehouse/transactions.duckdb) and provides interactive dashboards for: (1) ingestion pipeline health — records loaded, quarantined, duplicates skipped per run, (2) data quality check results — pass/fail/warn status per check over time, (3) financial analytics — daily spend by category trends, monthly account summaries, top merchants, currency breakdowns, (4) pipeline metadata overview — ingestion runs, transform runs, and check runs history. The dashboard should be a standalone Evidence.dev project in a `dashboard/` directory at the project root, reading directly from the DuckDB warehouse file. It should include pages for each analytics domain, use Evidence's built-in chart components, and be runnable via `npm run dev` from the dashboard directory."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Financial Analytics Dashboard (Priority: P1)

As a data analyst, I want to view financial analytics (daily spend by category, monthly account summaries, top merchants, currency breakdowns) in an interactive dashboard so I can explore spending patterns without writing SQL.

**Why this priority**: Financial analytics is the primary business value of the warehouse. The mart tables (`daily_spend_by_category`, `monthly_account_summary`) and the raw `transactions` table already exist, making this the most immediately valuable dashboard.

**Independent Test**: Can be fully tested by starting the dev server and navigating to the financial analytics page. Verified by seeing charts for daily spend trends, monthly account summaries, top merchants by spend, and currency distribution — all populated from actual DuckDB data.

**Acceptance Scenarios**:

1. **Given** the DuckDB warehouse has transaction data, **When** I navigate to the financial analytics page, **Then** I see a line/bar chart showing daily spend by category over time.
2. **Given** the DuckDB warehouse has transaction data, **When** I view the monthly account summary section, **Then** I see a table and/or chart showing debits, credits, and net flow per account per month.
3. **Given** the DuckDB warehouse has transaction data, **When** I view the top merchants section, **Then** I see a ranked list or bar chart of merchants by total spend.
4. **Given** the DuckDB warehouse has transaction data, **When** I view the currency breakdown section, **Then** I see a chart showing transaction volume and amounts by currency.

---

### User Story 2 - Ingestion Pipeline Health Dashboard (Priority: P2)

As a data engineer, I want to monitor ingestion pipeline health (records loaded, quarantined, duplicates skipped per run) in a dashboard so I can quickly identify ingestion problems.

**Why this priority**: Operational monitoring is the second most important use case. The `ingestion_runs` and `quarantine` tables already capture all needed metadata.

**Independent Test**: Can be fully tested by navigating to the ingestion health page after running the ingestion pipeline at least once. Verified by seeing run-level metrics (records loaded, quarantined, duplicates skipped), status indicators, and trend charts over time.

**Acceptance Scenarios**:

1. **Given** at least one ingestion run exists, **When** I navigate to the ingestion health page, **Then** I see a summary table of ingestion runs with records_loaded, records_quarantined, duplicates_skipped, status, and elapsed_seconds.
2. **Given** multiple ingestion runs exist, **When** I view the trend section, **Then** I see a chart showing records loaded vs quarantined vs duplicates skipped over time.
3. **Given** quarantine records exist, **When** I view the quarantine detail section, **Then** I see a breakdown of rejection reasons with counts.

---

### User Story 3 - Data Quality Check Results Dashboard (Priority: P3)

As a data engineer, I want to view data quality check results (pass/fail/warn status per check over time) in a dashboard so I can track data quality trends and identify recurring issues.

**Why this priority**: Data quality monitoring builds on ingestion health and provides deeper insight. The `check_runs` and `check_results` tables from Feature 004 contain all needed data.

**Independent Test**: Can be fully tested by navigating to the data quality page after running the check framework at least once. Verified by seeing per-check status over time and aggregate pass/fail/warn counts.

**Acceptance Scenarios**:

1. **Given** at least one check run exists, **When** I navigate to the data quality page, **Then** I see a summary showing total checks passed, failed, and errored for the most recent run.
2. **Given** multiple check runs exist, **When** I view the trends section, **Then** I see a chart showing check outcomes (pass/fail/error) over time per check name.
3. **Given** check results with violations exist, **When** I view a specific check detail, **Then** I see violation counts and severity levels.

---

### User Story 4 - Pipeline Metadata Overview (Priority: P4)

As a data engineer, I want a unified overview of all pipeline run history (ingestion runs, transform runs, check runs) so I can see the end-to-end pipeline health at a glance.

**Why this priority**: This is a convenience/overview page that aggregates what the other pages show individually. Lower priority because the individual pages already provide this data.

**Independent Test**: Can be fully tested by navigating to the overview/home page. Verified by seeing summary cards or tables for the latest ingestion run, latest transform run, and latest check run, plus a combined timeline.

**Acceptance Scenarios**:

1. **Given** pipeline runs exist, **When** I navigate to the home/overview page, **Then** I see summary cards showing the latest status for ingestion, transform, and check runs.
2. **Given** pipeline runs exist, **When** I view the timeline section, **Then** I see a combined chronological view of all run types with their statuses.

---

### Edge Cases

- What happens when the DuckDB warehouse file does not exist or is empty? Dashboard should display a meaningful empty state, not crash.
- What happens when some tables (e.g., `check_runs`) exist but have no rows? Charts should render with no data points rather than throwing errors.
- What happens when the dashboard is started but the DuckDB file is locked by another process? Evidence.dev connects in read-only mode to avoid write conflicts.
- What happens when the relative path to the DuckDB file changes? The connection configuration should use a relative path from the dashboard directory (`../data/warehouse/transactions.duckdb`).

## Clarifications

**Q1: Evidence.dev version and installation**
Use the latest stable Evidence.dev release. Initialize with the standard template (`npx degit evidence-dev/template`). The `dashboard/node_modules/` directory will be gitignored.

**Q2: Build mode**
Dev mode only (`npm run dev`). No static build support required.

**Q3: Chart type selection**
Agent picks the most appropriate Evidence.dev chart component per metric (line, bar, area, table, BigValue, etc.) based on the data shape.

**Q4: Date range filtering**
No date range filters or time period selectors. Show all available data.

**Q5: Navigation and layout**
Use Evidence.dev's default sidebar navigation. Overview page is the landing page at `/`. Page ordering in sidebar: Overview (home), Financial Analytics, Ingestion Health, Data Quality.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The dashboard MUST be a standalone Evidence.dev project in a `dashboard/` directory at the project root.
- **FR-002**: The dashboard MUST connect to the existing DuckDB warehouse at `data/warehouse/transactions.duckdb` using a relative path.
- **FR-003**: The dashboard MUST be startable via `npm run dev` from the `dashboard/` directory.
- **FR-004**: The dashboard MUST include a financial analytics page with daily spend by category charts, monthly account summaries, top merchants, and currency breakdowns.
- **FR-005**: The dashboard MUST include an ingestion health page showing per-run metrics from `ingestion_runs` and quarantine breakdowns.
- **FR-006**: The dashboard MUST include a data quality page showing check results from `check_runs` and `check_results`.
- **FR-007**: The dashboard MUST include a home/overview page with summary cards for all pipeline stages.
- **FR-008**: The dashboard MUST use Evidence.dev built-in chart components (BarChart, LineChart, DataTable, BigValue, etc.).
- **FR-009**: The dashboard MUST connect to DuckDB in read-only mode to avoid locking conflicts with pipeline processes.
- **FR-010**: All SQL queries in the dashboard MUST read from existing tables/views only (`transactions`, `quarantine`, `ingestion_runs`, `daily_spend_by_category`, `monthly_account_summary`, `transform_runs`, `check_runs`, `check_results`).

### Key Entities *(include if feature involves data)*

- **DuckDB Warehouse**: The existing analytical database at `data/warehouse/transactions.duckdb` containing all source tables. Dashboard reads from it without modification.
- **Evidence.dev Project**: A Node.js-based dashboard framework that generates interactive reports from SQL queries. Configured via `evidence.plugins.yaml` for DuckDB connectivity.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Running `npm run dev` from `dashboard/` starts the Evidence dev server and displays the home page within 30 seconds.
- **SC-002**: All four dashboard pages (overview, ingestion health, data quality, financial analytics) render without errors when the DuckDB warehouse contains data.
- **SC-003**: Charts and tables on each page display data that matches the underlying DuckDB tables when queried directly.
- **SC-004**: The dashboard runs in read-only mode — no writes to the DuckDB warehouse occur during dashboard operation.
