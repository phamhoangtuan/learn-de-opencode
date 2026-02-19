# Feature Specification: SQL Transformations Layer

**Feature Branch**: `003-sql-transformations`  
**Created**: 2026-02-19  
**Status**: Draft  
**Input**: User description: "Build a lightweight dbt-inspired transformation layer that runs idempotent SQL transforms against the existing DuckDB warehouse. Create a staging view (stg_transactions) that standardizes the raw transactions table, and mart aggregate tables (daily_spend_by_category, monthly_account_summary). Include a Python runner that executes .sql files in dependency order, a transform_runs metadata table tracking each execution, and tests validating output tables. Transformations should be pure SQL using CREATE OR REPLACE patterns for full-refresh idempotency. This is Feature 003 building on top of Feature 001 (synthetic data generator) and Feature 002 (DuckDB ingestion pipeline)."

## Clarifications

### Session 2026-02-19

- Q: Should SQL files be organized into subdirectories by layer (staging/, mart/) or flat in a single directory with naming conventions? → A: Flat directory with naming convention `<layer>__<model_name>.sql`. Simpler filesystem layout; the layer prefix in the filename provides sufficient organization for a lightweight tool. Subdirectories can be a future enhancement.
- Q: Should `stg_transactions` be a VIEW (computed on-the-fly) or a TABLE (materialized)? → A: VIEW via `CREATE OR REPLACE VIEW`. Staging views should always reflect the latest raw data without requiring explicit re-materialization. Mart tables use `CREATE OR REPLACE TABLE AS` since they perform heavy aggregation.
- Q: Should transform_runs track per-model execution details, or only run-level metadata? → A: Run-level only for v1. A single row in `transform_runs` per execution with aggregate model counts. Per-model tracking (model name, duration, status per model) is deferred to a future enhancement.
- Q: Should the runner support selective model execution (e.g., `--select model_name`)? → A: Out of scope for v1. The runner always executes all transforms in the directory. Selective execution is a natural future enhancement.
- Q: Should mart tables group by currency when aggregating amounts, or convert to a base currency? → A: Group by currency. The `daily_spend_by_category` mart includes `currency` as a grouping dimension alongside date and category. The `monthly_account_summary` also groups by currency. Currency conversion is a separate concern and out of scope.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Run SQL Transformations with a Single Command (Priority: P1)

As a data engineer, I want to run a single command that executes all SQL transformation files against the DuckDB warehouse in the correct dependency order, so that I can build analytics-ready views and tables from raw ingested data without manual SQL execution.

**Why this priority**: This is the core value proposition. Without a runner that resolves and executes transforms in order, the entire transformation layer has no surface to operate on. This story alone delivers an MVP that turns raw warehouse data into structured analytics outputs.

**Independent Test**: Can be fully tested by running the ingestion pipeline (Feature 002) to populate the warehouse, then running the transform command and verifying that all output views and tables exist and contain data.

**Acceptance Scenarios**:

1. **Given** the DuckDB warehouse contains ingested transaction data, **When** the user runs the transform command, **Then** all SQL transformation files are executed in dependency order and output views/tables are created or replaced.
2. **Given** the transform command has already been run, **When** the user runs it again with no changes, **Then** all transforms execute successfully and produce identical output (full-refresh idempotency via CREATE OR REPLACE).
3. **Given** a SQL transform file has a syntax error, **When** the transform command runs, **Then** the runner reports the error with the filename and line number, skips that transform, and continues executing independent transforms.
4. **Given** the DuckDB warehouse does not exist or is empty, **When** the user runs the transform command, **Then** the system reports that no source data is available and exits gracefully.

---

### User Story 2 - Staging View Standardizes Raw Data (Priority: P2)

As a data engineer, I want a staging view (`stg_transactions`) that standardizes and renames columns from the raw `transactions` table, so that downstream transforms operate on a clean, consistent interface decoupled from the raw ingestion schema.

**Why this priority**: The staging layer is the foundation for all downstream mart tables. It provides a stable contract that insulates analytics from changes in the raw ingestion schema. Without it, mart tables would couple directly to raw table structures.

**Independent Test**: Can be tested by querying `stg_transactions` after running transforms and verifying column names, data types, and row counts match the source `transactions` table with expected standardizations applied.

**Acceptance Scenarios**:

1. **Given** the `transactions` table contains ingested records, **When** transforms are executed, **Then** a `stg_transactions` view exists with standardized column names and types.
2. **Given** `stg_transactions` is queried, **When** results are inspected, **Then** the row count matches the source `transactions` table exactly (no row loss or gain in staging).
3. **Given** the raw `transactions` table has `amount` values in mixed currencies, **When** `stg_transactions` is queried, **Then** amounts are presented alongside their currency without alteration (staging does not perform currency conversion).
4. **Given** the raw `transactions` table has `timestamp` as TIMESTAMPTZ, **When** `stg_transactions` is queried, **Then** the timestamp is available both as the original TIMESTAMPTZ and as a derived DATE column.

---

### User Story 3 - Daily Spend by Category Mart (Priority: P3)

As a data analyst, I want a `daily_spend_by_category` table that aggregates transaction amounts by date and spending category, so that I can quickly analyze daily spending trends without writing aggregation queries from scratch.

**Why this priority**: This is the first concrete analytics output that demonstrates the value of the transformation layer. It serves as a reference implementation for the mart pattern and is immediately useful for spend analysis.

**Independent Test**: Can be tested by verifying that the table exists after transforms, that the sum of amounts per category per day matches a manual aggregation of `stg_transactions`, and that the table includes only completed debit transactions.

**Acceptance Scenarios**:

1. **Given** `stg_transactions` contains completed debit transactions across multiple dates and categories, **When** transforms are executed, **Then** `daily_spend_by_category` contains one row per (date, category, currency) combination.
2. **Given** `daily_spend_by_category` is queried, **When** results are compared to a manual aggregation of `stg_transactions`, **Then** the `total_amount`, `transaction_count`, and `avg_amount` columns match exactly.
3. **Given** the transforms are run twice, **When** `daily_spend_by_category` is queried after the second run, **Then** the results are identical to the first run (no duplicates from re-execution).

---

### User Story 4 - Monthly Account Summary Mart (Priority: P4)

As a data analyst, I want a `monthly_account_summary` table that aggregates account-level metrics by month (total debits, total credits, net flow, transaction count), so that I can analyze account activity trends over time.

**Why this priority**: This is the second mart table, demonstrating the generalizability of the transformation pattern. It provides account-level insights that complement the category-level spend analysis.

**Independent Test**: Can be tested by verifying that the table exists after transforms, that monthly aggregation per account matches a manual computation from `stg_transactions`, and that debit/credit breakdowns are correct.

**Acceptance Scenarios**:

1. **Given** `stg_transactions` contains transactions for multiple accounts across multiple months, **When** transforms are executed, **Then** `monthly_account_summary` contains one row per (year-month, account_id, currency) combination.
2. **Given** `monthly_account_summary` is queried, **When** results are inspected, **Then** `total_debits`, `total_credits`, `net_flow` (credits minus debits), and `transaction_count` are correct for each account-month.
3. **Given** transforms are run twice, **When** `monthly_account_summary` is queried after the second run, **Then** results are identical (idempotent full-refresh).

---

### User Story 5 - Transform Run Metadata Tracking (Priority: P5)

As a data engineer, I want each transform execution to be recorded in a `transform_runs` metadata table with run ID, start/end timestamps, status, and per-model details, so that I can audit when transforms were run and diagnose failures.

**Why this priority**: Operational visibility is important for production-readiness but does not block the core transformation functionality. It builds on the same metadata-tracking pattern established by Feature 002's `ingestion_runs` table.

**Independent Test**: Can be tested by running transforms and querying `transform_runs` to verify a new row was created with correct timestamps, status, and model count.

**Acceptance Scenarios**:

1. **Given** transforms are executed successfully, **When** `transform_runs` is queried, **Then** a new row exists with status 'completed', accurate start/end timestamps, and the count of models executed.
2. **Given** a transform fails during execution, **When** `transform_runs` is queried, **Then** the row shows status 'failed' with an error description.
3. **Given** transforms have been run multiple times, **When** `transform_runs` is queried, **Then** each run has a unique run ID and runs are ordered chronologically.

---

### Edge Cases

- What happens when a SQL file references a model that does not exist yet (broken dependency)? The runner must detect the unresolved dependency and report a clear error indicating which model is missing, without executing the broken transform.
- What happens when there is a circular dependency between SQL files? The runner must detect the cycle and abort with an error listing the models involved in the cycle.
- What happens when the `transforms/` directory is empty or missing? The system must report that no transforms were found and exit gracefully.
- What happens when a SQL file contains valid SQL but produces an empty result set? The table/view is created normally (empty tables are valid).
- What happens when the DuckDB database is locked by another process? The system must report a clear connection error.
- What happens when a SQL file is not valid UTF-8? The system must report a file-read error for that specific file and skip it.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST read `.sql` files from a configurable transforms directory (defaulting to `src/transforms/`).
- **FR-002**: System MUST parse dependency metadata from SQL file headers using a `-- depends_on:` comment convention (e.g., `-- depends_on: stg_transactions`).
- **FR-003**: System MUST resolve dependencies and execute SQL files in topological order, ensuring that upstream models are created before downstream models that depend on them.
- **FR-004**: System MUST detect circular dependencies and abort with a clear error listing the cycle.
- **FR-005**: System MUST execute each SQL file as a single statement against the DuckDB warehouse connection.
- **FR-006**: All SQL transforms MUST use `CREATE OR REPLACE VIEW` or `CREATE OR REPLACE TABLE ... AS` patterns to ensure full-refresh idempotency.
- **FR-007**: System MUST create a staging view `stg_transactions` that selects from the raw `transactions` table, standardizing column names, casting types as needed, and adding derived columns (e.g., `transaction_date` as DATE from `timestamp`).
- **FR-008**: System MUST create a mart table `daily_spend_by_category` aggregating completed debit transactions by date, category, and currency with columns: `transaction_date`, `category`, `currency`, `total_amount`, `transaction_count`, `avg_amount`.
- **FR-009**: System MUST create a mart table `monthly_account_summary` aggregating transactions by year-month, account, and currency with columns: `month`, `account_id`, `currency`, `total_debits`, `total_credits`, `net_flow`, `transaction_count`.
- **FR-010**: System MUST create and maintain a `transform_runs` metadata table tracking: `run_id`, `started_at`, `completed_at`, `status`, `models_executed`, `models_failed`, `elapsed_seconds`, `error_message`.
- **FR-011**: System MUST record each transform execution in `transform_runs` with a unique run ID.
- **FR-012**: System MUST provide a CLI interface runnable via `uv run` with PEP 723 inline dependency declarations, accepting optional parameters for database path and transforms directory.
- **FR-013**: System MUST produce a run summary at completion: models executed, models failed, elapsed time.
- **FR-014**: System MUST log transform activity (info, warning, error levels) to support operational monitoring.
- **FR-015**: System MUST handle SQL execution errors per-model: log the error, record it in metadata, skip the failed model, and continue executing independent models.
- **FR-016**: System MUST validate that the source `transactions` table exists before attempting any transforms.

### Key Entities

- **Transform Model**: A single `.sql` file representing a transformation. Key attributes: model name (derived from filename), SQL content, dependencies (other model names), output type (view or table).
- **Transform Run**: A single execution of the transform runner. Key attributes: unique run ID, start/end timestamps, execution status, count of models executed/failed, error details.
- **Staging View (stg_transactions)**: A SQL view that standardizes the raw `transactions` table into a clean interface for downstream consumption. Derived columns only; no data aggregation.
- **Mart Table (daily_spend_by_category)**: An aggregate table providing daily spend totals by spending category. Filters to completed debit transactions only.
- **Mart Table (monthly_account_summary)**: An aggregate table providing monthly account-level debit/credit summaries and net flow.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Running the transform command after ingestion produces all expected views and tables (`stg_transactions`, `daily_spend_by_category`, `monthly_account_summary`) with correct data.
- **SC-002**: Running the transform command twice in succession produces identical output tables with zero duplication (full-refresh idempotency verified).
- **SC-003**: The `transform_runs` table accurately records each execution with correct timestamps, model counts, and status.
- **SC-004**: A staging view row count matches the source `transactions` table row count exactly (zero data loss in staging).
- **SC-005**: Mart table aggregations match manual SQL verification queries against `stg_transactions` with zero discrepancy.
- **SC-006**: The transform runner correctly resolves a dependency graph of 3+ models and executes them in valid topological order.
- **SC-007**: A deliberate SQL syntax error in one model does not prevent other independent models from executing.
- **SC-008**: The full transform pipeline completes in under 10 seconds for a warehouse with 100,000 transaction records.

## Assumptions

- The DuckDB warehouse at `data/warehouse/transactions.duckdb` has been populated by Feature 002's ingestion pipeline.
- The raw `transactions` table schema matches the data model defined in Feature 002 (13 columns: 9 source + transaction_date + source_file + ingested_at + run_id).
- SQL files follow a naming convention of `<layer>__<model_name>.sql` (e.g., `staging__stg_transactions.sql`, `mart__daily_spend_by_category.sql`).
- The DuckDB database is single-user (no concurrent write access during transforms).
- Transforms use full-refresh strategy only; incremental transforms are out of scope for this feature.
- The Python runner is a lightweight orchestration tool, not a full dbt replacement. It handles dependency resolution and execution but does not implement features like ref() macros, Jinja templating, or snapshot strategies.
