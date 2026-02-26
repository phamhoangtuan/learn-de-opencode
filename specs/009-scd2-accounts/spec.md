# Feature Specification: SCD Type 2 — Slowly Changing Dimensions for Accounts

**Feature Branch**: `009-scd2-accounts`
**Created**: 2026-02-26
**Status**: Draft
**Input**: User description: "SCD Type 2 (Slowly Changing Dimensions) for Accounts — Build a dim_accounts dimension table that tracks account attribute changes over time using SCD Type 2 with valid_from/valid_to and is_current flag."

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Account Dimension Build (Priority: P1)

As a data analyst, I want a dimension table that summarizes each account's behavioral profile (primary currency, primary spending category, transaction volume, total spend) so that I can join it to fact tables and segment accounts without scanning the full transaction history.

**Why this priority**: Without the dimension table, no downstream analysis or reporting on account attributes is possible. This is the foundational building block.

**Independent Test**: Can be fully tested by running the pipeline on sample transaction data and querying dim_accounts to verify one row per account with correct derived attributes. Delivers immediate analytical value.

**Acceptance Scenarios**:

1. **Given** the transactions table contains records for 5 distinct accounts, **When** the SCD2 dimension build runs, **Then** dim_accounts contains exactly 5 rows, all with `is_current = true`.
2. **Given** an account has transactions in USD (60%) and EUR (40%), **When** the dimension build runs, **Then** `primary_currency` is "USD" (the mode).
3. **Given** an account has no prior dim_accounts record, **When** the dimension build runs, **Then** a new row is inserted with `valid_from` set to the pipeline run timestamp and `valid_to` set to `NULL` (indicating an open/current row).

---

### User Story 2 — Change Detection and History Tracking (Priority: P1)

As a data analyst, I want the dimension to detect when an account's behavioral attributes change between pipeline runs and preserve the full history of changes, so that I can analyze how account behavior evolves over time.

**Why this priority**: SCD Type 2 change tracking is the core value proposition. Without it, the feature is just a snapshot table.

**Independent Test**: Can be tested by running the pipeline twice with different transaction data that causes attribute shifts for an account, then verifying historical rows exist with correct validity windows.

**Acceptance Scenarios**:

1. **Given** an account's `primary_currency` was "USD" in the previous run, **When** new transactions shift it to "EUR" and the pipeline runs again, **Then** the old row is closed (`valid_to` = run_date minus one day, `is_current = false`) and a new row is inserted with `valid_from` = run_date, `valid_to` = NULL, `is_current = true` and `primary_currency = "EUR"`.
2. **Given** an account's attributes have not changed since the last run, **When** the pipeline runs again, **Then** no new row is created and the existing current row remains unchanged.
3. **Given** an account has gone through 3 attribute changes over 3 runs, **When** querying dim_accounts for that account, **Then** exactly 4 rows exist (1 initial + 3 changes) with non-overlapping validity windows.

---

### User Story 3 — Account History Mart (Priority: P2)

As a data analyst, I want a mart view that exposes the complete account change timeline in an analysis-ready format, so that I can easily build reports on account behavior evolution without understanding the SCD2 mechanics.

**Why this priority**: Simplifies downstream consumption but depends on the core dimension being built first.

**Independent Test**: Can be tested by querying the mart after multiple pipeline runs and verifying the timeline is complete, ordered, and includes duration calculations.

**Acceptance Scenarios**:

1. **Given** dim_accounts contains multiple historical rows for an account, **When** querying mart__account_history, **Then** results include all versions ordered by valid_from with a computed `version_duration_days` column.
2. **Given** the current version of an account, **When** querying mart__account_history, **Then** `version_duration_days` reflects days from valid_from to today (not to the sentinel date).

---

### User Story 4 — Dimension Integrity Checks (Priority: P2)

As a pipeline operator, I want automated data quality checks that verify the SCD2 dimension is structurally correct, so that I can trust the data and catch bugs early.

**Why this priority**: Ensures the SCD2 logic is correct. Important but can be added after the core dimension works.

**Independent Test**: Can be tested by running the quality checks after a pipeline execution and verifying all pass on correctly built data, and that intentionally corrupted data triggers failures.

**Acceptance Scenarios**:

1. **Given** a correctly built dim_accounts, **When** integrity checks run, **Then** all checks pass (no overlapping validity ranges, exactly one is_current per account_id, no null surrogate keys).
2. **Given** a dim_accounts with two rows marked `is_current = true` for the same account_id, **When** integrity checks run, **Then** the check fails and reports the violation.

---

## Clarifications

### Session 2026-02-26

- Q: If the dimension build fails mid-execution, should the system roll back all SCD changes or allow partial state? → A: Full rollback — wrap entire SCD merge in a single transaction; on failure, no rows are modified.
- Q: Should the SCD merge logic be implemented as pure SQL, Python, or hybrid? → A: Python module — dedicated `src/dimensions/` module using DuckDB operations directly, consistent with existing pipeline architecture.
- Q: Should historical (non-current) rows in dim_accounts be retained indefinitely or purged? → A: Indefinite retention — all historical rows are kept forever; the history is the analytical product.

---

### Edge Cases

- What happens when an account appears for the first time? A new row is inserted with `valid_from` = run timestamp, `valid_to` = NULL (open row), `is_current = true`.
- What happens when all tracked attributes change simultaneously? A single new version row is created (not one per attribute).
- What happens when the pipeline runs but no transactions were ingested? The dimension build still runs but detects no changes, leaving all existing rows unchanged.
- What happens when the dimension build fails mid-execution? The entire SCD merge is wrapped in a single database transaction; on failure, all changes are rolled back atomically — no partial state is written to dim_accounts.
- What happens when a previously seen account has no new transactions in the current run? Its attributes are still recomputed from the full transaction history; if unchanged, no new version is created.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST maintain a `dim_accounts` table with surrogate key, natural key (`account_id`), tracked attributes, `valid_from`, `valid_to`, and `is_current` columns.
- **FR-002**: System MUST derive account attributes from the full transaction history: `primary_currency` (mode), `primary_category` (mode), `transaction_count`, `total_spend`, `first_seen` (earliest timestamp), `last_seen` (latest timestamp).
- **FR-003**: System MUST detect attribute changes by comparing newly computed attributes against the current dimension row for each account.
- **FR-004**: When attributes change, system MUST close the current row (set `valid_to` to `run_date - 1 day`, `is_current` to false) and insert a new row (with `valid_from` = run_date, `valid_to` = NULL, `is_current` = true). This produces non-overlapping validity windows per account_id.
- **FR-005**: When no attributes change, system MUST leave the existing current row untouched.
- **FR-006**: System MUST assign a unique surrogate key to each dimension row.
- **FR-007**: System MUST provide a `mart__account_history` transform that exposes the full change timeline with computed duration.
- **FR-008**: System MUST provide data quality checks verifying: (a) exactly one `is_current = true` per `account_id`, (b) no overlapping validity ranges per `account_id`, (c) no null surrogate keys.
- **FR-009**: The dimension build MUST execute as a step in the existing pipeline after transforms and before quality checks.
- **FR-010**: System MUST record dimension build metadata (accounts processed, new versions created, unchanged accounts) in the pipeline run tracking.
- **FR-011**: The entire SCD merge operation (row closures + insertions) MUST execute within a single atomic transaction; any failure MUST trigger a full rollback with no partial writes to dim_accounts.
- **FR-012**: The dimension build logic MUST be implemented as a dedicated Python module (separate from the SQL transform runner), keeping change-detection, row closure, and insertion logic independently testable.

### Key Entities

- **dim_accounts**: The SCD Type 2 dimension table. Each row represents a version of an account's behavioral profile. Natural key is `account_id`; surrogate key is a monotonically increasing `BIGINT`. Tracked attributes: `primary_currency`, `primary_category`, `transaction_count`, `total_spend`, `first_seen`, `last_seen`. Contains `run_id` for lineage tracing.
- **mart__account_history**: An analysis-ready VIEW (not a table) over dim_accounts, adding `version_number` and `version_duration_days` computed fields.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: After a pipeline run with N distinct accounts in the transactions table, dim_accounts contains exactly N rows with `is_current = true`.
- **SC-002**: When account attributes change between runs, the system creates a new version within the same pipeline execution — no manual intervention required.
- **SC-003**: All dimension integrity checks pass on every pipeline run (zero violations for overlapping ranges, duplicate current flags, or null surrogate keys).
- **SC-004**: The account history mart returns the complete version timeline for any account, ordered chronologically, with accurate duration calculations.
- **SC-005**: The dimension build step adds less than 5 seconds to total pipeline execution time for datasets up to 100,000 transactions.

- **FR-013**: Historical (non-current) rows in dim_accounts MUST be retained indefinitely; no purge or archival mechanism is required.
- **FR-014**: System MUST document and verify PII handling for `dim_accounts`. `account_id` is a synthetic identifier derived from ingested transaction data and is not considered PII in this system. No masking, encryption, or RBAC controls beyond the existing DuckDB file-system-level access are required. An automated test MUST assert this classification holds (i.e., no new PII columns exist in dim_accounts).

## Assumptions

- Account attributes are derived from the **full** transaction history (not just the current run's transactions), ensuring consistent profiles regardless of run order.
- The `primary_currency` and `primary_category` are determined by mode (most frequent value). In case of a tie, the alphabetically first value is used for deterministic behavior.
- Current rows carry `valid_to = NULL` (open row). The sentinel `'9999-12-31'::TIMESTAMPTZ` appears only as a `COALESCE` default inside the overlap-check SQL (T026) — it is never stored in the table.
- The surrogate key uses a DuckDB sequence for monotonic generation.
- The dimension build runs on every pipeline execution, even if no new data was ingested (to detect attribute shifts from recomputation).

## Dependencies

- Feature 002 (DuckDB Ingestion) — transactions table must exist.
- Feature 003 (SQL Transformations) — DAG-based transform runner for mart__account_history.
- Feature 004 (Data Quality Checks) — check framework for integrity checks.
- Feature 006 (Pipeline Orchestration) — integration into the pipeline DAG.
