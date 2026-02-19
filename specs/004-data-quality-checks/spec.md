# Feature Specification: Data Quality Checks

**Feature Branch**: `004-data-quality-checks`
**Created**: 2026-02-19
**Status**: Clarified
**Input**: User description: "Data quality / observability layer -- automated data quality checks that run after transforms (row count assertions, freshness checks, anomaly detection). Think Great Expectations lite or dbt-style tests embedded in SQL."

## Clarifications

*2026-02-19 clarification session:*

**Q1: Should checks integrate into the transform runner or be a separate CLI?**
A: Separate CLI only (`run_checks.py`). Decoupled from the transform runner. User runs checks explicitly after transforms.

**Q2: What should the freshness staleness threshold be?**
A: Hardcoded 48 hours in the SQL check file. No CLI or header-based configuration for simplicity.

**Q3: How should check results be stored?**
A: Two DuckDB metadata tables: `check_runs` (per-run aggregate) and `check_results` (per-check detail). Same pattern as `transform_runs`.

**Q4: What naming convention for check SQL files?**
A: `check__<name>.sql` (e.g., `check__row_count_staging.sql`). Consistent with `staging__xxx.sql` and `mart__xxx.sql`.

**Q5: How many sample violation rows to capture?**
A: Up to 5 rows per failed check. Stored as text in `check_results.sample_violations`.

## User Scenarios & Testing

### User Story 1 - Run SQL-Based Quality Checks After Transforms (Priority: P1)

As a data engineer, I want to define quality checks as SQL files that run automatically after transforms complete, so that I can catch data issues before downstream consumers use bad data.

**Why this priority**: Without a mechanism to run checks, nothing else in this feature works. This is the core loop: discover check SQL files, execute them, report pass/fail.

**Independent Test**: Can be fully tested by creating a simple check SQL file (e.g., assert row count > 0), running the check runner, and verifying it reports pass/fail with details.

**Acceptance Scenarios**:

1. **Given** a DuckDB warehouse with completed transforms and a `checks/` directory containing valid check SQL files, **When** I run the check runner, **Then** each check SQL is executed and results are reported as pass or fail.
2. **Given** a check SQL that returns zero rows, **When** the check runner executes it, **Then** the check is marked as "pass" (no violations found).
3. **Given** a check SQL that returns one or more rows, **When** the check runner executes it, **Then** the check is marked as "fail" and the violating rows are captured.

---

### User Story 2 - Check Metadata and Reporting (Priority: P2)

As a data engineer, I want each check to have metadata (name, severity, description) declared via SQL comment headers, so that I can understand what failed and how critical it is.

**Why this priority**: Bare pass/fail is not useful without knowing what was checked and how urgent the failure is. Metadata makes the system actionable.

**Independent Test**: Can be tested by creating check files with `-- check:`, `-- severity:`, and `-- description:` headers, running checks, and verifying the report includes this metadata.

**Acceptance Scenarios**:

1. **Given** a check SQL file with `-- check: no_null_transaction_ids`, `-- severity: critical`, and `-- description: ...` headers, **When** the check runner parses it, **Then** the check name, severity, and description are extracted.
2. **Given** a check with severity "critical" that fails, **When** the run completes, **Then** the overall run status is "failed".
3. **Given** only "warn"-severity checks that fail, **When** the run completes, **Then** the overall run status is "warn" (not "failed").

---

### User Story 3 - Row Count Assertions (Priority: P3)

As a data engineer, I want pre-built check SQL files that verify staging row counts match source and mart tables are non-empty, so that I get baseline data completeness monitoring out of the box.

**Why this priority**: Row count checks are the simplest, most universal quality check. They catch catastrophic failures (empty tables, dropped rows) immediately.

**Independent Test**: Can be tested by running transforms on a known dataset, then running the row-count checks and verifying they pass. Can also test with an empty mart table to verify failure detection.

**Acceptance Scenarios**:

1. **Given** a warehouse where `stg_transactions` has the same row count as `transactions`, **When** the staging row count check runs, **Then** it passes.
2. **Given** a warehouse where `daily_spend_by_category` has zero rows, **When** the mart non-empty check runs, **Then** it fails with the violating condition.

---

### User Story 4 - Freshness Checks (Priority: P4)

As a data engineer, I want a check that verifies the latest data in transform output tables is not stale (e.g., most recent `transaction_date` or `ingested_at` is within an acceptable window), so that I can detect pipeline staleness.

**Why this priority**: Freshness is a key quality dimension (Timeliness in DAMA-DMBOK 6Cs). After completeness, timeliness is the next most impactful check.

**Independent Test**: Can be tested by running transforms on a dataset with known dates, then running the freshness check with a configurable threshold and verifying pass/fail.

**Acceptance Scenarios**:

1. **Given** a warehouse where the latest `ingested_at` in `stg_transactions` is within 48 hours, **When** the freshness check runs with a 48-hour threshold, **Then** it passes.
2. **Given** a warehouse where the latest `ingested_at` is older than the threshold, **When** the freshness check runs, **Then** it fails and reports the age of the stale data.

---

### User Story 5 - Uniqueness and Referential Integrity Checks (Priority: P5)

As a data engineer, I want checks that verify primary key uniqueness in mart tables and referential integrity between staging and mart layers, so that I can detect grain violations and orphaned records.

**Why this priority**: Uniqueness and referential integrity are fundamental quality dimensions. They catch subtle bugs like duplicate aggregation or join fanouts.

**Independent Test**: Can be tested by inserting duplicate grain rows into a mart table and verifying the uniqueness check catches them.

**Acceptance Scenarios**:

1. **Given** `daily_spend_by_category` with unique `(transaction_date, category, currency)` grains, **When** the uniqueness check runs, **Then** it passes.
2. **Given** `monthly_account_summary` with a duplicated `(month, account_id, currency)` grain, **When** the uniqueness check runs, **Then** it fails and reports the duplicate rows.

---

### User Story 6 - CLI Entrypoint (Priority: P6)

As a data engineer, I want a CLI command `uv run src/run_checks.py` that runs all checks and prints a summary, so that I can integrate quality checks into my workflow.

**Why this priority**: CLI is the delivery mechanism. Without it the checks exist but are not user-accessible. Lower priority because it's a thin wrapper.

**Independent Test**: Can be tested by running the CLI and verifying it exits with code 0 on all-pass, code 1 on any critical failure, and prints a human-readable summary.

**Acceptance Scenarios**:

1. **Given** all checks pass, **When** I run `uv run src/run_checks.py`, **Then** it prints a summary and exits with code 0.
2. **Given** a critical check fails, **When** I run `uv run src/run_checks.py`, **Then** it prints the failure details and exits with code 1.
3. **Given** only warn-severity checks fail, **When** I run `uv run src/run_checks.py`, **Then** it prints warnings and exits with code 0.

---

### Edge Cases

- What happens when the checks directory is empty? Runner completes with 0 checks, status "completed", exit code 0.
- What happens when a check SQL has a syntax error? The check is marked as "error" (distinct from "fail"), the error message is captured, and execution continues to the next check.
- What happens when the database doesn't exist? Runner exits immediately with a clear error message before executing any checks.
- What happens when a check SQL returns non-tabular results (e.g., DDL)? The check should be treated as an error with a descriptive message.
- What happens when the transforms haven't been run yet (mart tables don't exist)? Checks that reference missing tables should fail with clear error messages rather than crashing the entire run.

## Requirements

### Functional Requirements

- **FR-001**: System MUST discover `.sql` files from a configurable checks directory (default: `src/checks/`).
- **FR-002**: System MUST parse `-- check:`, `-- severity:`, and `-- description:` comment headers from each check SQL file.
- **FR-003**: Check names MUST be derived from the `-- check:` header if present, otherwise from the filename (stripping `.sql`).
- **FR-004**: Severity levels MUST be one of: `critical`, `warn`. Default is `warn` if not specified.
- **FR-005**: A check SQL MUST be considered "pass" if it returns zero rows and "fail" if it returns one or more rows (violation-based: rows = bad records).
- **FR-006**: System MUST capture the violating rows (up to a configurable limit, default 5) for failed checks.
- **FR-007**: System MUST continue executing remaining checks even if one fails or errors.
- **FR-008**: System MUST record each check run in a `check_runs` metadata table in DuckDB with run_id, timestamp, total checks, passed, failed, errored, and overall status.
- **FR-009**: System MUST record per-check results in a `check_results` metadata table with check name, severity, status (pass/fail/error), violation count, sample violations, elapsed time, and error message.
- **FR-010**: Overall run status MUST be "failed" if any `critical` check fails, "warn" if only `warn` checks fail, and "passed" if all checks pass.
- **FR-011**: CLI entrypoint MUST exit with code 1 if overall status is "failed", code 0 otherwise.
- **FR-012**: CLI MUST print a human-readable summary of all check results.
- **FR-013**: System MUST provide at least 6 pre-built check SQL files covering row counts, freshness, uniqueness, and referential integrity.
- **FR-014**: All check SQL files MUST use `SELECT` statements only (read-only, no mutations).
- **FR-015**: System MUST validate that the DuckDB database exists before running checks.
- **FR-016**: Check execution MUST be idempotent -- running checks twice produces the same pass/fail results (metadata rows are additive).

### Key Entities

- **CheckModel**: A parsed check SQL file with name, severity, description, and SQL content.
- **CheckResult**: The outcome of a single check execution: pass/fail/error, violation count, sample rows, elapsed time.
- **CheckRunResult**: Aggregate run result with totals and overall status.
- **check_runs table**: DuckDB metadata table tracking each check run.
- **check_results table**: DuckDB metadata table tracking per-check outcomes.

## Success Criteria

### Measurable Outcomes

- **SC-001**: At least 6 pre-built check SQL files are included and pass against a healthy warehouse.
- **SC-002**: A failing critical check causes exit code 1; a failing warn check produces exit code 0.
- **SC-003**: Check runner completes in under 5 seconds for the current 3-model warehouse.
- **SC-004**: Check metadata (name, severity, description) is correctly extracted from SQL headers.
- **SC-005**: Violating rows are captured (up to limit) in the check_results metadata table.
- **SC-006**: Running checks twice produces identical pass/fail outcomes (idempotency).
- **SC-007**: All tests pass with `uv run python -m pytest` and `ruff check .` has zero errors.
- **SC-008**: Check runner gracefully handles missing tables, SQL errors, and empty check directories without crashing.

## Assumptions

1. The DuckDB warehouse at `data/warehouse/transactions.duckdb` has been populated by Features 001-002 and transformed by Feature 003.
2. Check SQL files follow the violation-based convention: returned rows = violations (zero rows = pass).
3. Checks are read-only and never modify the warehouse data.
4. The `src/checks/` directory is the default location for check SQL files.
5. Freshness thresholds are hardcoded in the SQL (not configurable via CLI) for simplicity.
6. This feature does not integrate with the transform runner -- checks are a separate CLI command run after transforms.
