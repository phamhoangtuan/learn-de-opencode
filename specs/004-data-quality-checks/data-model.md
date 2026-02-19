# Data Model: Data Quality Checks

**Feature**: 004-data-quality-checks
**Date**: 2026-02-19

## New DuckDB Tables

### check_runs

One row per check-runner invocation. Tracks aggregate outcomes.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| run_id | VARCHAR | PRIMARY KEY | UUID for the check run |
| started_at | TIMESTAMPTZ | NOT NULL | When the run started |
| completed_at | TIMESTAMPTZ | | When the run finished |
| status | VARCHAR | NOT NULL | Overall status: passed, warn, failed, error |
| total_checks | INTEGER | NOT NULL | Number of checks discovered |
| checks_passed | INTEGER | NOT NULL | Number of checks that passed |
| checks_failed | INTEGER | NOT NULL | Number of checks that failed |
| checks_errored | INTEGER | NOT NULL | Number of checks that errored |
| elapsed_seconds | DOUBLE | NOT NULL | Total wall-clock time |
| error_message | VARCHAR | | Top-level error if run couldn't complete |

### check_results

One row per check per invocation. Tracks individual check outcomes.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | VARCHAR | PRIMARY KEY | UUID for this result row |
| run_id | VARCHAR | NOT NULL | FK to check_runs.run_id |
| check_name | VARCHAR | NOT NULL | Name from `-- check:` header or filename |
| severity | VARCHAR | NOT NULL | `critical` or `warn` |
| description | VARCHAR | | From `-- description:` header |
| status | VARCHAR | NOT NULL | `pass`, `fail`, `error` |
| violation_count | INTEGER | | Number of violating rows (NULL if error) |
| sample_violations | VARCHAR | | JSON string of up to 5 violating rows |
| elapsed_seconds | DOUBLE | NOT NULL | Time to execute this check |
| error_message | VARCHAR | | Error details if status = error |

## Python Domain Models

### CheckSeverity (Enum)

```
CRITICAL = "critical"
WARN = "warn"
```

### CheckStatus (Enum)

```
PASS = "pass"
FAIL = "fail"
ERROR = "error"
```

### RunStatus (Enum)

```
PASSED = "passed"
WARN = "warn"
FAILED = "failed"
ERROR = "error"
```

### CheckModel (dataclass)

Parsed from a `.sql` file in `src/checks/`.

| Field | Type | Description |
|-------|------|-------------|
| name | str | Check name from header or filename |
| file_path | Path | Path to the SQL file |
| sql | str | The SQL SELECT statement |
| severity | CheckSeverity | critical or warn |
| description | str | Human-readable description |

### CheckResult (dataclass)

Result of executing one check.

| Field | Type | Description |
|-------|------|-------------|
| name | str | Check name |
| severity | CheckSeverity | critical or warn |
| status | CheckStatus | pass, fail, or error |
| violation_count | int | Number of violating rows |
| sample_violations | list[dict] | Up to 5 violating rows as dicts |
| elapsed_seconds | float | Execution time |
| error | str or None | Error message if status = error |

### CheckRunResult (dataclass)

Aggregate result for one check-runner invocation.

| Field | Type | Description |
|-------|------|-------------|
| run_id | str | UUID |
| status | RunStatus | passed, warn, failed, error |
| total_checks | int | Checks discovered |
| checks_passed | int | Checks that passed |
| checks_failed | int | Checks that failed |
| checks_errored | int | Checks that errored |
| elapsed_seconds | float | Total time |
| check_results | list[CheckResult] | Per-check details |
| error_message | str or None | Top-level error |

## Check SQL File Format

```sql
-- check: <check_name>
-- severity: critical|warn
-- description: <human-readable description>
SELECT <violating_rows>
FROM <table>
WHERE <violation_condition>;
```

File naming: `check__<name>.sql`

## Pre-Built Check Files

| File | Check Name | Severity | SQL Logic |
|------|-----------|----------|-----------|
| `check__row_count_staging.sql` | row_count_staging | critical | SELECT 1 WHERE (SELECT COUNT(*) FROM stg_transactions) != (SELECT COUNT(*) FROM transactions) |
| `check__mart_not_empty.sql` | mart_not_empty | critical | SELECT table_name FROM (...) WHERE row_count = 0 |
| `check__freshness.sql` | freshness | warn | SELECT ... WHERE latest ingested_at > 48h ago |
| `check__unique_daily_spend_grain.sql` | unique_daily_spend_grain | critical | SELECT ... GROUP BY grain HAVING COUNT(*) > 1 |
| `check__unique_monthly_summary_grain.sql` | unique_monthly_summary_grain | critical | SELECT ... GROUP BY grain HAVING COUNT(*) > 1 |
| `check__accepted_values_currency.sql` | accepted_values_currency | warn | SELECT ... WHERE currency NOT IN ('USD','EUR','GBP','JPY') |

## Relationship to Existing Tables

```
Existing (Features 001-003):
  transactions          (source, Feature 002)
  quarantine            (Feature 002)
  ingestion_runs        (Feature 002)
  stg_transactions      (VIEW, Feature 003)
  daily_spend_by_category   (TABLE, Feature 003)
  monthly_account_summary   (TABLE, Feature 003)
  transform_runs        (Feature 003)

New (Feature 004):
  check_runs            (metadata)
  check_results         (metadata)
```
