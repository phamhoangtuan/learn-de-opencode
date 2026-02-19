# Implementation Plan: Data Quality Checks

**Branch**: `004-data-quality-checks` | **Date**: 2026-02-19 | **Spec**: `specs/004-data-quality-checks/spec.md`

## Summary

Add a SQL-based data quality check framework that discovers check `.sql` files from `src/checks/`, executes them against the DuckDB warehouse, and reports pass/fail/error with severity levels. Checks follow the dbt convention: returned rows = violations (zero rows = pass). Includes 6 pre-built checks covering completeness, timeliness, uniqueness, and validity. Results are persisted in `check_runs` and `check_results` DuckDB metadata tables.

## Technical Context

**Language/Version**: Python 3.11+
**Primary Dependencies**: DuckDB (SQL execution, metadata storage)
**Storage**: DuckDB embedded database at `data/warehouse/transactions.duckdb`
**Testing**: pytest (unit, integration, quality tests)
**Target Platform**: Local development (macOS/Linux)
**Performance Goals**: All checks complete in < 5 seconds
**Constraints**: Read-only checks (no data mutation), violation-based convention
**Scale/Scope**: 6 pre-built checks, 3 model tables + 1 staging view

## Constitution Check

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Data Quality First | Core focus | This feature IS the quality layer |
| II. Metadata & Lineage | Pass | check_runs + check_results tables track all outcomes |
| III. Security & Privacy | Pass | No PII handling, read-only operations |
| IV. Integration & Interop | Pass | SQL files, standard DuckDB |
| V. Architecture Integrity | Pass | Follows same module pattern as transformer/ |

## Project Structure

### Documentation

```text
specs/004-data-quality-checks/
  spec.md
  plan.md
  research.md
  data-model.md
  checklists/
    data-quality.md
  tasks.md
```

### Source Code

```text
src/
  checker/
    __init__.py
    models.py          # CheckSeverity, CheckStatus, RunStatus enums + dataclasses
    parser.py          # Parse check SQL files (headers + SQL content)
    runner.py          # Execute checks, record metadata, compute run status
  checks/
    check__row_count_staging.sql
    check__mart_not_empty.sql
    check__freshness.sql
    check__unique_daily_spend_grain.sql
    check__unique_monthly_summary_grain.sql
    check__accepted_values_currency.sql
  run_checks.py        # CLI entrypoint (PEP 723)

tests/
  unit/
    test_check_parser.py
    test_check_runner.py
  integration/
    test_checks.py
  quality/
    test_check_quality.py
```

**Structure Decision**: New `src/checker/` module parallel to `src/transformer/`. Same file-per-concern pattern (models, parser, runner). Check SQL files live in `src/checks/` parallel to `src/transforms/`.

## Complexity Tracking

No constitution violations. Feature follows established patterns from Feature 003.
