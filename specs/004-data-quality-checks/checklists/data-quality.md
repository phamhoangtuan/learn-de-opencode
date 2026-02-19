# Data Quality Checklist: Data Quality Checks

**Feature**: 004-data-quality-checks
**Date**: 2026-02-19
**Purpose**: Verify the quality check framework itself meets DAMA-DMBOK standards

## Completeness

- [x] CHK001: All 6 pre-built check SQL files exist in `src/checks/`
- [x] CHK002: Each check SQL file has `-- check:`, `-- severity:`, and `-- description:` headers
- [x] CHK003: Parser extracts all metadata fields correctly
- [x] CHK004: Runner discovers and executes all check files in the directory

## Consistency

- [x] CHK005: `check__row_count_staging` passes on a healthy warehouse
- [x] CHK006: `check__mart_not_empty` passes on a warehouse with data
- [x] CHK007: `check__freshness` passes on recently-ingested data
- [x] CHK008: `check__unique_daily_spend_grain` passes on correct mart data
- [x] CHK009: `check__unique_monthly_summary_grain` passes on correct mart data
- [x] CHK010: `check__accepted_values_currency` passes on valid currencies

## Correctness

- [x] CHK011: A check returning zero rows is marked "pass"
- [x] CHK012: A check returning rows is marked "fail" with correct violation count
- [x] CHK013: A check with SQL syntax error is marked "error" (not crash)
- [x] CHK014: Critical failure sets run status to "failed" and exit code 1
- [x] CHK015: Warn-only failure sets run status to "warn" and exit code 0
- [x] CHK016: All-pass sets run status to "passed" and exit code 0
- [x] CHK017: Sample violations capture up to 5 rows as JSON

## Metadata & Lineage

- [x] CHK018: `check_runs` table created with correct schema
- [x] CHK019: `check_results` table created with correct schema
- [x] CHK020: Each run creates exactly one `check_runs` row
- [x] CHK021: Each check creates exactly one `check_results` row per run
- [x] CHK022: `run_id` links `check_runs` to `check_results`

## Idempotency

- [x] CHK023: Running checks twice on same data produces same pass/fail outcomes
- [x] CHK024: Running checks twice creates two separate `check_runs` rows

## Robustness

- [x] CHK025: Empty checks directory completes with 0 checks, status "passed"
- [x] CHK026: Missing database file produces clear error before executing checks
- [x] CHK027: Missing mart tables produce per-check errors, not runner crash
