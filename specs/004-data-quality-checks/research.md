# Research: Data Quality Checks

**Feature**: 004-data-quality-checks
**Date**: 2026-02-19

## Approach: dbt-Style SQL Test Convention

The dominant pattern in data engineering for quality checks is the **dbt test convention**: a check is a SQL `SELECT` that returns **violating rows**. Zero rows = pass, any rows = fail. This is simple, composable, and requires no custom DSL.

### Why This Pattern

1. **No new abstractions** -- checks are just SQL files, same tooling as transforms.
2. **Composable** -- each check is independent, can be added/removed by dropping files.
3. **Debuggable** -- when a check fails, the violating rows are the diagnostic output.
4. **Familiar** -- every data engineer knows `SELECT ... WHERE bad_condition`.

### Alternatives Considered

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| Great Expectations | Rich ecosystem, auto-profiling | Heavy dependency, learning overhead | Reject -- overkill for 3 tables |
| Soda Core | YAML-based checks, SaaS option | External dependency, YAML DSL | Reject -- adds complexity |
| Custom Python assertions | Full flexibility | Not reusable as SQL, harder to read | Reject -- SQL is clearer |
| **dbt-style SQL checks** | Simple, no deps, composable | No auto-profiling | **Selected** |

## Architecture Decision: Separate Module

The check runner will be a new `src/checker/` module, parallel to `src/transformer/`. This keeps responsibilities clean:

- `src/transformer/` -- writes data (CREATE OR REPLACE)
- `src/checker/` -- reads data (SELECT only, never mutates)

The checker shares the same parser pattern (SQL comment headers) but with different header fields (`-- check:`, `-- severity:`, `-- description:` vs `-- model:`, `-- depends_on:`).

## Severity Model

Two severity levels:

| Severity | On Failure | Exit Code |
|----------|-----------|-----------|
| `critical` | Run status = "failed" | 1 |
| `warn` | Run status = "warn" | 0 |

If no checks fail, status = "passed", exit code 0.

## Metadata Tables

Two new tables in DuckDB, following the `transform_runs` pattern:

- `check_runs` -- one row per invocation
- `check_results` -- one row per check per invocation

## Pre-Built Checks (6 minimum)

Based on the DAMA-DMBOK 6Cs quality dimensions:

| Check | Dimension | Severity | What It Validates |
|-------|-----------|----------|-------------------|
| `check__row_count_staging.sql` | Completeness | critical | stg_transactions row count = transactions row count |
| `check__mart_not_empty.sql` | Completeness | critical | Both mart tables have > 0 rows |
| `check__freshness.sql` | Timeliness | warn | Latest ingested_at within 48 hours |
| `check__unique_daily_spend_grain.sql` | Uniqueness | critical | No duplicate (date, category, currency) in daily_spend |
| `check__unique_monthly_summary_grain.sql` | Uniqueness | critical | No duplicate (month, account_id, currency) in monthly_summary |
| `check__accepted_values_currency.sql` | Validity | warn | All currencies in stg_transactions are in allowed set |

## Performance

With 3 models and ~10K rows, all checks should complete in under 1 second. DuckDB's in-process execution means no network overhead.
