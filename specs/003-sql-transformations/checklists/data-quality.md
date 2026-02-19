# Data Quality Checklist: SQL Transformations Layer

**Purpose**: Verify data quality, correctness, and idempotency of all SQL transformation outputs
**Created**: 2026-02-19
**Feature**: [spec.md](../spec.md)

## Staging Layer Quality

- [x] CHK001 `stg_transactions` row count matches `transactions` table exactly (zero data loss)
- [x] CHK002 `stg_transactions` has no NULL values in columns defined as NOT NULL in the source
- [x] CHK003 `stg_transactions.transaction_timestamp` correctly maps from `transactions.timestamp`
- [x] CHK004 `stg_transactions.transaction_date` is a valid DATE derived from the timestamp
- [x] CHK005 All lineage columns (source_file, ingested_at, run_id) pass through without alteration

## Mart Table Correctness

- [x] CHK006 `daily_spend_by_category` only includes transactions where `transaction_type = 'debit'` AND `status = 'completed'`
- [x] CHK007 `daily_spend_by_category.total_amount` matches `SUM(amount)` from manual aggregation of `stg_transactions`
- [x] CHK008 `daily_spend_by_category.transaction_count` matches `COUNT(*)` from manual aggregation
- [x] CHK009 `daily_spend_by_category.avg_amount` equals `total_amount / transaction_count`
- [x] CHK010 `daily_spend_by_category` grain is unique on (transaction_date, category, currency) -- no duplicate keys
- [x] CHK011 `monthly_account_summary` only includes transactions where `status = 'completed'`
- [x] CHK012 `monthly_account_summary.total_debits` matches filtered `SUM(amount) WHERE transaction_type = 'debit'`
- [x] CHK013 `monthly_account_summary.total_credits` matches filtered `SUM(amount) WHERE transaction_type = 'credit'`
- [x] CHK014 `monthly_account_summary.net_flow` equals `total_credits - total_debits`
- [x] CHK015 `monthly_account_summary` grain is unique on (month, account_id, currency) -- no duplicate keys

## Idempotency

- [x] CHK016 Running transforms twice produces identical `stg_transactions` output (VIEW, so inherently idempotent)
- [x] CHK017 Running transforms twice produces identical `daily_spend_by_category` row counts and values
- [x] CHK018 Running transforms twice produces identical `monthly_account_summary` row counts and values
- [x] CHK019 No orphan tables or views left behind after re-execution

## Dependency Resolution

- [x] CHK020 Staging models execute before mart models that depend on them
- [x] CHK021 Circular dependency detection produces a clear error message
- [x] CHK022 Missing dependency detection produces a clear error identifying the unresolvable model

## Metadata Tracking

- [x] CHK023 Each transform run creates exactly one `transform_runs` row
- [x] CHK024 `transform_runs.models_executed` reflects the actual count of successfully executed models
- [x] CHK025 `transform_runs.models_failed` reflects the actual count of failed models
- [x] CHK026 `transform_runs.elapsed_seconds` is positive and reasonable (not zero, not negative)
- [x] CHK027 `transform_runs.status` is 'completed' for successful runs, 'failed' for infrastructure failures

## Notes

- Check items off as completed: `[x]`
- Add comments or findings inline
- All quality checks should be implemented as automated tests where possible
