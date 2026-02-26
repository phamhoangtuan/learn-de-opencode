# Contract: Data Quality Checks — dim_accounts

**Feature**: 009-scd2-accounts
**Date**: 2026-02-26

---

## Overview

Three new SQL check files are added to `src/checks/` to verify the structural integrity of the `dim_accounts` SCD Type 2 dimension table (FR-008). All three are `critical` severity — a failure in any one indicates a broken SCD merge and must block the pipeline from being considered healthy.

Each check follows the standard check file format established in Feature 004:

```sql
-- check: <check_name>
-- severity: critical|warn
-- description: <human-readable description>
SELECT <violating_rows> ...;
```

A check **passes** when the SELECT returns zero rows. A check **fails** when the SELECT returns one or more rows, each representing a distinct violation.

---

## Check 1: `check__dim_accounts_single_current.sql`

### Metadata

| Field | Value |
|-------|-------|
| **Check name** | `dim_accounts_single_current` |
| **File** | `src/checks/check__dim_accounts_single_current.sql` |
| **Severity** | `critical` |
| **Description** | Every account_id must have exactly one is_current=TRUE row in dim_accounts |

### Purpose

Detects any `account_id` that has more than one row with `is_current = TRUE`. This condition indicates the SCD merge inserted a new version row without successfully closing the previous current row — a fundamental corruption of the dimension's current-state view.

### SQL Query

```sql
-- check: dim_accounts_single_current
-- severity: critical
-- description: Every account_id must have exactly one is_current=TRUE row in dim_accounts
SELECT
    account_id,
    COUNT(*) AS current_row_count
FROM dim_accounts
WHERE is_current = TRUE
GROUP BY account_id
HAVING COUNT(*) > 1;
```

### Expected Result

| Condition | Result |
|-----------|--------|
| PASS | Query returns zero rows. Every `account_id` has exactly one `is_current = TRUE` row. |
| FAIL | Query returns one or more rows. Each row identifies an `account_id` with `current_row_count > 1`. Example: `account_id = 'ACC001', current_row_count = 2`. |

### Acceptance Scenario Coverage

Covers spec acceptance scenario: "Given a dim_accounts with two rows marked `is_current = true` for the same account_id, When integrity checks run, Then the check fails and reports the violation."

---

## Check 2: `check__dim_accounts_no_overlapping_ranges.sql`

### Metadata

| Field | Value |
|-------|-------|
| **Check name** | `dim_accounts_no_overlapping_ranges` |
| **File** | `src/checks/check__dim_accounts_no_overlapping_ranges.sql` |
| **Severity** | `critical` |
| **Description** | No two versions of the same account_id may have overlapping valid_from/valid_to date ranges |

### Purpose

Detects any pair of rows for the same `account_id` whose validity windows overlap. Two ranges overlap when one row's `valid_from` falls before the other row's `valid_to` and vice versa (strict overlap: `a.valid_from < b.valid_to AND b.valid_from < a.valid_to`). A self-join with `a.account_sk < b.account_sk` avoids reporting each overlapping pair twice and excludes self-comparison.

Overlapping ranges indicate the SCD merge closed a row with an incorrect `valid_to` date, or inserted a new row with a `valid_from` that precedes the end of an existing version window.

### SQL Query

```sql
-- check: dim_accounts_no_overlapping_ranges
-- severity: critical
-- description: No two versions of the same account_id may have overlapping valid_from/valid_to date ranges
SELECT
    a.account_id,
    a.account_sk        AS sk_a,
    a.valid_from        AS valid_from_a,
    a.valid_to          AS valid_to_a,
    b.account_sk        AS sk_b,
    b.valid_from        AS valid_from_b,
    b.valid_to          AS valid_to_b
FROM dim_accounts a
JOIN dim_accounts b
    ON  a.account_id  = b.account_id
    AND a.account_sk  < b.account_sk
WHERE a.valid_from < b.valid_to
  AND b.valid_from < a.valid_to;
```

### Expected Result

| Condition | Result |
|-----------|--------|
| PASS | Query returns zero rows. All validity windows for every `account_id` are non-overlapping. |
| FAIL | Query returns one or more rows. Each row identifies a pair of overlapping version rows for the same `account_id`. Example: `account_id = 'ACC001', sk_a = 1, valid_from_a = 2026-01-01, valid_to_a = 9999-12-31, sk_b = 5, valid_from_b = 2026-02-01, valid_to_b = 9999-12-31`. |

### Acceptance Scenario Coverage

Covers spec success criterion SC-003: "zero violations for overlapping ranges."

---

## Check 3: `check__dim_accounts_no_null_sk.sql`

### Metadata

| Field | Value |
|-------|-------|
| **Check name** | `dim_accounts_no_null_sk` |
| **File** | `src/checks/check__dim_accounts_no_null_sk.sql` |
| **Severity** | `critical` |
| **Description** | No row in dim_accounts may have a NULL account_sk surrogate key |

### Purpose

Detects any row in `dim_accounts` where `account_sk` is NULL. Because `account_sk` is assigned via `nextval('dim_accounts_sk_seq')` at INSERT time, a NULL value indicates the sequence was not invoked correctly — for example, if a row was inserted via a code path that bypassed the sequence default, or if the column default was overridden with an explicit NULL.

A NULL surrogate key breaks all downstream joins on `account_sk` and makes the row unaddressable in the dimension.

### SQL Query

```sql
-- check: dim_accounts_no_null_sk
-- severity: critical
-- description: No row in dim_accounts may have a NULL account_sk surrogate key
SELECT
    account_id,
    valid_from,
    valid_to,
    is_current
FROM dim_accounts
WHERE account_sk IS NULL;
```

### Expected Result

| Condition | Result |
|-----------|--------|
| PASS | Query returns zero rows. Every row has a non-NULL `account_sk`. |
| FAIL | Query returns one or more rows. Each row identifies a dimension record with a missing surrogate key. Example: `account_id = 'ACC007', valid_from = 2026-02-26, valid_to = 9999-12-31, is_current = TRUE`. |

### Acceptance Scenario Coverage

Covers spec success criterion SC-003: "zero violations for ... null surrogate keys."

---

## Summary

| File | Check Name | Severity | Detects |
|------|-----------|----------|---------|
| `check__dim_accounts_single_current.sql` | `dim_accounts_single_current` | critical | account_id with more than one `is_current = TRUE` row |
| `check__dim_accounts_no_overlapping_ranges.sql` | `dim_accounts_no_overlapping_ranges` | critical | Two version rows for the same account_id with overlapping `valid_from`/`valid_to` windows |
| `check__dim_accounts_no_null_sk.sql` | `dim_accounts_no_null_sk` | critical | Any row with `account_sk IS NULL` |

All three checks are discovered and executed automatically by the existing check runner (Feature 004) during each pipeline run.
