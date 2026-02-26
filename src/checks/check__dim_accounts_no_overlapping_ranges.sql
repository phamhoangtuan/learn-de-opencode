-- check: dim_accounts_no_overlapping_ranges
-- severity: critical
-- description: No two versions of the same account_id may have overlapping valid_from/valid_to ranges.
--   Returns pairs of conflicting account_sk values.
--   Zero rows = check passes.
SELECT a.account_id, a.account_sk, b.account_sk AS b_account_sk
FROM dim_accounts a
JOIN dim_accounts b
  ON a.account_id = b.account_id
 AND a.account_sk < b.account_sk
WHERE a.valid_from < COALESCE(b.valid_to, '9999-12-31'::TIMESTAMPTZ)
  AND b.valid_from < COALESCE(a.valid_to, '9999-12-31'::TIMESTAMPTZ)
