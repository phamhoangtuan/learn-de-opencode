-- check: dim_accounts_single_current
-- severity: critical
-- description: Each account_id must have exactly one is_current=TRUE row.
--   Returns account_id + count for any account with more than one current row.
--   Zero rows = check passes.
SELECT account_id, COUNT(*) AS current_count
FROM dim_accounts
WHERE is_current = TRUE
GROUP BY account_id
HAVING COUNT(*) > 1
