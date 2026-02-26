-- check: dim_accounts_no_null_sk
-- severity: critical
-- description: All dim_accounts rows must have a non-null surrogate key.
--   Zero rows = check passes.
SELECT account_id, valid_from
FROM dim_accounts
WHERE account_sk IS NULL
