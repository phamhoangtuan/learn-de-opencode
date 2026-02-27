-- check: fct_no_unknown_account_sk
-- severity: warn
-- description: Reports rows with account_sk = -1 (expected on first run, should be 0 in steady state)
SELECT
    transaction_id,
    account_id,
    account_sk
FROM fct_transactions
WHERE account_sk = -1;
