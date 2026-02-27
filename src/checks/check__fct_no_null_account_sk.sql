-- check: fct_no_null_account_sk
-- severity: critical
-- description: No NULL account_sk in fct_transactions (COALESCE should produce -1, not NULL)
SELECT
    transaction_id,
    account_id,
    account_sk
FROM fct_transactions
WHERE account_sk IS NULL;
