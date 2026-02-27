-- check: fct_unique_transaction_id
-- severity: critical
-- description: No duplicate transaction_id in fct_transactions (catches JOIN fan-out)
SELECT
    transaction_id,
    COUNT(*) AS occurrence_count
FROM fct_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;
