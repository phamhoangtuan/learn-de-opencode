-- check: accepted_values_currency
-- severity: warn
-- description: All currency values in staging must be in the accepted set (USD, EUR, GBP, JPY)
SELECT
    transaction_id,
    currency
FROM stg_transactions
WHERE currency NOT IN ('USD', 'EUR', 'GBP', 'JPY');
