-- model: daily_spend_by_category
-- depends_on: stg_transactions
CREATE OR REPLACE TABLE daily_spend_by_category AS
SELECT
    transaction_date,
    category,
    currency,
    SUM(amount)            AS total_amount,
    COUNT(*)               AS transaction_count,
    SUM(amount) / COUNT(*) AS avg_amount
FROM stg_transactions
WHERE transaction_type = 'debit'
  AND status = 'completed'
GROUP BY transaction_date, category, currency
ORDER BY transaction_date, category, currency;
