-- model: monthly_account_summary
-- depends_on: stg_transactions
CREATE OR REPLACE TABLE monthly_account_summary AS
SELECT
    DATE_TRUNC('month', transaction_date)::DATE AS month,
    account_id,
    currency,
    SUM(CASE WHEN transaction_type = 'debit'  THEN amount ELSE 0 END) AS total_debits,
    SUM(CASE WHEN transaction_type = 'credit' THEN amount ELSE 0 END) AS total_credits,
    SUM(CASE WHEN transaction_type = 'credit' THEN amount ELSE 0 END)
      - SUM(CASE WHEN transaction_type = 'debit' THEN amount ELSE 0 END) AS net_flow,
    COUNT(*) AS transaction_count
FROM stg_transactions
WHERE status = 'completed'
GROUP BY DATE_TRUNC('month', transaction_date)::DATE, account_id, currency
ORDER BY month, account_id, currency;
