-- check: unique_monthly_summary_grain
-- severity: critical
-- description: No duplicate (month, account_id, currency) grains in monthly_account_summary
SELECT
    month,
    account_id,
    currency,
    COUNT(*) AS row_count
FROM monthly_account_summary
GROUP BY month, account_id, currency
HAVING COUNT(*) > 1;
