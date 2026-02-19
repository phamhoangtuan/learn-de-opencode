-- check: unique_daily_spend_grain
-- severity: critical
-- description: No duplicate (transaction_date, category, currency) grains in daily_spend_by_category
SELECT
    transaction_date,
    category,
    currency,
    COUNT(*) AS row_count
FROM daily_spend_by_category
GROUP BY transaction_date, category, currency
HAVING COUNT(*) > 1;
