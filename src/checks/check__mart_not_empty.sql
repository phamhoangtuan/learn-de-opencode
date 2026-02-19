-- check: mart_not_empty
-- severity: critical
-- description: All mart tables must contain at least one row
SELECT table_name, row_count
FROM (
    SELECT 'daily_spend_by_category' AS table_name,
           (SELECT COUNT(*) FROM daily_spend_by_category) AS row_count
    UNION ALL
    SELECT 'monthly_account_summary' AS table_name,
           (SELECT COUNT(*) FROM monthly_account_summary) AS row_count
) marts
WHERE row_count = 0;
