-- check: row_count_staging
-- severity: critical
-- description: Staging view row count must match source transactions table
SELECT
    'stg_transactions' AS check_target,
    src.cnt AS source_count,
    stg.cnt AS staging_count
FROM
    (SELECT COUNT(*) AS cnt FROM transactions) src,
    (SELECT COUNT(*) AS cnt FROM stg_transactions) stg
WHERE src.cnt != stg.cnt;
