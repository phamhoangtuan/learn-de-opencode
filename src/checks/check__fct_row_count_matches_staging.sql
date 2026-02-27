-- check: fct_row_count_matches_staging
-- severity: critical
-- description: fct_transactions row count must match stg_transactions (LEFT JOIN preserves all rows)
SELECT
    'row_count_mismatch' AS check_detail,
    stg_count,
    fct_count,
    stg_count - fct_count AS difference
FROM (
    SELECT
        (SELECT COUNT(*) FROM stg_transactions) AS stg_count,
        (SELECT COUNT(*) FROM fct_transactions) AS fct_count
)
WHERE stg_count != fct_count;
