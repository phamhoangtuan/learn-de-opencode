-- model: mart__account_history
-- depends_on: dim_accounts
CREATE OR REPLACE VIEW mart__account_history AS
SELECT
    account_sk,
    account_id,
    primary_currency,
    primary_category,
    transaction_count,
    total_spend,
    first_seen,
    last_seen,
    valid_from,
    valid_to,
    is_current,
    ROW_NUMBER() OVER (
        PARTITION BY account_id
        ORDER BY valid_from ASC
    )                                                        AS version_number,
    CASE
        WHEN is_current THEN
            CAST(CURRENT_DATE - CAST(valid_from AS DATE) AS INTEGER)
        ELSE
            CAST(CAST(valid_to AS DATE) - CAST(valid_from AS DATE) AS INTEGER)
    END                                                      AS version_duration_days
FROM dim_accounts
ORDER BY account_id, valid_from
