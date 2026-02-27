-- model: fct_transactions
-- depends_on: stg_transactions, dim_accounts
CREATE OR REPLACE TABLE fct_transactions AS
SELECT
    stg.transaction_id,
    stg.transaction_timestamp,
    stg.transaction_date,
    COALESCE(d.account_sk, -1) AS account_sk,
    stg.account_id,
    stg.amount,
    stg.currency,
    stg.merchant_name,
    stg.category,
    stg.transaction_type,
    stg.status,
    stg.source_file,
    stg.ingested_at,
    stg.run_id
FROM stg_transactions stg
LEFT JOIN dim_accounts d
  ON  stg.account_id = d.account_id
  AND stg.transaction_timestamp >= d.valid_from
  AND stg.transaction_timestamp < COALESCE(d.valid_to, '9999-12-31 23:59:59+00'::TIMESTAMPTZ);
