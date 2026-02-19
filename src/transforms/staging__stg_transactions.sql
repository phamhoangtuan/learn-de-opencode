-- model: stg_transactions
-- depends_on: transactions
CREATE OR REPLACE VIEW stg_transactions AS
SELECT
    transaction_id,
    "timestamp"         AS transaction_timestamp,
    transaction_date,
    amount,
    currency,
    merchant_name,
    category,
    account_id,
    transaction_type,
    status,
    source_file,
    ingested_at,
    run_id
FROM transactions;
