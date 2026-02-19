# Data Model: SQL Transformations Layer

**Feature**: 003-sql-transformations  
**Date**: 2026-02-19

## Entities

### Transform Run (warehouse table: `transform_runs`)

Metadata table tracking each transform pipeline execution. Follows the same pattern as Feature 002's `ingestion_runs` table.

| Field              | Type                     | Constraints                                          | Description                                          |
|--------------------|--------------------------|------------------------------------------------------|------------------------------------------------------|
| run_id             | VARCHAR                  | PRIMARY KEY, NOT NULL                                | Unique identifier (UUID) for this transform run      |
| started_at         | TIMESTAMP WITH TIME ZONE | NOT NULL                                             | When the transform run began                         |
| completed_at       | TIMESTAMP WITH TIME ZONE | NULL (set on completion)                             | When the transform run finished                      |
| status             | VARCHAR                  | NOT NULL, IN ('running','completed','failed')        | Run outcome                                          |
| models_executed    | INTEGER                  | NOT NULL, DEFAULT 0                                  | Number of models successfully executed               |
| models_failed      | INTEGER                  | NOT NULL, DEFAULT 0                                  | Number of models that failed execution               |
| elapsed_seconds    | DOUBLE                   | NULL (set on completion)                             | Total transform run duration in seconds              |
| error_message      | VARCHAR                  | NULL                                                 | Error description if status is 'failed'              |

### Staging View: `stg_transactions`

A SQL VIEW (not materialized) that standardizes the raw `transactions` table. Always reflects the latest ingested data.

| Column             | Type                     | Source                                               | Description                                          |
|--------------------|--------------------------|------------------------------------------------------|------------------------------------------------------|
| transaction_id     | VARCHAR                  | transactions.transaction_id                          | Unique transaction identifier                        |
| transaction_timestamp | TIMESTAMP WITH TIME ZONE | transactions.timestamp                            | Renamed for clarity (avoids SQL reserved word)       |
| transaction_date   | DATE                     | transactions.transaction_date                        | Date component for partitioning/grouping             |
| amount             | DOUBLE                   | transactions.amount                                  | Transaction amount in original currency              |
| currency           | VARCHAR                  | transactions.currency                                | ISO 4217 currency code                               |
| merchant_name      | VARCHAR                  | transactions.merchant_name                           | Merchant name                                        |
| category           | VARCHAR                  | transactions.category                                | Spending category                                    |
| account_id         | VARCHAR                  | transactions.account_id                              | Account identifier                                   |
| transaction_type   | VARCHAR                  | transactions.transaction_type                        | 'debit' or 'credit'                                  |
| status             | VARCHAR                  | transactions.status                                  | 'completed', 'pending', or 'failed'                  |
| source_file        | VARCHAR                  | transactions.source_file                             | Lineage: source Parquet file                         |
| ingested_at        | TIMESTAMP WITH TIME ZONE | transactions.ingested_at                             | Lineage: when record was ingested                    |
| run_id             | VARCHAR                  | transactions.run_id                                  | Lineage: ingestion run that loaded this record       |

**Notes**:
- Renames `timestamp` to `transaction_timestamp` to avoid conflict with SQL reserved word.
- Passes through all columns to maintain full lineage.
- No filtering or aggregation -- staging is a 1:1 mapping.

### Mart Table: `daily_spend_by_category`

Aggregates completed debit transactions by date, category, and currency.

| Column             | Type    | Description                                                    |
|--------------------|---------|----------------------------------------------------------------|
| transaction_date   | DATE    | The date of the transactions                                   |
| category           | VARCHAR | Spending category (e.g., Groceries, Dining)                    |
| currency           | VARCHAR | ISO 4217 currency code                                         |
| total_amount       | DOUBLE  | Sum of transaction amounts for this date/category/currency     |
| transaction_count  | INTEGER | Number of transactions in this group                           |
| avg_amount         | DOUBLE  | Average transaction amount (total_amount / transaction_count)  |

**Filters**: Only includes transactions where `transaction_type = 'debit'` AND `status = 'completed'`.  
**Grain**: One row per (transaction_date, category, currency).

### Mart Table: `monthly_account_summary`

Aggregates account-level debit/credit metrics by month and currency.

| Column             | Type    | Description                                                    |
|--------------------|---------|----------------------------------------------------------------|
| month              | DATE    | First day of the month (e.g., 2026-01-01)                      |
| account_id         | VARCHAR | Account identifier                                             |
| currency           | VARCHAR | ISO 4217 currency code                                         |
| total_debits       | DOUBLE  | Sum of debit transaction amounts                               |
| total_credits      | DOUBLE  | Sum of credit transaction amounts                              |
| net_flow           | DOUBLE  | total_credits - total_debits                                   |
| transaction_count  | INTEGER | Total number of transactions (debit + credit)                  |

**Filters**: Only includes transactions where `status = 'completed'`.  
**Grain**: One row per (month, account_id, currency).

## Relationships

```text
transactions (raw)  ---- standardized by ----> stg_transactions (view)
stg_transactions    ---- aggregated into ----> daily_spend_by_category (table)
stg_transactions    ---- aggregated into ----> monthly_account_summary (table)
transform_runs      ---- tracks execution of ----> all models
```

- `stg_transactions` is a VIEW over `transactions` -- no data duplication.
- `daily_spend_by_category` and `monthly_account_summary` are materialized tables, rebuilt on each transform run via CREATE OR REPLACE TABLE AS.
- `transform_runs` is independent of the data tables; it tracks operational metadata only.

## State Transitions

### Transform Run Lifecycle

```text
running --> completed   (all models executed, zero or more failures)
running --> failed       (critical error preventing execution, e.g., no source data)
```

- A run with some model failures but overall completion still gets status 'completed' (partial success).
- Only infrastructure-level failures (no database connection, no source table, circular deps) produce 'failed' status.

### Output Tables

Output views and tables are stateless in the traditional sense: they are fully replaced on each run. There are no incremental state transitions -- each run produces a complete fresh output.
