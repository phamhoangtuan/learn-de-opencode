# Data Model: DuckDB Ingestion Pipeline

**Feature**: 002-duckdb-ingestion  
**Date**: 2026-02-18

## Entities

### Transaction (warehouse table: `transactions`)

The core entity stored in the DuckDB warehouse. Combines the 9 source columns from raw Parquet files with 4 enrichment columns added during ingestion.

| Field              | Type                  | Constraints                                            | Description                                          |
|--------------------|-----------------------|--------------------------------------------------------|------------------------------------------------------|
| transaction_id     | VARCHAR               | PRIMARY KEY, NOT NULL                                  | UUID-format hex string (8-4-4-4-12), unique per txn  |
| timestamp          | TIMESTAMP WITH TIME ZONE | NOT NULL                                            | Transaction time in UTC, microsecond precision       |
| amount             | DOUBLE                | NOT NULL, > 0                                          | Transaction amount in the specified currency         |
| currency           | VARCHAR               | NOT NULL, IN ('USD','EUR','GBP','JPY')                 | ISO 4217 currency code                               |
| merchant_name      | VARCHAR               | NOT NULL                                               | Name of the merchant                                 |
| category           | VARCHAR               | NOT NULL                                               | Spending category (e.g., Groceries, Dining)          |
| account_id         | VARCHAR               | NOT NULL, MATCHES 'ACC-\d{5}'                          | Account identifier in ACC-XXXXX format               |
| transaction_type   | VARCHAR               | NOT NULL, IN ('debit','credit')                        | Transaction direction                                |
| status             | VARCHAR               | NOT NULL, IN ('completed','pending','failed')          | Transaction processing status                        |
| transaction_date   | DATE                  | NOT NULL, derived from timestamp                       | Computed date for partition-style filtering           |
| source_file        | VARCHAR               | NOT NULL                                               | Filename of the source Parquet file                   |
| ingested_at        | TIMESTAMP WITH TIME ZONE | NOT NULL, DEFAULT CURRENT_TIMESTAMP                 | When this record was loaded into the warehouse       |
| run_id             | VARCHAR               | NOT NULL                                               | Unique identifier for the pipeline execution run     |

**Validation rules**:
- `amount` must be strictly positive (> 0)
- `currency` must be one of the four supported codes
- `transaction_type` must be 'debit' or 'credit'
- `status` must be 'completed', 'pending', or 'failed'
- `account_id` must match the pattern `ACC-\d{5}`
- `transaction_id` must not be null or empty
- `timestamp` must not be null
- `merchant_name` and `category` must not be null or empty

### Quarantine Record (warehouse table: `quarantine`)

Stores records that failed validation, along with diagnostic metadata.

| Field              | Type                  | Constraints                | Description                                          |
|--------------------|-----------------------|----------------------------|------------------------------------------------------|
| quarantine_id      | INTEGER               | PRIMARY KEY, AUTO INCREMENT | Unique identifier for the quarantine entry           |
| source_file        | VARCHAR               | NOT NULL                   | Filename of the source Parquet file                   |
| record_data        | VARCHAR               | NOT NULL                   | JSON-serialized original record data                  |
| rejection_reason   | VARCHAR               | NOT NULL                   | Description of why the record was rejected            |
| rejected_at        | TIMESTAMP WITH TIME ZONE | NOT NULL, DEFAULT CURRENT_TIMESTAMP | When the rejection occurred              |
| run_id             | VARCHAR               | NOT NULL                   | Pipeline run that attempted ingestion                 |

**Notes**:
- File-level rejections (schema mismatch) store a summary in `record_data` (e.g., `{"error": "missing columns: ['amount']"}`) rather than individual records.
- Record-level rejections store the full original record as JSON, enabling later inspection and potential reprocessing.

### Ingestion Run (warehouse table: `ingestion_runs`)

Metadata table tracking each pipeline execution for operational visibility.

| Field              | Type                  | Constraints                | Description                                          |
|--------------------|-----------------------|----------------------------|------------------------------------------------------|
| run_id             | VARCHAR               | PRIMARY KEY, NOT NULL      | Unique identifier (UUID) for this pipeline run       |
| started_at         | TIMESTAMP WITH TIME ZONE | NOT NULL                | When the pipeline run began                          |
| completed_at       | TIMESTAMP WITH TIME ZONE | NULL (set on completion) | When the pipeline run finished                       |
| status             | VARCHAR               | NOT NULL, IN ('running','completed','failed') | Run outcome              |
| files_processed    | INTEGER               | NOT NULL, DEFAULT 0        | Number of Parquet files processed                    |
| records_loaded     | INTEGER               | NOT NULL, DEFAULT 0        | Number of records successfully loaded                |
| records_quarantined| INTEGER               | NOT NULL, DEFAULT 0        | Number of records sent to quarantine                 |
| duplicates_skipped | INTEGER               | NOT NULL, DEFAULT 0        | Number of duplicate records skipped                  |
| elapsed_seconds    | DOUBLE                | NULL (set on completion)   | Total pipeline run duration in seconds               |

## Relationships

```text
Ingestion Run (1) ---- produces ----> (N) Transaction
Ingestion Run (1) ---- produces ----> (N) Quarantine Record
Source File   (1) ---- contains ----> (N) Transaction
Source File   (1) ---- contains ----> (N) Quarantine Record
```

- Each `Transaction` belongs to exactly one `Ingestion Run` (via `run_id`) and one `Source File` (via `source_file`).
- Each `Quarantine Record` belongs to exactly one `Ingestion Run` and one `Source File`.
- An `Ingestion Run` processes one or more `Source Files` and produces zero or more `Transactions` and `Quarantine Records`.
- `Source File` is not a separate table; it is tracked as a column in both `transactions` and `quarantine`, and as a logical concept.

## State Transitions

### Ingestion Run Lifecycle

```text
running --> completed   (normal termination)
running --> failed       (unrecoverable error)
```

- Runs begin in `running` state when the pipeline starts.
- On successful completion, status transitions to `completed` and `completed_at`/`elapsed_seconds` are set.
- On failure, status transitions to `failed` with `completed_at` set to the failure time.

### Records do not transition states

Once a record is loaded into `transactions`, it remains there permanently (append-only warehouse). Once a record is placed in `quarantine`, it remains there unless manually removed by the user. There is no automatic reprocessing of quarantined records.

## Schema Versioning

- The initial schema is v1.0 as defined above.
- Schema changes should be handled via migration scripts (not yet needed for v1).
- Adding nullable columns is a non-breaking change.
- Renaming or removing columns requires a migration with data transformation.
- The `ingestion_runs` table provides an audit trail of when schema versions were active.
