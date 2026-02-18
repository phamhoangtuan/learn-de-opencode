# Feature Specification: DuckDB Ingestion Pipeline

**Feature Branch**: `002-duckdb-ingestion`  
**Created**: 2026-02-18  
**Status**: Draft  
**Input**: User description: "Build a data ingestion pipeline that reads raw Parquet files from data/raw/ and loads them into a local DuckDB warehouse with schema validation, deduplication, idempotent loads, lineage tracking, and date-based partitioning"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Ingest Raw Transaction Files (Priority: P1)

As a data engineer, I want to run a single command that reads all raw Parquet files from the `data/raw/` directory and loads them into a local DuckDB database, so that I have a queryable analytical warehouse without manual data manipulation.

**Why this priority**: This is the core value proposition. Without basic ingestion, no other functionality (validation, dedup, lineage) has a surface to operate on. This story alone delivers an MVP that moves raw files into a queryable store.

**Independent Test**: Can be fully tested by generating sample Parquet files with the Feature 001 generator, running the ingestion command, and querying the DuckDB database to confirm all records are present and queryable.

**Acceptance Scenarios**:

1. **Given** one or more Parquet files exist in `data/raw/`, **When** the user runs the ingestion command, **Then** all records from those files are loaded into the DuckDB warehouse and are queryable.
2. **Given** the `data/raw/` directory is empty, **When** the user runs the ingestion command, **Then** the system reports that no files were found and exits gracefully without errors.
3. **Given** the DuckDB database does not yet exist, **When** the user runs the ingestion command for the first time, **Then** the database and required tables are created automatically.
4. **Given** a Parquet file has already been fully ingested, **When** the user runs the ingestion command again, **Then** no duplicate records are inserted (idempotent behavior).

---

### User Story 2 - Schema Validation and Quarantine (Priority: P2)

As a data engineer, I want the pipeline to validate each record against the expected schema before loading, and reject malformed records to a quarantine area, so that the warehouse only contains clean, trustworthy data.

**Why this priority**: Data quality is the second-most-critical concern after basic ingestion. Bad data in the warehouse undermines all downstream analysis. This story ensures the warehouse acts as a quality gate.

**Independent Test**: Can be tested by crafting Parquet files with intentional schema violations (missing columns, wrong types, null required fields, out-of-range values) and verifying that valid records load while invalid records land in quarantine with clear error descriptions.

**Acceptance Scenarios**:

1. **Given** a Parquet file contains records with all required columns and valid types, **When** the file is ingested, **Then** all records pass validation and are loaded into the warehouse.
2. **Given** a Parquet file contains records with missing required columns, **When** the file is ingested, **Then** the entire file is rejected to quarantine with a descriptive error indicating which columns are missing.
3. **Given** a Parquet file contains individual records with null values in required fields, **When** the file is ingested, **Then** the invalid records are sent to quarantine and valid records from the same file are loaded normally.
4. **Given** a Parquet file contains records with values outside expected ranges (e.g., negative amounts, unknown currencies), **When** the file is ingested, **Then** those records are quarantined with a description of the validation failure.
5. **Given** records have been quarantined, **When** the user queries the quarantine area, **Then** they can see the original record data, the source file, the timestamp of the ingestion attempt, and the reason for rejection.

---

### User Story 3 - Deduplication by Transaction ID (Priority: P3)

As a data engineer, I want the pipeline to detect and skip duplicate transactions based on `transaction_id`, so that re-running the pipeline or ingesting overlapping files does not create duplicate records in the warehouse.

**Why this priority**: Deduplication ensures data integrity when the pipeline is run repeatedly or when source files overlap. It builds on top of basic ingestion (US1) and is critical for operational reliability but is separable from schema validation.

**Independent Test**: Can be tested by ingesting the same Parquet file twice and verifying that the record count in the warehouse does not change on the second run, and that duplicates are logged/reported.

**Acceptance Scenarios**:

1. **Given** a transaction with `transaction_id = "abc-123"` already exists in the warehouse, **When** a file containing a record with the same `transaction_id` is ingested, **Then** the duplicate record is skipped and a deduplication event is logged.
2. **Given** a single Parquet file contains two records with the same `transaction_id`, **When** the file is ingested, **Then** only one copy is loaded and the duplicate is logged.
3. **Given** the pipeline runs against a directory where all files have been previously ingested, **When** the pipeline completes, **Then** the total record count in the warehouse remains unchanged and the run summary reports the number of duplicates skipped.

---

### User Story 4 - Lineage Tracking (Priority: P4)

As a data engineer, I want the pipeline to record the provenance of every record (which source file it came from, when it was ingested, pipeline run ID), so that I can trace any record back to its origin for auditing and debugging.

**Why this priority**: Lineage is a foundational data governance capability that supports debugging, auditing, and compliance. It enriches the data model but does not block core ingestion or quality flows.

**Independent Test**: Can be tested by ingesting files and querying lineage metadata to confirm each record is tagged with its source file, ingestion timestamp, and run identifier. Verify that re-ingesting the same file under a new run produces a new lineage entry only for new records.

**Acceptance Scenarios**:

1. **Given** a Parquet file is ingested, **When** the user queries any loaded record, **Then** they can determine the source file name, ingestion timestamp, and pipeline run ID associated with that record.
2. **Given** multiple files are ingested across separate pipeline runs, **When** the user queries the lineage metadata, **Then** each run has a unique run ID and records are correctly associated with their respective runs.
3. **Given** a record was quarantined, **When** the user queries the quarantine area, **Then** the quarantined record also has lineage information (source file, run ID, ingestion timestamp).

---

### User Story 5 - Date-Based Partitioning (Priority: P5)

As a data engineer, I want the warehouse to organize transaction data by date (based on the transaction timestamp), so that queries filtering by date range are efficient and the data is logically structured for downstream consumers.

**Why this priority**: Partitioning is a performance and organizational optimization. It matters for query efficiency at scale but is not required for correctness. It can be layered on after the core ingestion pipeline is stable.

**Independent Test**: Can be tested by ingesting transactions spanning multiple dates and verifying that the data is organized by date in the warehouse, and that date-filtered queries perform efficiently against the partitioned structure.

**Acceptance Scenarios**:

1. **Given** a Parquet file contains transactions spanning multiple dates, **When** the file is ingested, **Then** the records are organized in the warehouse by their transaction date.
2. **Given** the warehouse contains partitioned data, **When** the user queries for a specific date range, **Then** only the relevant partitions are scanned (query performance scales with the date range, not total data volume).
3. **Given** a new file is ingested with transactions on a date that already has existing data, **When** the ingestion completes, **Then** the new records are appended to the correct date partition without disrupting existing data.

---

### Edge Cases

- What happens when a Parquet file is corrupted or unreadable? The system rejects the entire file to quarantine with an appropriate error message.
- What happens when the DuckDB database file is locked by another process? The system retries with backoff or reports a clear error to the user.
- What happens when disk space runs out during ingestion? The system fails gracefully, reporting the error, and does not leave the database in a corrupted state.
- What happens when a Parquet file has the correct column names but wrong data types (e.g., `amount` as string)? The file-level schema check catches this and quarantines the entire file.
- What happens when the same pipeline run is interrupted and restarted? The idempotent design ensures no duplicates; already-ingested records are skipped on retry.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST read Parquet files from a configurable source directory (defaulting to `data/raw/`).
- **FR-002**: System MUST load valid transaction records into a local DuckDB database file (defaulting to `data/warehouse/transactions.duckdb`).
- **FR-003**: System MUST create the DuckDB database and required tables automatically on first run if they do not exist.
- **FR-004**: System MUST validate each file's schema against the expected transaction schema (9 columns: `transaction_id`, `timestamp`, `amount`, `currency`, `merchant_name`, `category`, `account_id`, `transaction_type`, `status`).
- **FR-005**: System MUST validate individual records for null values in required fields and values within expected ranges.
- **FR-006**: System MUST reject files with schema-level errors (missing/extra columns, wrong data types) to quarantine as a whole file.
- **FR-007**: System MUST reject individual records failing value-level validation to quarantine while loading valid records from the same file.
- **FR-008**: System MUST store quarantined records with: original data, source file name, rejection reason, ingestion timestamp, and pipeline run ID.
- **FR-009**: System MUST deduplicate records by `transaction_id`, skipping any record whose `transaction_id` already exists in the warehouse.
- **FR-010**: System MUST handle within-file duplicates (same `transaction_id` appearing multiple times in one file), loading only the first occurrence.
- **FR-011**: System MUST be idempotent: re-running the pipeline with the same input files produces no change in the warehouse state.
- **FR-012**: System MUST tag every loaded record with lineage metadata: source file name, ingestion timestamp, and pipeline run ID.
- **FR-013**: System MUST generate a unique run ID for each pipeline execution.
- **FR-014**: System MUST organize warehouse data by transaction date to enable efficient date-range queries.
- **FR-015**: System MUST provide a CLI interface for running the pipeline, accepting optional parameters for source directory, database path, and output format.
- **FR-016**: System MUST produce a run summary at completion: files processed, records loaded, records quarantined, duplicates skipped, and elapsed time.
- **FR-017**: System MUST log pipeline activity (info, warning, error levels) to support operational monitoring.

### Key Entities

- **Transaction**: The core record being ingested. Contains 9 fields (transaction_id, timestamp, amount, currency, merchant_name, category, account_id, transaction_type, status) plus lineage metadata (source_file, ingested_at, run_id).
- **Quarantine Record**: A rejected record stored with original data, source file, rejection reason, ingestion timestamp, and run ID. Enables data quality investigation and potential reprocessing.
- **Pipeline Run**: A single execution of the ingestion pipeline, identified by a unique run ID. Tracks which files were processed, how many records were loaded/quarantined/deduplicated, and timing information.
- **Source File**: A Parquet file in the raw data directory. Tracked for lineage and idempotency (knowing which files have been processed).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All valid records from raw Parquet files are queryable in the warehouse after a single pipeline run, with zero data loss for conforming records.
- **SC-002**: Re-running the pipeline against already-ingested files produces zero new records and completes without errors (idempotency).
- **SC-003**: Records failing validation are captured in quarantine with sufficient detail (source file, reason, timestamp) to diagnose and resolve data quality issues within one inspection step.
- **SC-004**: Every record in the warehouse can be traced back to its source file and ingestion run within one query (lineage completeness).
- **SC-005**: Date-range queries against the warehouse scan only the relevant data partitions, not the entire dataset.
- **SC-006**: The pipeline processes 1 million transaction records in under 60 seconds on a standard development machine.
- **SC-007**: The pipeline run summary accurately reports: files processed, records loaded, records quarantined, duplicates skipped, and elapsed time.
- **SC-008**: The pipeline handles corrupted or unreadable files gracefully without crashing or corrupting the warehouse.

## Assumptions

- Raw Parquet files are produced by the Feature 001 generator and follow its output schema (9 columns as defined in `TransactionSchema.polars_schema()`).
- The file naming convention is `transactions_YYYYMMDD_HHMMSS.parquet` but the pipeline should accept any `.parquet` file in the source directory.
- The local DuckDB database is single-user (no concurrent write access from multiple pipeline instances).
- The pipeline runs on a single machine with sufficient disk space for the warehouse and quarantine data.
- Date-based partitioning uses the `timestamp` column's date component (UTC).
- The quarantine area is a separate table within the same DuckDB database (not a separate file system location).
