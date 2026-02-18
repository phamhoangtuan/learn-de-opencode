# Research: DuckDB Ingestion Pipeline

**Feature**: 002-duckdb-ingestion  
**Date**: 2026-02-18

## Research Areas

### 1. DuckDB as Local Analytical Warehouse

**Decision**: Use DuckDB as the embedded analytical database for the local warehouse.

**Rationale**: DuckDB is an in-process OLAP database that reads and writes Parquet natively, supports SQL, and requires zero infrastructure setup. It aligns perfectly with the project's local-first, single-user design. DuckDB's columnar storage engine is optimized for analytical queries and supports efficient date-range filtering. Its transactional writes (ACID) ensure database integrity even if the pipeline is interrupted.

**Alternatives considered**:
- **SQLite**: Excellent embedded database but optimized for OLTP, not analytical workloads. Poor performance on columnar scans and aggregations over large datasets. Lacks native Parquet support.
- **PostgreSQL**: Powerful but requires external server setup, which contradicts the zero-infrastructure local-first approach. Overkill for a learning project's warehouse layer.
- **Raw Parquet files with partitioning**: Would work for storage but lacks SQL query capabilities, transactional writes, and deduplication without additional tooling.

### 2. Ingestion Architecture: Polars Read + DuckDB Write

**Decision**: Use Polars to read and validate Parquet files, then use DuckDB's Python API to write validated data.

**Rationale**: Polars is already a project dependency (Feature 001) and excels at Parquet I/O and DataFrame operations. Using Polars for the read/validate stage maintains consistency with the existing codebase. DuckDB's Python API (`duckdb.sql()`) can directly ingest Polars DataFrames without serialization overhead via the zero-copy Arrow interface. This avoids adding heavy new dependencies while leveraging each tool's strengths.

**Alternatives considered**:
- **DuckDB-only (read Parquet directly)**: DuckDB can read Parquet natively, but validation logic (null checks, range checks, quarantine routing) is more naturally expressed in Polars DataFrame operations. Using DuckDB for both read and write would require complex SQL for validation that's harder to test and maintain.
- **Pandas**: Slower than Polars for large datasets, higher memory usage, and not already in the project's dependency tree.
- **Apache Arrow directly**: Too low-level for the validation logic needed. Would require significant boilerplate.

### 3. Schema Validation Strategy: Two-Tier Validation

**Decision**: Implement two-tier validation: file-level schema check (columns, types) followed by record-level value validation (nulls, ranges).

**Rationale**: Separating validation into two tiers matches the spec's quarantine behavior. File-level errors (missing columns, wrong types) indicate a fundamentally broken file that should be rejected entirely. Record-level errors (null values, out-of-range amounts) indicate individual bad records within an otherwise valid file. This distinction simplifies error handling and produces clearer quarantine diagnostics.

**Alternatives considered**:
- **Single-pass validation**: Check everything at the record level. This would be less efficient for files with structural problems (why scan 1M records if columns are missing?) and would produce confusing quarantine entries mixing structural and value errors.
- **External schema registry**: Tools like Great Expectations or Pandera could provide schema validation, but they add heavy dependencies for a straightforward 9-column schema. Custom validation with Polars expressions is lighter and sufficient.

### 4. Deduplication Strategy: Anti-Join on transaction_id

**Decision**: Use an anti-join between incoming records and existing warehouse `transaction_id` values to filter duplicates before insertion.

**Rationale**: An anti-join is the standard pattern for append-only deduplication. Load existing `transaction_id` values from DuckDB, perform a left anti-join in Polars to retain only new records, then insert. This is efficient (DuckDB can quickly return a set of existing IDs), correct (exactly-once semantics), and testable. For within-file duplicates, Polars `unique(subset="transaction_id", keep="first")` handles dedup before the anti-join.

**Alternatives considered**:
- **INSERT OR IGNORE with unique constraint**: Relies on the database to reject duplicates at insert time. Works but is slower for large batches (each row triggers a constraint check) and provides less visibility into how many duplicates were skipped.
- **Merge/Upsert (MERGE INTO)**: DuckDB supports this but it's designed for update-or-insert semantics, not append-only dedup. Adds unnecessary complexity.
- **Hash-based dedup (file checksums)**: Track which files have been ingested by checksum. Fast but doesn't handle partial re-ingestion or overlapping files with different names.

### 5. Lineage Metadata Approach: Inline Columns

**Decision**: Add three lineage columns directly to the transactions table: `source_file` (text), `ingested_at` (timestamp), `run_id` (text).

**Rationale**: Inline lineage columns are the simplest approach that satisfies the spec's requirement that "every record can be traced back to its source file and ingestion run within one query." No joins required, no separate lineage tables to maintain. The overhead is minimal (three additional columns per row) and the traceability is immediate.

**Alternatives considered**:
- **Separate lineage table**: A normalized `ingestion_runs` table joined by `run_id`. More relational, but adds complexity for a single-table warehouse. Would require a JOIN for every lineage query, violating the "within one query" success criterion if interpreted strictly.
- **File-level lineage only**: Track lineage at the file level (which files were ingested in which run) but not per-record. Insufficient because a file may be partially ingested (valid records loaded, invalid quarantined) and you need per-record traceability.

### 6. Date-Based Partitioning Strategy: Computed Column

**Decision**: Add a computed `transaction_date` column (DATE type extracted from `timestamp`) and use it for DuckDB table partitioning or as an indexed filter column.

**Rationale**: DuckDB does not support Hive-style partition directories like Spark/Trino. Instead, DuckDB optimizes columnar scans internally. Adding an explicit `transaction_date` DATE column enables efficient range predicates (`WHERE transaction_date BETWEEN ...`) which DuckDB's zone map optimization can accelerate. This provides the query performance benefit of partitioning without requiring external partitioning infrastructure.

**Alternatives considered**:
- **Hive-partitioned Parquet export**: Write warehouse data as partitioned Parquet files organized by date. This works for file-based consumers but defeats the purpose of having a queryable DuckDB warehouse. Better suited for a future "export" feature.
- **Separate tables per date**: Creates a table per date (e.g., `transactions_20260101`). Extremely hard to query across dates and manage. Anti-pattern.
- **No partitioning**: Rely on DuckDB's internal columnar optimization alone. Works for moderate scale but adding the explicit date column costs almost nothing and enables cleaner queries.
