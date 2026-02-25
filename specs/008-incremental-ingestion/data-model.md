# Data Model: Incremental Ingestion Manifest

**Feature**: 008-incremental-ingestion  
**Date**: 2026-02-25  
**Warehouse**: `data/warehouse/transactions.duckdb`

---

## Overview

This feature introduces two new tables (`file_manifest` and
`manifest_metadata`) and extends the existing `ingestion_runs` table with
four manifest-level statistics columns. All other existing tables
(`transactions`, `quarantine`) are unchanged.

---

## Table: `file_manifest`

**Purpose**: Current-state tracking of every Parquet source file that has been
considered for ingestion. One row per file path; the row reflects the latest
ingestion attempt. Satisfies FR-001 through FR-016.

**Primary key**: `file_path` — relative path from the configured `source_dir`.

```sql
CREATE TABLE IF NOT EXISTS file_manifest (
    file_path        VARCHAR     PRIMARY KEY,
    file_size_bytes  BIGINT      NOT NULL,
    file_hash        VARCHAR     NOT NULL,
    file_mtime       DOUBLE      NOT NULL,
    run_id           VARCHAR     NOT NULL,
    processed_at     TIMESTAMPTZ NOT NULL,
    status           VARCHAR     NOT NULL
                     CHECK (status IN ('pending', 'success', 'failed')),
    error_message    VARCHAR                     -- NULL for success/pending entries
);
```

### Column Descriptions

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `file_path` | `VARCHAR` | NOT NULL | Relative path from `source_dir` root (e.g. `transactions_20260101_120000.parquet`). Primary key — one row per source file. |
| `file_size_bytes` | `BIGINT` | NOT NULL | File size in bytes at the time the manifest entry was last written. Used for informational purposes and future optimizations (e.g., estimated processing time). |
| `file_hash` | `VARCHAR` | NOT NULL | SHA-256 hex digest of the raw file bytes (64 hex characters). The ground-truth identity of the file's content. Change detection compares this value against the computed hash at pipeline startup. |
| `file_mtime` | `DOUBLE` | NOT NULL | File system modification timestamp as a Unix timestamp (seconds since epoch, float). Used for watermark pre-filter comparisons. Set from `os.stat(file).st_mtime` at the time the manifest entry is written. |
| `run_id` | `VARCHAR` | NOT NULL | Foreign key → `ingestion_runs.run_id`. Identifies which pipeline run last touched this entry (either the pending write or the final success/fail update). |
| `processed_at` | `TIMESTAMPTZ` | NOT NULL | UTC timestamp when this manifest entry was last written (either the `pending` UPSERT or the `success`/`failed` UPDATE). Not necessarily the same as `ingestion_runs.completed_at`. |
| `status` | `VARCHAR` | NOT NULL | Current processing state. One of: `pending` (file is being processed by the current run — crash recovery state), `success` (file was successfully ingested; will be skipped on next incremental run if hash is unchanged), `failed` (ingestion failed; will be retried on next run regardless of hash). |
| `error_message` | `VARCHAR` | NULL | Human-readable description of the failure for `status='failed'` entries. Also optionally populated for `status='success'` entries with partial quarantine (e.g., `"100 records quarantined"`). NULL for clean success entries. |

### Constraints and Indexes

- `file_path` is the PRIMARY KEY (implicit unique index, B-tree).
- `status` CHECK constraint enforces the enum values at the database level.
- No additional indexes are required for the expected query patterns (manifest
  lookups are by `file_path`, which is the PK).

### Status State Machine

```
                    ┌──────────────────────────────────┐
                    │                                  │
 [not in manifest]  │   upsert_pending()               │   mark_success()
         ──────────►│   pending  ──────────────────────►   success
                    │       │                          │
                    │       │   mark_failed()          │
                    │       └─────────────────────────►   failed
                    │                                  │
                    │   (next run: retry)              │
                    │   failed ──────► pending         │
                    │                                  │
                    │   (full-refresh: reset all)      │
                    │   success ─────► pending         │
                    └──────────────────────────────────┘
```

---

## Table: `manifest_metadata`

**Purpose**: Stores the high-water mark watermark per source directory. One row
per source directory. Used as a fast pre-filter to avoid hashing files that
were almost certainly already processed (FR-009, FR-010, US5).

**Primary key**: `source_dir` — canonical string form of the source directory path.

```sql
CREATE TABLE IF NOT EXISTS manifest_metadata (
    source_dir       VARCHAR     PRIMARY KEY,
    watermark_mtime  DOUBLE      NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL
);
```

### Column Descriptions

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `source_dir` | `VARCHAR` | NOT NULL | Canonical path to the source directory (e.g., `data/raw`). Primary key — one row per source directory. |
| `watermark_mtime` | `DOUBLE` | NOT NULL | Unix timestamp (float) of the maximum `file_mtime` across all files that have been successfully ingested from this source directory. On `--full-refresh`, reset to `NULL` by deleting the row or setting to `0.0`. Files with `mtime ≤ watermark_mtime` are skipped without hashing on incremental runs. |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL | UTC timestamp of the last watermark update. Informational only — used for operational inspection. |

---

## Table: `ingestion_runs` (extended)

**Purpose**: Existing table from Feature 002. Extended with four manifest-level
statistics columns to satisfy FR-014 and SC-007.

### New columns

```sql
ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS files_checked   INTEGER DEFAULT 0;
ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS files_skipped   INTEGER DEFAULT 0;
ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS files_ingested  INTEGER DEFAULT 0;
ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS files_failed    INTEGER DEFAULT 0;
```

### Full schema after extension

```sql
CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id              VARCHAR     PRIMARY KEY,
    started_at          TIMESTAMPTZ NOT NULL,
    completed_at        TIMESTAMPTZ,
    status              VARCHAR     NOT NULL,          -- 'running' | 'completed' | 'failed'
    files_processed     INTEGER     NOT NULL DEFAULT 0,  -- existing: total files touched
    records_loaded      INTEGER     NOT NULL DEFAULT 0,
    records_quarantined INTEGER     NOT NULL DEFAULT 0,
    duplicates_skipped  INTEGER     NOT NULL DEFAULT 0,
    elapsed_seconds     DOUBLE,
    -- NEW manifest statistics (Feature 008)
    files_checked       INTEGER     NOT NULL DEFAULT 0,  -- files discovered in source_dir
    files_skipped       INTEGER     NOT NULL DEFAULT 0,  -- files skipped by manifest
    files_ingested      INTEGER     NOT NULL DEFAULT 0,  -- files ingested this run
    files_failed        INTEGER     NOT NULL DEFAULT 0   -- files that failed ingestion
);
```

### Column Descriptions (new only)

| Column | Type | Description |
|--------|------|-------------|
| `files_checked` | `INTEGER` | Total number of Parquet files discovered in `source_dir` at the start of this run (before manifest filtering). Equals `files_skipped + files_ingested + files_failed`. |
| `files_skipped` | `INTEGER` | Number of files bypassed because they were already in the manifest with `status='success'` and an unchanged hash. On `--full-refresh` runs, this is always 0. |
| `files_ingested` | `INTEGER` | Number of files successfully processed (at least one record loaded; status set to `success` in manifest). |
| `files_failed` | `INTEGER` | Number of files where ingestion failed (I/O error, schema rejection, or unhandled exception; status set to `failed` in manifest). |

---

## Entity-Relationship Description

```
ingestion_runs (PK: run_id)
    │
    │ 1:N (one run processes many files)
    ▼
file_manifest (PK: file_path)
    file_manifest.run_id → ingestion_runs.run_id

manifest_metadata (PK: source_dir)
    [standalone — no FK, keyed by source directory path]

transactions (PK: transaction_id)
    transactions.run_id → ingestion_runs.run_id  [existing Feature 002 link]

quarantine
    quarantine.run_id → ingestion_runs.run_id    [existing Feature 002 link]
```

**Cardinality notes**:
- One `ingestion_run` → zero or more `file_manifest` entries (the entries
  whose `run_id` equals this run's ID).
- One `file_manifest` entry → exactly one `ingestion_run` (the latest run
  that touched this file). Historical linkage is available by cross-referencing
  `transactions.run_id` for a given file (via `transactions.source_file`).
- `manifest_metadata` is independent of per-run linkage; it is a global
  operational table keyed by source directory.

---

## Referential Integrity

DuckDB does not enforce FOREIGN KEY constraints at the DML level by default
(they are documented but not enforced). The application layer maintains
referential integrity by:

1. **Always creating the `ingestion_runs` record first** (`create_run()` is
   called before any manifest writes).
2. **Writing manifest entries only within a live run** (`upsert_pending()`
   uses the current `run_id` which is guaranteed to exist in `ingestion_runs`).
3. **SC-003** (zero orphaned manifest records) is maintained by design
   constraint, not FK enforcement.

---

## Sample Data

### Scenario: First run (3 files, all new)

**Before run** (tables empty):

`file_manifest`:
*(empty)*

`manifest_metadata`:
*(empty)*

**After run** (`run_id = abc123`):

`file_manifest`:

| file_path | file_size_bytes | file_hash | file_mtime | run_id | processed_at | status | error_message |
|-----------|----------------|-----------|------------|--------|--------------|--------|---------------|
| `transactions_20260101_120000.parquet` | 524288 | `a3f2...8b4c` | 1767225600.0 | `abc123` | `2026-01-01 12:05:00+00` | `success` | NULL |
| `transactions_20260102_120000.parquet` | 512000 | `d1e9...2f7a` | 1767312000.0 | `abc123` | `2026-01-01 12:05:01+00` | `success` | NULL |
| `transactions_20260103_120000.parquet` | 540672 | `7b3c...9e1d` | 1767398400.0 | `abc123` | `2026-01-01 12:05:02+00` | `success` | NULL |

`manifest_metadata`:

| source_dir | watermark_mtime | updated_at |
|------------|----------------|------------|
| `data/raw` | 1767398400.0 | `2026-01-01 12:05:03+00` |

`ingestion_runs`:

| run_id | started_at | completed_at | status | files_checked | files_skipped | files_ingested | files_failed | files_processed | records_loaded |
|--------|------------|--------------|--------|--------------|--------------|----------------|--------------|----------------|----------------|
| `abc123` | `2026-01-01 12:05:00+00` | `2026-01-01 12:05:03+00` | `completed` | 3 | 0 | 3 | 0 | 3 | 3000 |

---

### Scenario: Second incremental run (no new files)

**After run** (`run_id = def456`):

`file_manifest`: *(unchanged — all 3 rows still show `run_id = abc123` because no file was touched)*

`ingestion_runs` (new row added):

| run_id | started_at | completed_at | status | files_checked | files_skipped | files_ingested | files_failed |
|--------|------------|--------------|--------|--------------|--------------|----------------|--------------|
| `abc123` | ... | ... | `completed` | 3 | 0 | 3 | 0 |
| `def456` | `2026-01-01 13:00:00+00` | `2026-01-01 13:00:00+00` | `completed` | 3 | 3 | 0 | 0 |

---

### Scenario: Third run — one file modified, one file new

A producer overwrites `transactions_20260101_120000.parquet` with corrected
data (new hash), and adds `transactions_20260104_120000.parquet` (new file).

**After run** (`run_id = ghi789`):

`file_manifest`:

| file_path | file_hash | run_id | status |
|-----------|-----------|--------|--------|
| `transactions_20260101_120000.parquet` | `NEW_HASH_5f2a...` | `ghi789` | `success` |
| `transactions_20260102_120000.parquet` | `d1e9...2f7a` | `abc123` | `success` |
| `transactions_20260103_120000.parquet` | `7b3c...9e1d` | `abc123` | `success` |
| `transactions_20260104_120000.parquet` | `c8f1...4d0b` | `ghi789` | `success` |

Note: `transactions_20260101` now shows `run_id = ghi789` (updated in place
because its hash changed). Files 02 and 03 retain `run_id = abc123` (unchanged).

---

### Scenario: Pipeline crash mid-run (file 2 was in-flight)

After crash, manifest shows:

| file_path | run_id | status | error_message |
|-----------|--------|--------|---------------|
| `transactions_20260101_120000.parquet` | `crashed_run` | `success` | NULL |
| `transactions_20260102_120000.parquet` | `crashed_run` | `pending` | NULL |
| `transactions_20260103_120000.parquet` | `abc123` | `success` | NULL |

On next restart: file 02 has `status='pending'` → treated as unprocessed,
re-attempted. File 01 is `success` + same hash → skipped. File 03 is
`success` + same hash → skipped.

---

### Scenario: `--full-refresh` run

Before full-refresh, all 3 files are `success`. Running with `--full-refresh`
resets all to `pending` via bulk UPDATE:

| file_path | run_id | status |
|-----------|--------|--------|
| `transactions_20260101_120000.parquet` | `refresh_run` | `pending` |
| `transactions_20260102_120000.parquet` | `refresh_run` | `pending` |
| `transactions_20260103_120000.parquet` | `refresh_run` | `pending` |

After run completes, all are updated to `success` with `run_id = refresh_run`.
The `manifest_metadata` watermark is also reset (set to `0.0` or the row is
deleted) before processing begins, then updated to the new max mtime at the end.
