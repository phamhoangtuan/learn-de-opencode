# Feature Specification: Incremental Ingestion with Watermark and Manifest Tracking

**Feature Branch**: `008-incremental-ingestion`  
**Created**: 2026-02-25  
**Status**: Draft  
**Input**: User description: "Incremental ingestion with watermark/manifest system. Currently every pipeline run re-scans all Parquet files in data/raw/ and relies on deduplication to prevent duplicates. The new feature should track which files have been processed (via a manifest table in DuckDB) so only new/unprocessed files are ingested on subsequent runs. This is a fundamental production pattern."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Skip Already-Processed Files on Subsequent Runs (Priority: P1)

As a data engineer, I want the ingestion pipeline to consult a file manifest before processing any file, so that only new, unprocessed Parquet files are ingested on each run — eliminating redundant re-scanning and deduplication overhead.

**Why this priority**: This is the core value proposition of incremental ingestion. Without manifest-based file tracking, every run is a full scan that wastes compute and relies on downstream deduplication as the only correctness guard. Solving file-level tracking alone delivers the MVP of this feature.

**Independent Test**: Can be fully tested by ingesting a set of files, confirming they are recorded in the manifest, then adding one new file and re-running — verifying that only the new file is processed while the previously-ingested files are skipped.

**Acceptance Scenarios**:

1. **Given** three Parquet files exist in `data/raw/` and have all been ingested in a prior run, **When** the pipeline runs again with no new files added, **Then** zero files are processed, the run summary reports "0 files processed (3 skipped — already in manifest)", and the warehouse record count is unchanged.
2. **Given** three Parquet files have been previously ingested and one new file is added to `data/raw/`, **When** the pipeline runs, **Then** only the new file is ingested, the three prior files are skipped, and the manifest is updated with the new file's entry.
3. **Given** no manifest table exists yet (first ever run), **When** the pipeline runs, **Then** the manifest table is created automatically, all files are processed as new, and each file's entry is written to the manifest upon successful ingestion.
4. **Given** the DuckDB database does not yet exist, **When** the pipeline runs for the first time, **Then** the database, warehouse tables, and manifest table are all created automatically.

---

### User Story 2 - File Manifest with Hash-Based Change Detection (Priority: P2)

As a data engineer, I want the manifest to record each processed file's content hash (MD5 or SHA-256) and file size so that if a source file is replaced or modified in place, the pipeline detects the change and re-ingests the updated file.

**Why this priority**: File path alone is insufficient for change detection — a file may be silently overwritten with corrected data. Content hashing ensures the manifest reflects actual file content, not just names. This prevents silent data staleness without requiring a full-refresh.

**Independent Test**: Can be tested by ingesting a file, modifying its content (simulating a correction), re-running the pipeline, and verifying the file is re-processed because its hash changed, and the manifest entry is updated with the new hash and a new ingestion run ID.

**Acceptance Scenarios**:

1. **Given** a file `transactions_20260101.parquet` is in the manifest with hash `abc123`, **When** the file at that path is replaced with new content (hash `def456`), **Then** on the next pipeline run the file is detected as changed and re-ingested, and the manifest entry is updated with the new hash, new run ID, and updated `processed_at` timestamp.
2. **Given** a file exists at the same path and has the same hash as its manifest entry, **When** the pipeline runs, **Then** the file is skipped without re-ingestion.
3. **Given** a file is listed in the manifest with status `failed`, **When** the pipeline runs, **Then** the file is retried regardless of whether its hash has changed (failed files are always re-attempted).

---

### User Story 3 - Full-Refresh Mode via CLI Flag (Priority: P3)

As a data engineer, I want to run the pipeline with a `--full-refresh` flag that bypasses the manifest and reprocesses all files in the source directory, so that I can recover from a corrupted warehouse or manifest, or force a clean reload.

**Why this priority**: Incremental mode is the default, but operators must have an escape hatch for recovery and backfill scenarios. Full-refresh is a standard production pattern required for correctness guarantees when the warehouse state is uncertain.

**Independent Test**: Can be tested by running the pipeline normally (files recorded in manifest), then running with `--full-refresh`, and verifying all files are reprocessed, the manifest entries are reset, and the warehouse reflects the full dataset.

**Acceptance Scenarios**:

1. **Given** all files have been ingested and recorded in the manifest, **When** the user runs the pipeline with `--full-refresh`, **Then** all files in `data/raw/` are reprocessed regardless of their manifest status, and the manifest entries are reset/updated for all files.
2. **Given** the warehouse is empty but the manifest has entries (e.g., after a manual warehouse drop), **When** the user runs with `--full-refresh`, **Then** all files are reprocessed and the warehouse is repopulated correctly.
3. **Given** the pipeline is run with `--full-refresh`, **When** the run completes, **Then** the run summary clearly states "Full refresh mode — manifest bypassed, N files processed."
4. **Given** the pipeline is run without `--full-refresh` (default incremental mode), **When** new and existing files are present, **Then** only files not in the manifest (or with changed hashes) are processed.

---

### User Story 4 - Manifest Metadata Integration with Ingestion Runs (Priority: P4)

As a data engineer, I want the file manifest to link each entry to the `ingestion_runs` metadata table via `run_id`, so that I can audit which pipeline run processed which files and query the full lineage from file to warehouse record.

**Why this priority**: Manifest entries without run-level context are incomplete for lineage and auditing purposes. Connecting the manifest to `ingestion_runs` completes the audit trail required by the constitution's Metadata & Lineage principle (DAMA-DMBOK Area 10).

**Independent Test**: Can be tested by running two incremental pipeline runs (each processing different files), querying the manifest joined to `ingestion_runs`, and confirming each manifest entry's `run_id` resolves to a valid run record with correct timing and status.

**Acceptance Scenarios**:

1. **Given** a pipeline run processes two files, **When** the run completes, **Then** both manifest entries have `run_id` values that match a row in the `ingestion_runs` table with status `completed`.
2. **Given** a pipeline run fails mid-way through processing, **When** the user queries the manifest, **Then** files successfully processed in that run have status `success`, files that failed have status `failed`, and the `run_id` links to the `ingestion_runs` record with status `failed`.
3. **Given** the manifest is queried over multiple runs, **When** joined to `ingestion_runs`, **Then** the user can reconstruct the full history: which run processed each file, when, and what the outcome was.

---

### User Story 5 - Watermark-Based Temporal Ordering (Priority: P5)

As a data engineer, I want the pipeline to track a high-water mark representing the latest file modification timestamp seen across all processed files, so that new files can be identified by file system modification time as a fast pre-filter before hash comparison.

**Why this priority**: File hash computation is I/O-intensive for large files. A modification-time watermark provides a cheap first-pass filter: files modified before the watermark are almost certainly already processed, allowing the hash check to be reserved for ambiguous cases. This optimizes pipeline startup time at scale.

**Independent Test**: Can be tested by ingesting a set of files, recording the watermark, then adding a new file with a newer modification time — verifying the pipeline processes only files newer than the watermark (plus hash verification for exact matches at the boundary).

**Acceptance Scenarios**:

1. **Given** five files have been ingested and the watermark is set to the latest mtime among them, **When** a new file with `mtime > watermark` is added, **Then** the pipeline identifies it as a candidate for ingestion via watermark filter and processes it.
2. **Given** no files have a modification time newer than the current watermark, **When** the pipeline runs, **Then** all files are pre-filtered as already processed and the run completes with zero files ingested.
3. **Given** the watermark is stored in the manifest metadata table, **When** the pipeline starts, **Then** it reads the watermark from the database (not from in-memory state), ensuring correctness across restarts.

---

### Edge Cases

- What happens when a file is deleted from `data/raw/` after being recorded in the manifest? The manifest retains the historical entry with status `success`; the deleted file is not re-processed or flagged as an error on subsequent runs.
- What happens when a file is partially written (still being written by a producer) when the pipeline runs? The pipeline should detect incomplete files via file size stability check or rely on a file-naming convention (e.g., `.tmp` suffix exclusion); incomplete files are skipped with a warning.
- What happens when the manifest table itself is corrupted or missing? The pipeline falls back to full-refresh mode for that run, logs a warning, and recreates the manifest table from scratch.
- What happens when two pipeline instances run concurrently against the same manifest? The manifest must use DuckDB's transactional semantics to prevent race conditions; concurrent runs on a single DuckDB file are serialized by DuckDB's single-writer model.
- What happens when disk space runs out while writing the manifest? The pipeline fails the current run gracefully without corrupting the manifest or warehouse; the failed file's manifest entry is marked `failed`.
- What happens when `--full-refresh` is combined with a very large number of files? The pipeline processes all files but does so in batches to avoid memory exhaustion; each batch updates the manifest atomically.
- What happens when the source directory path changes between runs? The manifest uses relative file paths; if the base directory changes, the operator must use `--full-refresh` to reprocess under the new path.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST maintain a `file_manifest` table in the DuckDB warehouse (`data/warehouse/transactions.duckdb`) to track the ingestion status of every Parquet file.
- **FR-002**: System MUST record the following attributes per manifest entry: `file_path` (relative to source directory), `file_size_bytes`, `file_hash` (SHA-256), `file_mtime` (file system modification timestamp), `run_id` (foreign key to `ingestion_runs`), `processed_at` (UTC timestamp), `status` (`pending` | `success` | `failed`), `error_message` (nullable).
- **FR-003**: System MUST skip files whose `file_path` AND `file_hash` already appear in the manifest with status `success`, without reading the file's contents.
- **FR-004**: System MUST re-ingest files whose `file_hash` has changed relative to their manifest entry (content change detection).
- **FR-005**: System MUST re-ingest files whose manifest entry has status `failed`, regardless of hash.
- **FR-006**: System MUST create the `file_manifest` table automatically on first run if it does not exist.
- **FR-007**: System MUST support a `--full-refresh` CLI flag that bypasses the manifest check and reprocesses all files in the source directory.
- **FR-008**: When `--full-refresh` is used, the system MUST reset manifest entries for all files (update status to `pending`) before beginning ingestion, then update each entry upon completion.
- **FR-009**: System MUST maintain a watermark representing the maximum `file_mtime` across all successfully processed files, stored in a `manifest_metadata` table keyed by source directory.
- **FR-010**: System MUST use the watermark as a pre-filter to exclude files modified before the watermark from hash computation, improving startup performance.
- **FR-011**: System MUST link every `file_manifest` entry to the `ingestion_runs` table via `run_id`.
- **FR-012**: System MUST update the manifest entry atomically: the entry is written with status `pending` before ingestion begins, updated to `success` or `failed` upon completion.
- **FR-013**: System MUST be backward compatible with the existing Feature 002 ingestion pipeline; incremental mode is the new default, and all existing pipeline behaviors (validation, deduplication, quarantine, lineage) remain unchanged.
- **FR-014**: System MUST include file manifest statistics in the run summary: files checked, files skipped (already processed), files ingested, files failed.
- **FR-015**: System MUST exclude files matching patterns `*.tmp`, `*.partial`, and hidden files (starting with `.`) from manifest consideration.
- **FR-016**: System MUST expose the manifest as a queryable DuckDB table for operational inspection without requiring a separate tool.

### Key Entities

- **FileManifest**: Tracks the processing state of each source Parquet file. Attributes: `file_path`, `file_size_bytes`, `file_hash` (SHA-256), `file_mtime`, `run_id`, `processed_at`, `status`, `error_message`. The combination of `file_path` is the primary key (latest entry per path wins).
- **ManifestMetadata**: A key-value metadata table storing the high-water mark watermark per source directory. Attributes: `source_dir`, `watermark_mtime`, `updated_at`. Used to accelerate file discovery on subsequent runs.
- **IngestionRun**: Existing entity from Feature 002. Extended with manifest-level statistics: `files_checked`, `files_skipped`, `files_ingested`, `files_failed`.
- **SourceFile**: A Parquet file in the raw data directory. Identified by relative path. Carries file system metadata (size, mtime) and content identity (SHA-256 hash) that are recorded in the FileManifest.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A pipeline run against a directory where all files have been previously processed completes in under 5 seconds regardless of the number of files, because file contents are not read (manifest short-circuits).
- **SC-002**: After an initial full-ingest run, a subsequent run adding one new file results in exactly 1 file ingested and N-1 files skipped, with no duplicates introduced into the warehouse.
- **SC-003**: Every `file_manifest` entry can be joined to `ingestion_runs` via `run_id` with zero orphaned manifest records (referential integrity).
- **SC-004**: Running the pipeline with `--full-refresh` produces a warehouse state equivalent to running from scratch against the same source files (idempotent full-refresh).
- **SC-005**: A file modified in place (same path, different content) is detected and re-ingested on the next incremental run without requiring `--full-refresh`.
- **SC-006**: The manifest survives a pipeline crash mid-run: entries for files that completed before the crash are marked `success`, entries for in-flight files are marked `failed`, and the pipeline correctly retries only failed/unprocessed files on restart.
- **SC-007**: The run summary accurately reports: files checked, files skipped, files ingested, files failed, and whether the run was incremental or full-refresh.
- **SC-008**: All existing Feature 002 acceptance criteria (schema validation, deduplication, quarantine, lineage) continue to pass without modification after this feature is implemented.

## Assumptions

- The DuckDB warehouse file is single-user; no concurrent write access from multiple pipeline instances is expected (enforced by DuckDB's single-writer model).
- Parquet files in `data/raw/` follow the naming convention `transactions_YYYYMMDD_HHMMSS.parquet` but the manifest accepts any `.parquet` file.
- SHA-256 hashing of the entire file is acceptable for change detection (files are not so large that hashing is prohibitively slow for new/changed files).
- File paths in the manifest are stored relative to the configured source directory to support portability across machines.
- The `ingestion_runs` table from Feature 002 remains the authoritative record of pipeline executions; the manifest is a subordinate tracking table that references it.
- `--full-refresh` does not drop and recreate the warehouse tables; it only resets manifest entries and re-ingests files (deduplication logic still applies to warehouse records).
- The watermark is reset to `NULL` on `--full-refresh` to force re-evaluation of all file mtimes.

## Clarifications

### Q1: Is the SHA-256 hash computed over raw file bytes or over parsed DataFrame content?

**Ambiguity**: FR-002 specifies `file_hash (SHA-256)` and FR-003 says skip files without "reading the file's contents," but hash computation inherently requires reading the file. It is unclear whether the hash is computed by streaming raw bytes (before any Parquet parse) or by hashing the serialized DataFrame after parsing.

**Resolution**: The hash MUST be computed over the **raw bytes** of the file on disk (e.g., `hashlib.sha256` streaming the file in binary mode), before any Parquet parsing occurs. This is the correct production pattern because: (a) it catches any byte-level change including metadata, encoding changes, and schema evolution; (b) it avoids the overhead of parsing every candidate file just to compute a hash; (c) it is deterministic and independent of Polars/DuckDB versions. The manifest skip check (FR-003) means the pipeline reads file *metadata* (size, mtime, path) and the raw bytes only for hashing — it does NOT parse the Parquet into a DataFrame for files that will be skipped.

---

### Q2: What manifest `status` is written when a file is only partially ingested (some records valid, some quarantined)?

**Ambiguity**: FR-002 defines `status` as `pending | success | failed` with no `partial` state. However, the existing pipeline (Feature 002) already supports files where some records pass validation and are loaded while others are quarantined. It is unclear whether a file with 900 valid records loaded and 100 quarantined should be recorded as `success` or `failed`.

**Resolution**: A file MUST be recorded with status `success` if at least one record was successfully loaded into the warehouse, regardless of how many records were quarantined. Status `failed` is reserved exclusively for: (a) files that could not be read at all (I/O error, corrupted Parquet), (b) files that failed file-level schema validation entirely (all records rejected), or (c) files where an unhandled exception occurred during processing. This aligns with the existing Feature 002 behavior where partial ingestion is a normal, expected outcome (quarantine handles the invalid subset). The `error_message` column may optionally record a summary like "100 records quarantined" for `success` entries with quarantined records.

---

### Q3: Does `--full-refresh` preserve manifest history or overwrite it in place?

**Ambiguity**: FR-008 says "reset manifest entries for all files (update status to `pending`)" and the Assumptions section says "reset manifest entries." US3 scenario 1 says "manifest entries are reset/updated for all files." It is unclear whether the reset is an in-place UPDATE of the existing row (destroying history), a DELETE + re-insert, or the insertion of new rows (preserving prior run history).

**Resolution**: `--full-refresh` MUST perform an **in-place UPDATE** of existing manifest rows (setting `status = 'pending'`, `run_id` to the current run ID, clearing `error_message`) rather than deleting or inserting new rows. This is consistent with the `file_manifest` being a **current-state table** (one row per file path, latest state wins — as stated in the Key Entities section: "latest entry per path wins"). Historical run-level lineage is preserved in `ingestion_runs` via the `run_id` foreign key. Files with no existing manifest entry are handled identically to their first-time ingestion (INSERT with `pending`). After all files complete, each entry is updated to `success` or `failed` per FR-012.

---

### Q4: Is `file_manifest` an append-only log or a current-state table (one row per file)?

**Ambiguity**: The Key Entities section states "latest entry per path wins" (implying upsert / current-state), but FR-012 says the entry is "written with status `pending` before ingestion begins, updated to `success` or `failed` upon completion" (implying in-place update of a single row). SC-003 requires every entry to join to `ingestion_runs` with "zero orphaned manifest records," which is easier to enforce with a current-state model. However, US4 scenario 3 says the user can "reconstruct the full history: which run processed each file" — which seems to require a log.

**Resolution**: `file_manifest` MUST be a **current-state table** with exactly one row per `file_path` (the latest processing state). The primary key is `file_path`. Run-level history is NOT stored in the manifest itself — it is available by querying `ingestion_runs` joined on `run_id`. When a file is reprocessed (due to hash change, `failed` retry, or `--full-refresh`), its manifest row is updated in place with the new `run_id`, `file_hash`, `processed_at`, and `status`. This design: (a) keeps the manifest compact and O(files) not O(runs × files); (b) satisfies SC-003 (one row per file = one `run_id` per row, trivially non-orphaned); (c) is consistent with FR-008's "reset" semantics. Full audit history across runs is reconstructable by inspecting `ingestion_runs` directly.

---

### Q5: How does the manifest `run_id` get assigned when the pending entry is written before ingestion begins?

**Ambiguity**: FR-012 says "the entry is written with status `pending` before ingestion begins" and must be linked to the current `run_id`. In the existing pipeline (`pipeline.py`), `run_id` is generated at the start of `run_pipeline()` before any file processing. However, for a current-state table with one row per file (Clarification Q4), it is unclear whether the pending write is an INSERT (first time) or an UPSERT/UPDATE (subsequent runs), and whether the `run_id` is set to the current run's ID at the pending stage or only at completion.

**Resolution**: The manifest write sequence for each file MUST be: (1) **UPSERT** the manifest entry with `status = 'pending'`, `run_id = <current_run_id>`, `file_hash = <computed_hash>`, `file_size_bytes`, `file_mtime`, and `processed_at = NOW()` **before** calling the ingestion logic for that file; (2) **UPDATE** the same row to `status = 'success'` or `status = 'failed'` (and populate `error_message` if failed) **after** ingestion completes. Using the current `run_id` at the pending stage ensures that if the pipeline crashes mid-file, the manifest entry correctly points to the run that was attempting the file, and the `ingestion_runs` record for that run will show `failed` status — satisfying SC-006. The UPSERT pattern (INSERT OR REPLACE / INSERT ... ON CONFLICT DO UPDATE) is used because the file may or may not have a prior manifest entry.
