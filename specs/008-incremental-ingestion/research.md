# Research Notes: Incremental Ingestion with Manifest Tracking

**Feature**: 008-incremental-ingestion  
**Date**: 2026-02-25  
**Author**: speckit.plan  

---

## 1. SHA-256 Hashing for Parquet Files in Python

### Approach: Streaming Raw Bytes with `hashlib`

The correct production pattern is to hash the raw file bytes *before* any
Parquet parsing. Clarification Q1 confirms this explicitly.

```python
import hashlib
from pathlib import Path

def compute_file_hash(path: Path, chunk_size: int = 65536) -> str:
    """SHA-256 over raw file bytes, streamed in 64 KB chunks."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()
```

**Why 64 KB chunks?**  
64 KB (65 536 bytes) is a common sweet spot: large enough to amortize
`read()` syscall overhead, small enough to avoid excessive memory pressure on
very large files. For typical Parquet files in this project (< 50 MB), even a
single-read approach would work, but streaming is the correct default for
correctness at any scale.

**Why raw bytes rather than DataFrame content?**

| Factor | Raw bytes | DataFrame content |
|--------|-----------|-------------------|
| Detects byte-level change (metadata, footer, encoding) | Yes | No |
| Independent of Polars/DuckDB version | Yes | No — parsed output may change across library versions |
| Avoids Parquet parse overhead for skipped files | Yes | No — must parse first |
| Deterministic across platforms | Yes | No — float precision, sort order |

**stdlib only, no extra dependencies**: `hashlib` is part of Python's standard
library. `hashlib.sha256()` is always available on CPython 3.11+.

**Performance**: For a 10 MB Parquet file, SHA-256 streaming takes approximately
25–50 ms on modern hardware (memory-bound, ~200–400 MB/s throughput). For
files that will be *skipped*, we avoid the hash entirely via the mtime
watermark pre-filter (FR-010).

---

## 2. DuckDB UPSERT Syntax

DuckDB (0.9+) supports standard SQL UPSERT via `INSERT ... ON CONFLICT DO UPDATE`.

### Syntax

```sql
INSERT INTO file_manifest (
    file_path, file_size_bytes, file_hash, file_mtime,
    run_id, processed_at, status, error_message
)
VALUES (?, ?, ?, ?, ?, ?::TIMESTAMPTZ, 'pending', NULL)
ON CONFLICT (file_path) DO UPDATE SET
    file_size_bytes = excluded.file_size_bytes,
    file_hash       = excluded.file_hash,
    file_mtime      = excluded.file_mtime,
    run_id          = excluded.run_id,
    processed_at    = excluded.processed_at,
    status          = 'pending',
    error_message   = NULL;
```

- `ON CONFLICT (file_path)` — triggers when the primary key already exists.
- `excluded.*` — refers to the values from the proposed INSERT row.
- This is equivalent to PostgreSQL's `ON CONFLICT DO UPDATE` (also called
  "upsert") and MySQL's `INSERT ... ON DUPLICATE KEY UPDATE`.

### Alternative: `INSERT OR REPLACE`

DuckDB also supports SQLite-style `INSERT OR REPLACE INTO ...` which
performs a DELETE + INSERT internally. **Avoid this** for manifest entries
because it changes the row's ROWID semantics and, more importantly, it would
destroy and recreate the row (rather than updating in place), making
`ON CONFLICT DO UPDATE` the semantically cleaner choice here.

### Watermark upsert (manifest_metadata)

```sql
INSERT INTO manifest_metadata (source_dir, watermark_mtime, updated_at)
VALUES (?, ?, NOW())
ON CONFLICT (source_dir) DO UPDATE SET
    watermark_mtime = excluded.watermark_mtime,
    updated_at      = excluded.updated_at;
```

### DuckDB version compatibility

`ON CONFLICT DO UPDATE` was introduced in DuckDB 0.7. The project already
uses DuckDB 0.10+, so there are no compatibility concerns.

---

## 3. Trade-offs: Hash-Based vs mtime-Based Change Detection

### mtime-based (file modification timestamp)

**Pros**:
- Zero I/O: reading `os.stat()` is a single syscall, no file content access.
- Extremely fast for large directories (O(1) per file).

**Cons**:
- **Not reliable for correctness**: mtime can be manipulated (`touch`, rsync
  with `--times`, copy preserving timestamps). A producer could replace a file
  with wrong data while preserving the original mtime.
- FAT/FAT32 filesystems have 2-second mtime granularity; NFS mtime can drift.
- `git checkout` and many ETL tools reset mtime to a commit or transfer time.

### hash-based (SHA-256 content digest)

**Pros**:
- **Cryptographically reliable**: two files with the same SHA-256 are
  identical with overwhelming probability (collision resistance).
- Detects byte-level changes regardless of filesystem metadata.
- Independent of copy/transfer tools.

**Cons**:
- Requires reading the entire file (I/O-intensive for large files).
- 25–50 ms per 10 MB file; scales linearly.

### Decision for this feature: hybrid (mtime pre-filter + hash confirmation)

The spec's FR-010 mandates a **watermark as a pre-filter**, not as the sole
detection mechanism. The implementation uses:

1. **Watermark (mtime)**: cheap first pass. Files with `mtime ≤ watermark` are
   almost certainly already processed — skip hash computation entirely.
2. **SHA-256 hash**: for files above the watermark (candidates), compute hash
   and compare against the manifest. This is the ground truth check.

This hybrid approach gives:
- Fast startup for runs where nothing has changed (no I/O beyond `os.stat()`).
- Correct detection of modified files even when mtime is preserved.
- No false negatives: a file below the watermark but modified in place would
  have a new mtime (file system updates mtime on write), so it would still be
  above the watermark on the next run.

**Edge case**: the watermark is set to the max mtime of successfully processed
files. A file modified in place always gets a new mtime from the OS (unless
explicitly faked), so it will naturally exceed the watermark.

---

## 4. Why `file_path` is the Right Primary Key (Not `file_name`)

### `file_name` is ambiguous

A file name like `transactions_20260101_120000.parquet` is unique within a
single directory but:
- If the source directory is ever reorganized into subdirectories (e.g.,
  `data/raw/2026/01/`), two files in different subdirectories could share
  the same name.
- Non-recursive discovery would not find subdirectory files anyway, but the
  manifest should be future-proof.

### `file_path` (relative to source_dir) is unambiguous

Storing the path relative to the configured `source_dir` (e.g.,
`transactions_20260101_120000.parquet` or `2026/01/transactions.parquet`)
uniquely identifies each file within the manifest scope.

**Relative vs absolute**: relative paths ensure the manifest is portable
across machines where the project root may differ (the spec's Assumptions
section explicitly states this requirement). The `source_dir` is a known
runtime parameter, so `source_dir / relative_path` always reconstructs the
full path.

**Primary key implications**: with `file_path` as PK, the manifest is a
current-state table (one row per file), which satisfies Clarification Q4
and enables efficient `ON CONFLICT DO UPDATE` semantics without needing a
surrogate key.

---

## 5. Two-Phase Commit Pattern (Pending Before, Success/Fail After)

This is a standard crash-recovery pattern for idempotent pipelines.

### Motivation

Without two-phase commit, a pipeline crash during file processing leaves
ambiguous state: was the file partially loaded? Fully loaded? Not at all?

### Pattern

**Phase 1 — Before ingestion**: write manifest entry with `status='pending'`
and current `run_id`. If the pipeline crashes after this, the entry is in the
database and the next run will see `status='pending'` (or more precisely,
since we only retry `failed` and new files, the entry will be updated to
`failed` by the crash-recovery logic, or left as `pending` to be detected
on the next run).

**Phase 2 — After ingestion**: update entry to `status='success'` or
`status='failed'`. This is the commit point.

**What happens if the pipeline crashes between phase 1 and phase 2?**  
The manifest entry remains at `status='pending'`. On restart, the pipeline
checks the manifest: a `pending` entry means the file was mid-flight. Per
FR-005's principle (retry failed files), `pending` files are treated as
needing reprocessing. The `_filter_files_by_manifest` function's `should_skip`
returns `False` for `pending` entries, so they are reprocessed.

**Why not a database transaction wrapping both phases?**  
DuckDB auto-commits by default. Wrapping both the manifest write and the
warehouse INSERT in a single transaction would prevent any data from being
written to the warehouse if the manifest write failed — which is the wrong
trade-off. The two-phase pattern is the correct approach for this
semi-transactional scenario.

### Comparison with industry patterns

| Pattern | Similarity |
|---------|-----------|
| Write-Ahead Log (WAL) | `pending` = WAL entry; `success/fail` = WAL commit |
| Two-phase commit (2PC) | Prepare phase = `pending`; Commit phase = `success` |
| Saga pattern | Each file is a saga step; manifest tracks compensation state |

---

## 6. Comparison with Other Manifest Implementations

### Spark `_SUCCESS` files

Spark writes a zero-byte `_SUCCESS` marker file in the output directory after
a job completes. Producers check for this file to confirm the job finished.

**Comparison**: `_SUCCESS` is a binary signal (present/absent). It carries
no metadata (no hash, no run ID, no per-file tracking). It is also
filesystem-coupled (not queryable via SQL). Our DuckDB manifest is strictly
superior for metadata richness and queryability.

### Delta Lake Transaction Log (`_delta_log/`)

Delta Lake maintains an ordered, append-only JSON log of all table mutations
(add/remove file operations). Each log entry includes file stats, schema, and
the operation type. The log is replay-able to reconstruct any historical table
version.

**Comparison**: Delta Lake's log is **append-only** (for auditability and
time-travel) whereas our manifest is **current-state** (one row per file,
latest state wins). Our design is simpler and appropriate for a file-tracking
manifest (not a data table mutation log). Historical run lineage is preserved
in `ingestion_runs`, not in the manifest — the correct separation of concerns.

Delta Lake also tracks data *inside* files (record counts, min/max per
column); our manifest tracks the file as a whole unit. This is appropriate
because our correctness guarantee is at the file level (ingest or skip), not
the record level (that's `dedup.py`'s job).

### dbt State (`manifest.json`)

dbt generates a `manifest.json` artifact after each run containing the
compiled DAG, node hashes, and execution results. The `dbt source freshness`
command checks source table modification times. dbt's `--state` flag enables
incremental model selection by comparing current manifest to a prior one.

**Comparison**: dbt's manifest is a *build artifact* (checked into CI),
whereas our manifest is a *runtime operational table* in DuckDB. dbt tracks
model (SQL transformation) state; we track raw file ingestion state. The
DuckDB table approach is queryable at any time without requiring a dbt CLI
invocation.

### Apache Hudi

Hudi maintains a timeline (`.hoodie/` directory) of commits as a
write-ahead log. Each commit records which files were written/replaced and
the record-level operation (insert/update/delete). Hudi supports both
copy-on-write (COW) and merge-on-read (MOR) storage strategies.

**Comparison**: Hudi is a full ACID table format for record-level
upserts on data lakes. Our manifest operates at the *file level* (not
record level) and uses DuckDB as the metadata store. Architecturally,
our manifest is analogous to Hudi's `.hoodie/` timeline applied to the
*source files* rather than the *output data files*.

### Summary table

| System | Granularity | Storage | Queryable | Append-only | Change detection |
|--------|-------------|---------|-----------|-------------|-----------------|
| Spark `_SUCCESS` | Job | Filesystem | No | Yes | None |
| Delta Lake `_delta_log` | File + Record | JSON files | Via `DESCRIBE HISTORY` | Yes | File path + size |
| dbt `manifest.json` | Model (SQL) | JSON file | No (CLI only) | No | Model hash |
| Hudi `.hoodie/` timeline | Record | Avro/JSON | Via Hudi CLI | Yes | Record key |
| **Our `file_manifest`** | File | DuckDB table | Yes (direct SQL) | No (current-state) | File path + SHA-256 |

Our approach is the simplest correct solution for the file-level ingestion
tracking problem: a SQL-queryable, current-state table with content-hash
change detection and run-level lineage via foreign key.
