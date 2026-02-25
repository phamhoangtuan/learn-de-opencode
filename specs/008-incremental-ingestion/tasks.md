# Tasks: Incremental Ingestion with Watermark and Manifest Tracking

**Input**: `specs/008-incremental-ingestion/spec.md`, `specs/008-incremental-ingestion/plan.md`, `specs/008-incremental-ingestion/checklists/ingestion.md`
**Branch**: `008-incremental-ingestion`
**Prerequisites**: plan.md (complete), spec.md (complete), checklists/ingestion.md (complete)

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to
- Paths are relative to the repository root

---

## Phase 1: Data Model & DDL

**Purpose**: Extend the DuckDB schema and Python models to support manifest tracking. These are foundational — all subsequent phases depend on them.

**⚠️ CRITICAL**: No manifest or pipeline integration work can begin until this phase is complete.

- [X] T001 [US1] Add `file_manifest` DDL to `src/ingestion/loader.py` — 8-column `CREATE TABLE IF NOT EXISTS` with `file_path VARCHAR PRIMARY KEY`, `file_size_bytes BIGINT NOT NULL`, `file_hash VARCHAR NOT NULL`, `file_mtime DOUBLE NOT NULL`, `run_id VARCHAR NOT NULL`, `processed_at TIMESTAMPTZ NOT NULL`, `status VARCHAR NOT NULL CHECK (status IN ('pending', 'success', 'failed'))`, `error_message VARCHAR` (CHK001, CHK002, FR-001, FR-002)
- [X] T002 [US5] Add `manifest_metadata` DDL to `src/ingestion/loader.py` — `CREATE TABLE IF NOT EXISTS manifest_metadata (source_dir VARCHAR PRIMARY KEY, watermark_mtime DOUBLE NOT NULL, updated_at TIMESTAMPTZ NOT NULL)` (CHK003, FR-009)
- [X] T003 [US1] In `src/ingestion/loader.py`: (a) update the baseline `CREATE TABLE IF NOT EXISTS ingestion_runs` DDL to include all four new columns (`files_checked INTEGER NOT NULL DEFAULT 0`, `files_skipped INTEGER NOT NULL DEFAULT 0`, `files_ingested INTEGER NOT NULL DEFAULT 0`, `files_failed INTEGER NOT NULL DEFAULT 0`) so fresh databases get them immediately; AND (b) add `ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS ...` migration guards in `create_tables()` for existing databases that already have the table without these columns (CHK005, FR-014, I-002 resolution)
- [X] T004 [P] [US4] Update `RunResult` dataclass in `src/ingestion/models.py` — add five new fields: `files_checked: int = 0`, `files_skipped: int = 0`, `files_ingested: int = 0`, `files_failed: int = 0`, `full_refresh: bool = False` (CHK064, FR-014)
- [X] T005 [US4] Update `RunResult.summary()` in `src/ingestion/models.py` — prepend `Mode: Incremental` or `Mode: Full refresh — manifest bypassed` line; add `Files checked`, `Files skipped`, `Files ingested`, `Files failed` lines (CHK065, CHK066, SC-007)

**Checkpoint**: Schema DDL and domain models are ready. Manifest module and pipeline integration can now begin.

---

## Phase 2: `manifest.py` Module

**Purpose**: Implement the `ManifestStore` class and `compute_file_hash` function in a new `src/ingestion/manifest.py` module. This module encapsulates all DuckDB interactions for manifest state and is a prerequisite for Phase 3.

- [X] T006 [US1] Create `src/ingestion/manifest.py` — define `ManifestEntry` dataclass with fields: `file_path: str`, `file_size_bytes: int`, `file_hash: str`, `file_mtime: float`, `run_id: str`, `processed_at: str`, `status: str`, `error_message: str | None = None` (plan.md ManifestEntry)
- [X] T007 [P] [US2] Implement `compute_file_hash(path: Path, chunk_size: int = 65536) -> str` in `src/ingestion/manifest.py` — streams raw bytes via `iter(lambda: f.read(chunk_size), b"")` through `hashlib.sha256`, returns `h.hexdigest()` (CHK007–CHK012, Clarification Q1)
- [X] T008 [US1] Implement `ManifestStore.__init__(self, conn: duckdb.DuckDBPyConnection)` and `create_tables(self) -> None` in `src/ingestion/manifest.py` — stores the shared connection; `create_tables()` executes DDL for both `file_manifest` and `manifest_metadata` (idempotent `CREATE TABLE IF NOT EXISTS`) (CHK004, FR-006)
- [X] T009 [US1] Implement `ManifestStore.upsert_pending(self, file_path: str, file_hash: str, file_size_bytes: int, file_mtime: float, run_id: str) -> None` — INSERT … ON CONFLICT DO UPDATE setting `status='pending'`, `run_id`, `file_hash`, `file_size_bytes`, `file_mtime`, `processed_at=NOW()`, `error_message=NULL` (CHK013–CHK015, FR-012, Clarification Q5)
- [X] T010 [US1] Implement `ManifestStore.mark_success(self, file_path: str) -> None` — UPDATE `file_manifest` SET `status='success'`, `processed_at=NOW()` WHERE `file_path=?` (CHK016, FR-012)
- [X] T011 [US1] Implement `ManifestStore.mark_failed(self, file_path: str, error_message: str) -> None` — UPDATE `file_manifest` SET `status='failed'`, `error_message=?`, `processed_at=NOW()` WHERE `file_path=?` (CHK017, FR-012)
- [X] T012 [US2] Implement `ManifestStore.should_skip(self, file_path: str, current_hash: str) -> bool` — returns `True` only when a row exists with `status='success'` AND `file_hash=current_hash`; returns `False` for `failed`, `pending`, changed hash, or missing row. **Note**: implement `get_manifest_entry(self, file_path: str) -> ManifestEntry | None` as a private helper inside this method (SELECT * FROM file_manifest WHERE file_path=?; return None if no row). This satisfies CHK032 and W-001. (CHK028–CHK032, FR-003, FR-004, FR-005)
- [X] T013 [US3] Implement `ManifestStore.reset_all_to_pending(self, run_id: str) -> None` — bulk UPDATE `file_manifest SET status='pending', run_id=?, error_message=NULL, processed_at=NOW()` (does NOT delete/reinsert) (CHK024, FR-008, Clarification Q3)
- [X] T014 [US5] Implement `ManifestStore.get_watermark(self, source_dir: str) -> float | None` and `ManifestStore.update_watermark(self, source_dir: str, mtime: float) -> None` — `get_watermark` returns `None` if no row; `update_watermark` uses INSERT … ON CONFLICT DO UPDATE upsert (CHK034, CHK038, FR-009)

**Checkpoint**: `ManifestStore` is fully implemented and independently testable. Unit tests (Phase 5) and pipeline integration (Phase 3) can now proceed.

---

## Phase 3: `pipeline.py` Integration

**Purpose**: Wire the manifest layer into the existing ingestion pipeline. Depends on Phase 1 (DDL + models) and Phase 2 (`manifest.py`).

- [X] T015 [US3] Add `full_refresh: bool = False` keyword parameter to `run_pipeline()` signature in `src/ingestion/pipeline.py` (CHK022, plan.md run_pipeline signature)
- [X] T016 [US1] Add `_filter_files_by_manifest(files, manifest, source_dir, full_refresh) -> tuple[list[tuple[Path, str]], int]` helper function to `src/ingestion/pipeline.py` — applies mtime watermark pre-filter (skip if `mtime <= watermark` and not `full_refresh`), computes `file_hash` for candidates, calls `manifest.should_skip()`, returns `(pending_list, files_skipped_count)` (CHK033, CHK035, CHK036, FR-010, plan.md _filter_files_by_manifest)
- [X] T016a [US1] In `_filter_files_by_manifest()`, exclude temporary/partial/hidden files before hashing — skip any file matching `*.tmp`, `*.partial`, or filenames starting with `.` (FR-015); log a warning for each excluded file (CHK035 adjacent, FR-015)
- [X] T017 [US1] Integrate manifest initialisation into `run_pipeline()` in `src/ingestion/pipeline.py` — after `create_tables(conn)`, instantiate `ManifestStore(conn)` and call `manifest.create_tables()`; if `full_refresh`, call `manifest.reset_all_to_pending(run_id)` and reset watermark; replace bare `files` list with `_filter_files_by_manifest()` call to get pending files and skip count (CHK023, CHK025, CHK026, FR-006, FR-007, FR-008)
- [X] T018 [US1] Add two-phase commit around `_process_file()` calls in `run_pipeline()` in `src/ingestion/pipeline.py` — call `manifest.upsert_pending(...)` before each file; call `manifest.mark_success(rel_path)` on success, `manifest.mark_failed(rel_path, error)` on failure; accumulate `files_ingested` and `files_failed` counters (CHK013, CHK016, CHK017, CHK018, FR-012)
- [X] T019 [US5] Advance watermark after successful run in `run_pipeline()` in `src/ingestion/pipeline.py` — after the file loop, call `manifest.update_watermark(str(source_dir), max_mtime)` where `max_mtime` is the maximum `st_mtime` across successfully ingested files; **only call `update_watermark()` if `max_mtime is not None`** (i.e., at least one file was successfully ingested this run — zero-ingestion runs leave the watermark unchanged); extend `complete_run()` call with new keyword args: `files_checked`, `files_skipped`, `files_ingested`, `files_failed` (CHK037, CHK068, FR-009, I-004 resolution, plan.md manifest stats)

**Checkpoint**: Full incremental pipeline is integrated. E2E runs produce manifest entries and skips on second run.

---

## Phase 4: CLI

**Purpose**: Expose `--full-refresh` to operators via the command-line entry point. Depends on T015 (run_pipeline signature).

- [X] T020 [US3] Add `--full-refresh` argument to `parse_args()` in `src/ingest_transactions.py` — `action="store_true"`, `default=False`, with descriptive help text referencing recovery and forced reload (CHK020, FR-007)
- [X] T021 [US3] Pass `full_refresh=args.full_refresh` through to `run_pipeline(...)` call in `main()` in `src/ingest_transactions.py` (CHK021, CHK044)

**Checkpoint**: CLI accepts `--full-refresh`. Running `uv run src/ingest_transactions.py --full-refresh` triggers full-refresh mode.

---

## Phase 5: Unit Tests

**Purpose**: Cover all `ManifestStore` methods and `compute_file_hash` with isolated in-memory DuckDB tests. Write these before verifying integration. Tests live in `tests/ingestion/test_manifest.py`.

- [ ] T022 [P] [US2] Create `tests/ingestion/test_manifest.py` — test `compute_file_hash()` determinism: same temp file path returns identical hex digest on two calls (CHK047)
- [ ] T023 [P] [US2] In `tests/ingestion/test_manifest.py` — test `compute_file_hash()` change detection: modifying one byte of a temp file produces a different hash (CHK048)
- [ ] T024 [P] [US1] In `tests/ingestion/test_manifest.py` — test `ManifestStore.create_tables()` creates both `file_manifest` and `manifest_metadata` tables; calling `create_tables()` a second time raises no error (CHK049)
- [ ] T025 [P] [US1] In `tests/ingestion/test_manifest.py` — test `upsert_pending()` on a new file path inserts a row with `status='pending'` and the correct `run_id` (CHK050)
- [ ] T026 [P] [US1] In `tests/ingestion/test_manifest.py` — test `upsert_pending()` on an existing file path performs an UPDATE in place (row count stays at 1, fields are refreshed, no duplicate rows) (CHK050)
- [ ] T027 [P] [US1] In `tests/ingestion/test_manifest.py` — test `mark_success()` sets `status='success'` and refreshes `processed_at` on the target row (CHK054)
- [ ] T028 [P] [US1] In `tests/ingestion/test_manifest.py` — test `mark_failed()` sets `status='failed'` and populates `error_message` (CHK055)
- [ ] T029 [P] [US2] In `tests/ingestion/test_manifest.py` — test `should_skip()` returns `True` only when a manifest entry exists with `status='success'` and the same hash (CHK051)
- [ ] T030 [P] [US2] In `tests/ingestion/test_manifest.py` — test `should_skip()` returns `False` for `status='failed'` (CHK052), `status='pending'` (CHK030), and a changed hash with `status='success'` (CHK053)
- [ ] T031 [P] [US3] In `tests/ingestion/test_manifest.py` — test `reset_all_to_pending()` bulk-updates all rows to `status='pending'` and clears `error_message`; verify no new rows are inserted (CHK057)
- [ ] T032 [P] [US5] In `tests/ingestion/test_manifest.py` — test `get_watermark()` returns `None` on an empty `manifest_metadata` table (CHK056)
- [ ] T033 [P] [US5] In `tests/ingestion/test_manifest.py` — test `update_watermark()` persists the value; subsequent `get_watermark()` call on a new connection to the same DB file returns the stored value (CHK056)

**Checkpoint**: All unit tests pass (`pytest tests/ingestion/test_manifest.py`).

---

## Phase 6: Integration Tests

**Purpose**: Verify end-to-end incremental behaviour using real Parquet files in a temp directory with a temp DuckDB file. Tests live in `tests/ingestion/test_pipeline_incremental.py`.

- [ ] T034 [US1] Create `tests/ingestion/test_pipeline_incremental.py` — test first run processes all files and writes manifest entries with `status='success'`; second run with no new files reports `files_ingested=0` and `files_skipped=N` (CHK059, US1 scenarios 1 & 3, SC-002)
- [ ] T035 [US1] In `tests/ingestion/test_pipeline_incremental.py` — test adding one new file after initial run: only the new file is ingested, prior files are skipped, manifest updated for new file (CHK060, US1 scenario 2)
- [ ] T036 [US2] In `tests/ingestion/test_pipeline_incremental.py` — test modifying a file in place (different byte content, same path) causes re-ingestion on next run; manifest entry updated with new hash and new `run_id` (CHK061, US2 scenario 1, SC-005)
- [ ] T037 [US3] In `tests/ingestion/test_pipeline_incremental.py` — test `full_refresh=True` reprocesses all files even when manifest has all entries as `success`; run summary states full refresh mode (CHK062, US3, SC-004)
- [ ] T038 [US1] In `tests/ingestion/test_pipeline_incremental.py` — test crash recovery: manually set a manifest entry to `status='pending'` (simulating a mid-run crash), verify next incremental run retries that file (CHK019, SC-006)
- [ ] T039 [US4] In `tests/ingestion/test_pipeline_incremental.py` — test referential integrity: every `file_manifest.run_id` joins to a valid `ingestion_runs` row; zero orphaned manifest records after two runs (CHK063, SC-003)
- [ ] T040 [US1] In `tests/ingestion/test_pipeline_incremental.py` — test `RunResult` counter accuracy: `files_checked`, `files_skipped`, `files_ingested`, `files_failed` sum to the total files discovered (SC-007)

**Checkpoint**: All integration tests pass (`pytest tests/ingestion/test_pipeline_incremental.py`).

---

## Phase 7: Verification

**Purpose**: Final correctness sweep — full test suite, linting, and E2E pipeline smoke test.

- [ ] T041 Run full test suite: `pytest` from `src/` — all tests pass including existing Feature 002 tests (CHK041, SC-008, FR-013)
- [ ] T042 Run ruff linter: `ruff check .` from `src/` — zero violations (CI gate parity)
- [ ] T043 Run E2E pipeline twice: `uv run src/ingest_transactions.py` followed by a second `uv run src/ingest_transactions.py` — verify second run summary reports skips (e.g., "Files skipped: N") and `files_ingested=0` in output (SC-001, SC-002)

**Checkpoint**: All phases complete. Feature 008 is production-ready.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (DDL & Models)**: No dependencies — start immediately
- **Phase 2 (manifest.py)**: Depends on Phase 1 DDL additions being in place for `create_tables()` to reference
- **Phase 3 (pipeline.py)**: Depends on Phase 1 (models) + Phase 2 (ManifestStore API)
- **Phase 4 (CLI)**: Depends on T015 (run_pipeline signature) from Phase 3
- **Phase 5 (Unit Tests)**: Depends on Phase 2 (ManifestStore methods fully implemented)
- **Phase 6 (Integration Tests)**: Depends on Phase 3 + Phase 4 (full pipeline wired)
- **Phase 7 (Verification)**: Depends on all prior phases complete

### Within-Phase Parallel Opportunities

- **Phase 1**: T004 (models.py) can run in parallel with T001–T003 (loader.py) — different files
- **Phase 2**: T007 (`compute_file_hash`) can be implemented in parallel with T008–T014 method stubs in the same file — but T009–T014 require T008 (`__init__` + `create_tables`) to be done first
- **Phase 5**: All unit tests (T022–T033) marked [P] can be written in parallel once Phase 2 is complete — they test different methods

### User Story Traceability

| User Story | Tasks |
|-----------|-------|
| US1 — Skip already-processed files | T001, T003, T008, T009, T010, T011, T012, T015, T016, T017, T018, T024–T031, T034, T035, T038–T040 |
| US2 — Hash-based change detection | T007, T012, T022, T023, T029, T030, T036 |
| US3 — Full-refresh CLI flag | T005, T013, T015, T017, T020, T021, T031, T037 |
| US4 — Manifest ↔ ingestion_runs lineage | T004, T005, T018, T039 |
| US5 — Watermark-based temporal ordering | T002, T014, T019, T032, T033 |

---

## Notes

- [P] tasks = different files or independent methods, no data dependencies
- Each task is one atomic unit of work: one function, one test, one DDL block, or one method
- Existing `validator.py`, `dedup.py`, and `loader.py` (except DDL/`complete_run` additions) are NOT modified — per plan.md "UNCHANGED" designations
- Test file locations follow the existing project convention: `tests/ingestion/` (parallel to `src/ingestion/`)
- CHK references map to `checklists/ingestion.md` for acceptance verification
