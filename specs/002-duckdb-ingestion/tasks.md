# Tasks: DuckDB Ingestion Pipeline

**Input**: Design documents from `/specs/002-duckdb-ingestion/`  
**Prerequisites**: plan.md (required), spec.md (required), research.md, data-model.md, quickstart.md

**Tests**: Included — the spec requires comprehensive testing (unit, integration, quality tiers matching Feature 001's pattern).

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

---

## Phase 1: Setup

**Purpose**: Project initialization, dependencies, and directory structure

- [x] T001 Create `src/ingestion/` module directory with `__init__.py`
- [x] T002 [P] Add `duckdb` dependency to `pyproject.toml` and run `uv sync`
- [x] T003 [P] Create `data/warehouse/` directory and add to `.gitignore` if not already excluded
- [x] T004 [P] Create test fixture files in `tests/conftest.py` for ingestion tests (temp DuckDB, sample DataFrames, temp Parquet files)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

- [x] T005 Implement ingestion domain models (RunResult, RunStatus, ValidationResult) in `src/ingestion/models.py`
- [x] T006 [P] Implement DuckDB connection manager and table creation (transactions, quarantine, ingestion_runs) in `src/ingestion/loader.py`
- [x] T007 [P] Implement expected schema definition (9 source columns + types + validation rules) in `src/ingestion/validator.py`
- [x] T008 Write unit tests for domain models in `tests/unit/test_ingestion_models.py`
- [x] T009 [P] Write unit tests for DuckDB table creation and connection in `tests/unit/test_loader.py`
- [x] T010 [P] Write unit tests for schema definition constants in `tests/unit/test_validator.py`

**Checkpoint**: Foundation ready — database schema, domain models, and validation rules established. User story implementation can now begin.

---

## Phase 3: User Story 1 — Ingest Raw Transaction Files (Priority: P1) MVP

**Goal**: Run a single command that reads all raw Parquet files from `data/raw/` and loads them into a local DuckDB database.

**Independent Test**: Generate sample Parquet files, run the ingestion command, query DuckDB to confirm all records are present and queryable.

### Tests for User Story 1

- [ ] T011 [P] [US1] Write unit tests for Parquet file discovery (find .parquet files in directory) in `tests/unit/test_pipeline.py`
- [ ] T012 [P] [US1] Write unit tests for DataFrame-to-DuckDB insert logic in `tests/unit/test_loader.py`
- [ ] T013 [US1] Write integration test for end-to-end ingestion (Parquet in → DuckDB out) in `tests/integration/test_pipeline.py`

### Implementation for User Story 1

- [ ] T014 [US1] Implement Parquet file discovery (list .parquet files in source directory) in `src/ingestion/pipeline.py`
- [ ] T015 [US1] Implement Parquet reading via Polars in `src/ingestion/pipeline.py`
- [ ] T016 [US1] Implement DataFrame insertion into DuckDB transactions table in `src/ingestion/loader.py`
- [ ] T017 [US1] Implement pipeline orchestrator (discover → read → load → summary) in `src/ingestion/pipeline.py`
- [ ] T018 [US1] Implement run tracking (create ingestion_runs record, update on completion) in `src/ingestion/loader.py`
- [ ] T019 [US1] Implement CLI entrypoint with argparse in `src/ingest_transactions.py` (PEP 723 inline deps)
- [ ] T020 [US1] Implement run summary output (files processed, records loaded, elapsed time) in `src/ingestion/pipeline.py`

**Checkpoint**: MVP complete. Pipeline reads Parquet files and loads them into DuckDB. Queryable warehouse operational.

---

## Phase 4: User Story 2 — Schema Validation and Quarantine (Priority: P2)

**Goal**: Validate each record against the expected schema before loading, rejecting malformed records to a quarantine table.

**Independent Test**: Craft Parquet files with schema violations and verify valid records load while invalid records land in quarantine with error descriptions.

### Tests for User Story 2

- [ ] T021 [P] [US2] Write unit tests for file-level schema validation (missing columns, wrong types) in `tests/unit/test_validator.py`
- [ ] T022 [P] [US2] Write unit tests for record-level value validation (nulls, ranges) in `tests/unit/test_validator.py`
- [ ] T023 [P] [US2] Write unit tests for quarantine record insertion in `tests/unit/test_loader.py`
- [ ] T024 [US2] Write integration test for mixed valid/invalid records in one file in `tests/integration/test_pipeline.py`

### Implementation for User Story 2

- [ ] T025 [US2] Implement file-level schema validation (column names, data types) in `src/ingestion/validator.py`
- [ ] T026 [US2] Implement record-level value validation (null checks, range checks, enum checks) in `src/ingestion/validator.py`
- [ ] T027 [US2] Implement quarantine record insertion (store original data as JSON + rejection reason) in `src/ingestion/loader.py`
- [ ] T028 [US2] Integrate validation into pipeline orchestrator (validate before load, route failures to quarantine) in `src/ingestion/pipeline.py`
- [ ] T029 [US2] Update run summary to include records_quarantined count in `src/ingestion/pipeline.py`

**Checkpoint**: Pipeline validates all records. Invalid data quarantined with diagnostics. Warehouse contains only clean data.

---

## Phase 5: User Story 3 — Deduplication by Transaction ID (Priority: P3)

**Goal**: Detect and skip duplicate transactions based on `transaction_id`, both within-file and cross-file.

**Independent Test**: Ingest the same file twice and verify record count is unchanged. Ingest a file with internal duplicates and verify only unique records loaded.

### Tests for User Story 3

- [ ] T030 [P] [US3] Write unit tests for within-file deduplication in `tests/unit/test_dedup.py`
- [ ] T031 [P] [US3] Write unit tests for cross-file deduplication (anti-join against existing IDs) in `tests/unit/test_dedup.py`
- [ ] T032 [US3] Write integration test for idempotent re-ingestion in `tests/integration/test_pipeline.py`

### Implementation for User Story 3

- [ ] T033 [US3] Implement within-file deduplication (Polars unique by transaction_id, keep first) in `src/ingestion/dedup.py`
- [ ] T034 [US3] Implement cross-file deduplication (load existing IDs from DuckDB, anti-join) in `src/ingestion/dedup.py`
- [ ] T035 [US3] Integrate deduplication into pipeline orchestrator (dedup after validation, before load) in `src/ingestion/pipeline.py`
- [ ] T036 [US3] Update run summary to include duplicates_skipped count in `src/ingestion/pipeline.py`

**Checkpoint**: Pipeline is fully idempotent. Re-running produces zero new records. Duplicates tracked in summary.

---

## Phase 6: User Story 4 — Lineage Tracking (Priority: P4)

**Goal**: Record provenance of every record (source file, ingestion timestamp, pipeline run ID).

**Independent Test**: Ingest files and query lineage metadata to confirm each record is tagged with source file, ingestion timestamp, and run ID.

### Tests for User Story 4

- [ ] T037 [P] [US4] Write unit tests for lineage column enrichment in `tests/unit/test_pipeline.py`
- [ ] T038 [US4] Write integration test for multi-run lineage traceability in `tests/integration/test_pipeline.py`

### Implementation for User Story 4

- [ ] T039 [US4] Implement lineage column enrichment (add source_file, ingested_at, run_id columns to DataFrame before insert) in `src/ingestion/pipeline.py`
- [ ] T040 [US4] Ensure quarantine records also carry lineage metadata in `src/ingestion/loader.py`
- [ ] T041 [US4] Implement unique run_id generation (UUID) in `src/ingestion/pipeline.py`

**Checkpoint**: Every record in the warehouse is traceable to its source file and ingestion run.

---

## Phase 7: User Story 5 — Date-Based Partitioning (Priority: P5)

**Goal**: Organize warehouse data by transaction date for efficient date-range queries.

**Independent Test**: Ingest transactions spanning multiple dates and verify date-filtered queries leverage the partition column.

### Tests for User Story 5

- [ ] T042 [P] [US5] Write unit tests for transaction_date column derivation in `tests/unit/test_pipeline.py`
- [ ] T043 [US5] Write integration test for date-range query efficiency in `tests/integration/test_pipeline.py`

### Implementation for User Story 5

- [ ] T044 [US5] Implement transaction_date column derivation (extract DATE from timestamp) in `src/ingestion/pipeline.py`
- [ ] T045 [US5] Update DuckDB table schema to include transaction_date column in `src/ingestion/loader.py`

**Checkpoint**: Warehouse data is queryable by date. Date-range queries filter efficiently.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Quality assurance, documentation, and final validation

- [ ] T046 [P] Write data quality tests (6Cs validation: completeness, consistency, conformity) in `tests/quality/test_ingestion_quality.py`
- [ ] T047 [P] Add logging throughout pipeline (info, warning, error levels) in `src/ingestion/pipeline.py`
- [ ] T048 Run full test suite (`uv run pytest tests/ -v`) and ensure all tests pass
- [ ] T049 Run linter (`uv run ruff check .`) and fix any issues
- [ ] T050 Validate quickstart.md end-to-end (generate data → ingest → query)
- [ ] T051 Update `AGENTS.md` if any new technologies were added

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational — MVP target
- **User Story 2 (Phase 4)**: Depends on Foundational; integrates with US1 pipeline flow
- **User Story 3 (Phase 5)**: Depends on Foundational; integrates with US1 pipeline flow
- **User Story 4 (Phase 6)**: Depends on Foundational; enriches US1 pipeline flow
- **User Story 5 (Phase 7)**: Depends on Foundational; enriches US1 pipeline flow
- **Polish (Phase 8)**: Depends on all user stories being complete

### User Story Dependencies

- **US1 (P1)**: Can start after Foundational — no dependencies on other stories
- **US2 (P2)**: Can start after US1 (needs pipeline flow to integrate validation)
- **US3 (P3)**: Can start after US1 (needs pipeline flow to integrate dedup)
- **US4 (P4)**: Can start after US1 (needs pipeline flow to add lineage columns)
- **US5 (P5)**: Can start after US1 (needs pipeline flow to add date column)
- **US2-US5 can proceed in parallel** once US1 is complete

### Within Each User Story

- Tests written first (TDD)
- Models/services before integration
- Core implementation before pipeline integration
- Story complete before moving to next priority

### Parallel Opportunities

- T002, T003, T004 can run in parallel (Phase 1)
- T006, T007 can run in parallel; T009, T010 can run in parallel (Phase 2)
- T011, T012 can run in parallel (US1 tests)
- T021, T022, T023 can run in parallel (US2 tests)
- T030, T031 can run in parallel (US3 tests)
- T037 can run in parallel with US5 tests (T042)
- T046, T047 can run in parallel (Phase 8)

---

## Parallel Example: User Story 2

```bash
# Launch all tests for User Story 2 together:
Task: "Unit tests for file-level schema validation in tests/unit/test_validator.py"
Task: "Unit tests for record-level value validation in tests/unit/test_validator.py"
Task: "Unit tests for quarantine record insertion in tests/unit/test_loader.py"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: User Story 1 (basic ingest)
4. **STOP and VALIDATE**: Test end-to-end ingestion independently
5. You now have a working pipeline that loads Parquet into DuckDB

### Incremental Delivery

1. Setup + Foundational → Foundation ready
2. Add US1 (basic ingest) → Test → MVP!
3. Add US2 (validation + quarantine) → Test → Data quality gate
4. Add US3 (dedup) → Test → Idempotent pipeline
5. Add US4 (lineage) → Test → Auditable pipeline
6. Add US5 (date partitioning) → Test → Optimized queries
7. Polish → Full test suite, docs, linting
8. Each story adds value without breaking previous stories

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Tests written first (TDD approach) — verify they fail before implementing
- Commit after each phase completion
- Stop at any checkpoint to validate story independently
- Total tasks: 51
