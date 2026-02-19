# Tasks: SQL Transformations Layer

**Input**: Design documents from `/specs/003-sql-transformations/`
**Prerequisites**: plan.md (required), spec.md (required), research.md, data-model.md

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project structure and shared module skeleton

- [x] T001 [P] Create `src/transformer/` module with `__init__.py`
- [x] T002 [P] Create `src/transforms/` directory for SQL model files
- [x] T003 [P] Create domain models (TransformModel, TransformRunResult) in `src/transformer/models.py`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

- [x] T004 Implement SQL file parser in `src/transformer/parser.py` -- parse `-- model:` and `-- depends_on:` headers, extract SQL content, derive model name from filename
- [x] T005 Implement DAG resolution in `src/transformer/graph.py` -- Kahn's algorithm for topological sort, cycle detection, missing dependency detection
- [x] T006 Implement transform_runs DDL and metadata operations in `src/transformer/runner.py` -- create_transform_runs_table(), create_run(), complete_run()
- [x] T007 [P] Write unit tests for parser in `tests/unit/test_parser.py`
- [x] T008 [P] Write unit tests for graph resolution in `tests/unit/test_graph.py`

**Checkpoint**: Parser and DAG resolver tested. Runner infrastructure ready.

---

## Phase 3: User Story 1 - Run SQL Transformations (Priority: P1)

**Goal**: Single command executes all SQL files in dependency order against the DuckDB warehouse

**Independent Test**: Run the transform command and verify all output views/tables exist

### Tests for User Story 1

- [x] T009 [P] [US1] Write integration test for full transform pipeline in `tests/integration/test_transforms.py` -- verify end-to-end execution creates expected outputs
- [x] T010 [P] [US1] Write unit test for runner orchestration in `tests/unit/test_runner.py` -- mock DB, verify execution order and error handling

### Implementation for User Story 1

- [x] T011 [US1] Implement runner orchestration in `src/transformer/runner.py` -- discover SQL files, parse, resolve DAG, execute in order, handle per-model errors (FR-001 through FR-006, FR-015, FR-016)
- [x] T012 [US1] Create CLI entrypoint `src/run_transforms.py` with PEP 723 inline deps -- argparse for `--db-path` and `--transforms-dir` parameters (FR-012, FR-013, FR-014)

**Checkpoint**: Runner executes SQL files in dependency order. CLI entrypoint works via `uv run`.

---

## Phase 4: User Story 2 - Staging View (Priority: P2)

**Goal**: `stg_transactions` view standardizes the raw transactions table

**Independent Test**: Query `stg_transactions` and verify column names, types, and row count

### Tests for User Story 2

- [x] T013 [P] [US2] Write quality test for stg_transactions in `tests/quality/test_transform_quality.py` -- row count match, column presence, no data loss (CHK001-CHK005)

### Implementation for User Story 2

- [x] T014 [US2] Write SQL model `src/transforms/staging__stg_transactions.sql` -- CREATE OR REPLACE VIEW with column renaming (timestamp→transaction_timestamp), all lineage columns passed through (FR-007)

**Checkpoint**: `stg_transactions` view exists and passes quality tests.

---

## Phase 5: User Story 3 - Daily Spend by Category Mart (Priority: P3)

**Goal**: `daily_spend_by_category` table aggregates completed debit transactions by date, category, and currency

**Independent Test**: Query the mart table and compare against manual aggregation of stg_transactions

### Tests for User Story 3

- [x] T015 [P] [US3] Write quality test for daily_spend_by_category in `tests/quality/test_transform_quality.py` -- aggregation correctness, filter verification, grain uniqueness (CHK006-CHK010)

### Implementation for User Story 3

- [x] T016 [US3] Write SQL model `src/transforms/mart__daily_spend_by_category.sql` -- CREATE OR REPLACE TABLE AS with depends_on: stg_transactions, filter completed debits, group by (date, category, currency) (FR-008)

**Checkpoint**: `daily_spend_by_category` table exists and passes quality tests.

---

## Phase 6: User Story 4 - Monthly Account Summary Mart (Priority: P4)

**Goal**: `monthly_account_summary` table aggregates account metrics by month and currency

**Independent Test**: Query the mart table and verify debit/credit/net_flow calculations

### Tests for User Story 4

- [x] T017 [P] [US4] Write quality test for monthly_account_summary in `tests/quality/test_transform_quality.py` -- debit/credit breakdown, net_flow calculation, grain uniqueness (CHK011-CHK015)

### Implementation for User Story 4

- [x] T018 [US4] Write SQL model `src/transforms/mart__monthly_account_summary.sql` -- CREATE OR REPLACE TABLE AS with depends_on: stg_transactions, conditional aggregation for debits/credits, group by (month, account_id, currency) (FR-009)

**Checkpoint**: `monthly_account_summary` table exists and passes quality tests.

---

## Phase 7: User Story 5 - Transform Run Metadata (Priority: P5)

**Goal**: Each transform execution is tracked in `transform_runs` metadata table

**Independent Test**: Run transforms and query `transform_runs` for correct row with timestamps and model counts

### Tests for User Story 5

- [x] T019 [P] [US5] Write integration test for transform_runs tracking in `tests/integration/test_transforms.py` -- verify metadata row creation, status, model counts (CHK023-CHK027)

### Implementation for User Story 5

- [x] T020 [US5] Integrate metadata tracking into runner -- create_run at start, complete_run at end, count models executed/failed, record errors (FR-010, FR-011)

**Checkpoint**: `transform_runs` table populated correctly after each execution.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Idempotency verification and final quality

- [x] T021 [P] Write idempotency tests in `tests/quality/test_transform_quality.py` -- run transforms twice, verify identical outputs (CHK016-CHK019)
- [x] T022 Update `AGENTS.md` with Feature 003 technology and commands
- [x] T023 Run full test suite (`pytest`) and fix any failures

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies -- start immediately
- **Foundational (Phase 2)**: Depends on Phase 1 -- BLOCKS all user stories
- **US1 (Phase 3)**: Depends on Phase 2 -- BLOCKS US2-US5 (need runner to execute SQL)
- **US2 (Phase 4)**: Depends on Phase 3 (needs runner to execute the staging SQL)
- **US3 (Phase 5)**: Depends on Phase 4 (mart depends on stg_transactions)
- **US4 (Phase 6)**: Depends on Phase 4 (mart depends on stg_transactions). Can run parallel with US3.
- **US5 (Phase 7)**: Depends on Phase 3 (integrates into runner)
- **Polish (Phase 8)**: Depends on all user stories

### Parallel Opportunities

- T001, T002, T003 (Phase 1 setup) can run in parallel
- T007, T008 (unit tests) can run in parallel
- T009, T010 (US1 tests) can run in parallel
- T015 and T017 (US3/US4 quality tests) can run in parallel
- US3 and US4 implementation (T016, T018) can run in parallel after US2

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story
- Tests written before or alongside implementation
- All SQL models use CREATE OR REPLACE for idempotency
- Commit after each phase checkpoint
