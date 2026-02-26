# Tasks: SCD Type 2 — Accounts Dimension

**Input**: Design documents from `specs/009-scd2-accounts/`
**Prerequisites**: spec.md ✅ | plan.md ✅ | research.md ✅ | data-model.md ✅ | contracts/ ✅

**Organization**: Tasks grouped by user story — each story is independently testable.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no shared dependencies)
- **[Story]**: User story this task belongs to
- Paths are repository-root-relative

---

## Phase 1: Setup

**Purpose**: Create module scaffold and extend warehouse DDL.

- [ ] T001 Create `src/dimensions/` package with `src/dimensions/__init__.py`
- [ ] T002 Create empty `src/dimensions/dim_accounts.py` with module docstring and imports (duckdb, dataclasses, datetime, logging)
- [ ] T003 [P] Create test file `tests/unit/test_dim_accounts.py` with placeholder imports
- [ ] T004 [P] Create test file `tests/integration/test_dim_pipeline.py` with placeholder imports

**Checkpoint**: Package structure exists; no logic yet.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: DDL and table creation — must complete before any user story work.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [ ] T005 Add `_DIM_ACCOUNTS_SK_SEQ_DDL` and `_DIM_ACCOUNTS_DDL` string constants to `src/ingestion/loader.py` — DDL from data-model.md (sequence START 1; 13-column table: account_sk BIGINT PK DEFAULT nextval('dim_accounts_sk_seq'), account_id VARCHAR NOT NULL, primary_currency VARCHAR, primary_category VARCHAR, transaction_count BIGINT, total_spend DOUBLE, first_seen DATE, last_seen DATE, row_hash VARCHAR NOT NULL, valid_from TIMESTAMPTZ NOT NULL, valid_to TIMESTAMPTZ, is_current BOOLEAN NOT NULL DEFAULT TRUE, run_id VARCHAR NOT NULL)
- [ ] T006 Extend `create_tables()` in `src/ingestion/loader.py` to execute `_DIM_ACCOUNTS_SK_SEQ_DDL` then `_DIM_ACCOUNTS_DDL` — must remain idempotent (IF NOT EXISTS)
- [ ] T007 Implement `create_dim_tables(conn)` in `src/dimensions/dim_accounts.py` — calls the two new DDL statements via `conn.execute()`; idempotent; logs "Dimension tables ready"
- [ ] T008 Define `DimBuildResult` dataclass in `src/dimensions/dim_accounts.py` — fields: `accounts_processed: int`, `new_versions: int`, `unchanged: int`, `elapsed_seconds: float`
- [ ] T009 Define `DimBuildError(Exception)` class in `src/dimensions/dim_accounts.py` with docstring
- [ ] T010 Write unit tests for `create_dim_tables()` in `tests/unit/test_dim_accounts.py` — verify dim_accounts and sequence exist after call; verify idempotency (call twice, no error); verify all 13 columns present (including run_id)

**Checkpoint**: `create_dim_tables()` works; DDL integrated into `create_tables()`; foundation ready.

---

## Phase 3: User Story 1 — Account Dimension Build (Priority: P1) 🎯 MVP

**Goal**: Derive account behavioral profiles from transaction history and populate `dim_accounts` with one current row per account.

**Independent Test**: Run pipeline with 5-account transaction fixture → query `dim_accounts` → assert 5 rows, all `is_current=TRUE`, correct `primary_currency` and `transaction_count`.

### Implementation

- [ ] T011 [US1] Implement `_compute_account_profiles(conn)` private function in `src/dimensions/dim_accounts.py` — returns a DuckDB relation with columns: account_id, primary_currency, primary_category, transaction_count, total_spend, first_seen, last_seen, row_hash. Uses ROW_NUMBER+QUALIFY tie-breaking pattern from research R-003. Reads from `stg_transactions`. Hash = `md5(primary_currency||'|'||primary_category||'|'||transaction_count||'|'||total_spend||'|'||first_seen||'|'||last_seen)` with COALESCE nulls to ''
- [ ] T012 [US1] Implement `_insert_new_accounts(conn, run_id, run_date)` private function in `src/dimensions/dim_accounts.py` — inserts rows from computed profiles where account_id does NOT yet exist in dim_accounts. Sets valid_from=run_date, valid_to=NULL, is_current=TRUE, account_sk via nextval, run_id=run_id (lineage). Returns count of inserted rows
- [ ] T013 [US1] Implement `build_dim_accounts(conn, run_id, run_date)` public function in `src/dimensions/dim_accounts.py` — wraps `conn.begin()`/`conn.commit()`/`conn.rollback()` around the full SCD merge. For first run (empty dim): calls `_insert_new_accounts` only. Records elapsed_seconds. Returns `DimBuildResult`. Raises `DimBuildError` on failure after rollback
- [ ] T014 [US1] Write unit tests for `build_dim_accounts()` (first-run case) in `tests/unit/test_dim_accounts.py` — fixture: 5 accounts × varied currencies/categories including one account with equal-count USD/EUR (assert primary_currency="EUR" for alpha tie-break). Assert: 5 rows in dim_accounts; all is_current=TRUE; primary_currency = mode with alpha tie-break; transaction_count correct; valid_to IS NULL; run_id matches supplied run_id; DimBuildResult.new_versions == 5, unchanged == 0
- [ ] T015 [US1] Write integration test `test_dim_build_first_run` in `tests/integration/test_dim_pipeline.py` — runs full pipeline on tmp DB, verifies dim_accounts populated correctly end-to-end

**Checkpoint**: US1 complete — dim_accounts populated on first run. MVP deliverable.

---

## Phase 4: User Story 2 — Change Detection and History Tracking (Priority: P1)

**Goal**: Detect attribute changes between pipeline runs and write new SCD2 version rows while closing the previous ones.

**Independent Test**: Run pipeline twice with data that shifts `primary_currency` for one account → assert: old row closed (is_current=FALSE, valid_to set), new row inserted (is_current=TRUE), history has 2 rows for that account.

### Implementation

- [ ] T016 [US2] Implement `_expire_changed_rows(conn, run_date)` private function in `src/dimensions/dim_accounts.py` — executes Statement A MERGE: `WHEN MATCHED AND target.row_hash <> source.row_hash THEN UPDATE SET valid_to = run_date - INTERVAL '1 day', is_current = FALSE`. Uses `_incoming_accounts` temp view. Returns count of expired rows
- [ ] T017 [US2] Implement `_insert_updated_accounts(conn, run_id, run_date)` private function in `src/dimensions/dim_accounts.py` — executes Statement B INSERT: inserts new current rows for accounts where hash changed (NOT EXISTS guard). Returns count of inserted rows
- [ ] T018 [US2] Update `build_dim_accounts()` in `src/dimensions/dim_accounts.py` to use full two-statement pattern (expire → insert) inside the atomic transaction. Update `DimBuildResult` to set `new_versions` = newly inserted rows, `unchanged` = accounts_processed - new_versions
- [ ] T019 [US2] Write unit tests for change detection in `tests/unit/test_dim_accounts.py` — run build twice; second run changes primary_currency for 1 account. Assert: old row has is_current=FALSE and valid_to set; new row has is_current=TRUE; unchanged accounts untouched; DimBuildResult.new_versions == 1, unchanged == N-1
- [ ] T020 [US2] Write unit test for rollback atomicity in `tests/unit/test_dim_accounts.py` — mock conn.execute to raise on Statement B INSERT; verify dim_accounts state is identical before and after the failed call (no partial writes)
- [ ] T021 [US2] Write integration test `test_dim_build_change_detection` in `tests/integration/test_dim_pipeline.py` — two-run pipeline, verifies SCD2 history rows end-to-end

**Checkpoint**: US2 complete — full SCD2 change detection and history tracking working.

---

## Phase 5: User Story 3 — Account History Mart (Priority: P2)

**Goal**: Provide `mart__account_history` SQL view exposing full change timeline with version_number and version_duration_days.

**Independent Test**: After two-run pipeline, query `mart__account_history` → assert rows ordered by valid_from, version_number increments per account, version_duration_days correct (days-from-today for current row, not sentinel).

### Implementation

- [ ] T022 [US3] Create `src/transforms/mart__account_history.sql` with header comments `-- model: mart__account_history` and `-- depends_on: dim_accounts`. SQL: `CREATE OR REPLACE VIEW mart__account_history AS SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY valid_from) AS version_number, CASE WHEN is_current THEN CAST(current_date - valid_from::DATE AS INTEGER) ELSE CAST(valid_to::DATE - valid_from::DATE AS INTEGER) END AS version_duration_days FROM dim_accounts ORDER BY account_id, valid_from`
- [ ] T023 [US3] Write unit tests for `mart__account_history` in `tests/unit/test_dim_accounts.py` — fixture with multi-version account; assert version_number increments; assert version_duration_days correct for current and expired rows; assert sentinel date does NOT appear in version_duration_days
- [ ] T024 [US3] Write integration test `test_account_history_mart` in `tests/integration/test_dim_pipeline.py` — verifies mart query returns correct results after two-run pipeline

**Checkpoint**: US3 complete — account history mart queryable.

---

## Phase 6: User Story 4 — Dimension Integrity Checks (Priority: P2)

**Goal**: Three critical-severity data quality checks that verify SCD2 structural correctness.

**Independent Test**: Run checks on correctly-built dim_accounts → all pass (zero rows). Manually insert duplicate is_current row → single_current check fails.

### Implementation

- [ ] T025 [P] [US4] Create `src/checks/check__dim_accounts_single_current.sql` — header: `-- check: dim_accounts_single_current`, `-- severity: critical`, `-- description: Each account_id must have exactly one is_current=TRUE row`. SQL returns account_id + count for violating accounts: `SELECT account_id, COUNT(*) AS current_count FROM dim_accounts WHERE is_current = TRUE GROUP BY account_id HAVING COUNT(*) > 1`
- [ ] T026 [P] [US4] Create `src/checks/check__dim_accounts_no_overlapping_ranges.sql` — header: `-- check: dim_accounts_no_overlapping_ranges`, `-- severity: critical`, `-- description: No two versions of the same account_id may have overlapping valid_from/valid_to ranges`. SQL self-join: `SELECT a.account_id, a.account_sk, b.account_sk FROM dim_accounts a JOIN dim_accounts b ON a.account_id = b.account_id AND a.account_sk < b.account_sk WHERE a.valid_from < COALESCE(b.valid_to, '9999-12-31'::TIMESTAMPTZ) AND b.valid_from < COALESCE(a.valid_to, '9999-12-31'::TIMESTAMPTZ)`
- [ ] T027 [P] [US4] Create `src/checks/check__dim_accounts_no_null_sk.sql` — header: `-- check: dim_accounts_no_null_sk`, `-- severity: critical`, `-- description: All dim_accounts rows must have a non-null surrogate key`. SQL: `SELECT account_id, valid_from FROM dim_accounts WHERE account_sk IS NULL`
- [ ] T028 [US4] Write unit tests for all 3 checks in `tests/unit/test_dim_accounts.py` — for each check: assert zero rows on valid dim_accounts; assert correct violation rows returned on intentionally corrupted fixture
- [ ] T029 [US4] Write quality tests in `tests/quality/test_dim_quality.py` — end-to-end: run pipeline, run all 3 checks, assert all pass on clean data

**Checkpoint**: US4 complete — integrity checks fully operational.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Pipeline integration, logging, and documentation.

- [ ] T030 [P] Integrate `build_dim_accounts()` into `src/run_pipeline.py` as a new `dim_build` step between `transforms` and `checks` — add `PipelineStep("dim_build", ...)` to the DAG; pass `run_id` and `run_date=date.today()` to the function; emit `DimBuildResult` stats to stdout as JSON
- [ ] T031 [P] Add `dim_build` step to `src/run_pipeline.py` `--step` / `--from-step` / `--skip-steps` argument handling so it can be run in isolation
- [ ] T032 Add `dim_build_runs` metadata table DDL to `src/ingestion/loader.py` — columns: run_id (FK), accounts_processed, new_versions, unchanged, elapsed_seconds, completed_at TIMESTAMPTZ. Insert a row from `DimBuildResult` at end of each dim build run
- [ ] T033 [P] Run full test suite `uv run pytest tests/unit/test_dim_accounts.py tests/integration/test_dim_pipeline.py tests/quality/test_dim_quality.py -v` — all tests must pass with zero failures and zero errors before this task is complete
- [ ] T034 Write automated PII classification test in `tests/unit/test_dim_accounts.py` — assert that dim_accounts has exactly the 13 expected columns and none match a PII pattern (e.g., name, email, phone, ssn, dob). Documents FR-014: account_id is a synthetic identifier, not PII in this system

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — start immediately
- **Phase 2 (Foundational)**: Depends on Phase 1 — **BLOCKS all user stories**
- **Phase 3 (US1)**: Depends on Phase 2 — MVP deliverable
- **Phase 4 (US2)**: Depends on Phase 3 (needs first-run insert logic as base)
- **Phase 5 (US3)**: Depends on Phase 4 (mart needs multi-version data)
- **Phase 6 (US4)**: T025–T027 (SQL files) can be authored in parallel with Phases 3/4/5 — they have no runtime dependency. T028–T029 (tests) require Phase 4 complete to have meaningful multi-version fixtures
- **Phase 7 (Polish)**: Depends on all prior phases

### User Story Dependencies

- **US1 (P1)**: After Phase 2 — no story dependencies
- **US2 (P1)**: After US1 — extends the merge logic built in US1
- **US3 (P2)**: After US2 — mart needs multi-version rows to be meaningful
- **US4 (P2)**: After Phase 2 — can start in parallel with US1/US2/US3

### Parallel Opportunities

- T003, T004 can run in parallel with T005–T009 (different files)
- T025, T026, T027 (three check SQL files) are fully independent [P]
- T030, T031 (pipeline integration) are independent [P]
- US4 (T025–T029) can run in parallel with US1/US2/US3 once Phase 2 is done

---

## Parallel Example: Phase 6 (US4 Checks)

```
Task tool (parallel, single message):
  [1] subagent_type: "general-purpose", model: "claude-sonnet-4-6"
      prompt: "Create src/checks/check__dim_accounts_single_current.sql per T025 spec"
  [2] subagent_type: "general-purpose", model: "claude-sonnet-4-6"
      prompt: "Create src/checks/check__dim_accounts_no_overlapping_ranges.sql per T026 spec"
  [3] subagent_type: "general-purpose", model: "claude-sonnet-4-6"
      prompt: "Create src/checks/check__dim_accounts_no_null_sk.sql per T027 spec"
```

---

## Implementation Strategy

### MVP First (US1 only — Phases 1–3)

1. Phase 1: Create module scaffold
2. Phase 2: DDL + `create_dim_tables()` + dataclasses
3. Phase 3: `build_dim_accounts()` first-run insert path
4. **STOP and VALIDATE**: `dim_accounts` populated with correct profiles
5. Merge to show working dimension table

### Incremental Delivery

1. Phases 1–3 → dim_accounts populated (MVP)
2. Phase 4 → SCD2 change detection active
3. Phase 5 → account history mart queryable
4. Phase 6 → integrity checks guarding dimension quality
5. Phase 7 → fully integrated into pipeline DAG

---

## Notes

- [P] tasks operate on different files — safe to run in parallel
- TDD: Write test (T010, T014, T019, T023, T028) before implementing the function it covers
- Each `conn.begin()` block in `dim_accounts.py` must always pair with `conn.rollback()` in `except` and `conn.commit()` in `else`
- `valid_to` on expired rows = `run_date - INTERVAL '1 day'` (non-overlapping ranges)
- `valid_to` on current rows = `NULL` (not the sentinel — sentinel only used in overlap check SQL)
- The `mart__account_history` SQL file must include `-- depends_on: dim_accounts` so the transform runner executes it after the dim is built
