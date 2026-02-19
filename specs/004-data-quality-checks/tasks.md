# Tasks: Data Quality Checks

**Feature**: 004-data-quality-checks
**Date**: 2026-02-19
**Spec**: `specs/004-data-quality-checks/spec.md`
**Plan**: `specs/004-data-quality-checks/plan.md`

## Phase 1: Foundation -- Models and Parser

- [x] T001 [US1] Create `src/checker/__init__.py`
- [x] T002 [US1,US2] Create `src/checker/models.py` with CheckSeverity, CheckStatus, RunStatus enums and CheckModel, CheckResult, CheckRunResult dataclasses
- [x] T003 [US1,US2] Write unit tests for models (enum values, dataclass defaults, summary method)
- [x] T004 [US1,US2] Create `src/checker/parser.py` with `parse_check_file()` and `discover_checks()` functions
- [x] T005 [US1,US2] Write unit tests for parser (header extraction, filename fallback, severity default, empty file, missing file, non-SQL skipped)

## Phase 2: Runner Core -- Execute Checks and Record Metadata

- [x] T006 [US1] Create `src/checker/runner.py` with `run_checks()` function: discover checks, execute each, compute run status
- [x] T007 [US1] Implement `_execute_check()` helper: run SQL, capture violations (up to 5), handle errors
- [x] T008 [US1,US2] Implement run status logic: any critical fail = "failed", only warn fails = "warn", all pass = "passed"
- [x] T009 [US1] Implement `check_runs` and `check_results` DuckDB metadata table creation and row insertion
- [x] T010 [US1] Write unit tests for runner (mock DuckDB, test status logic, test error handling, test metadata recording)
- [x] T011 [US1] Implement database existence validation before running checks

## Phase 3: Pre-Built Check SQL Files

- [x] T012 [US3] Create `src/checks/check__row_count_staging.sql` (completeness: staging row count = source)
- [x] T013 [US3] Create `src/checks/check__mart_not_empty.sql` (completeness: both marts have rows)
- [x] T014 [US4] Create `src/checks/check__freshness.sql` (timeliness: ingested_at within 48h)
- [x] T015 [US5] Create `src/checks/check__unique_daily_spend_grain.sql` (uniqueness: no duplicate grains)
- [x] T016 [US5] Create `src/checks/check__unique_monthly_summary_grain.sql` (uniqueness: no duplicate grains)
- [x] T017 [US5] Create `src/checks/check__accepted_values_currency.sql` (validity: currencies in allowed set)

## Phase 4: CLI Entrypoint

- [x] T018 [US6] Create `src/run_checks.py` with PEP 723 inline deps, argparse, exit code logic

## Phase 5: Integration and Quality Tests

- [x] T019 [US1-US6] Write integration tests: full end-to-end with temp DB, all checks pass on healthy data
- [x] T020 [US1-US6] Write integration tests: inject failures (empty marts, duplicate grains) and verify detection
- [x] T021 [US1-US6] Write quality tests for checklist items CHK001-CHK027

## Phase 6: Polish

- [x] T022 Update `README.md` with Feature 004 commands and data model
- [x] T023 Update `AGENTS.md` with Feature 004 technology and commands
- [x] T024 Run `ruff check .` and fix any lint errors
- [x] T025 Run full test suite and verify all tests pass
