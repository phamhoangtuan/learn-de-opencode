# Tasks: Pipeline Orchestration Layer

**Feature**: 006-pipeline-orchestration  
**Date**: 2026-02-23  
**Spec**: `specs/006-pipeline-orchestration/spec.md`  
**Plan**: `specs/006-pipeline-orchestration/plan.md`

## Phase 1: Foundation -- Models and Module Structure

- [x] T001 [US1] Create `src/orchestrator/__init__.py`
- [x] T002 [P] [US1] Create `src/orchestrator/models.py` with `PipelineStep`, `StepResult`, `PipelineRun` dataclasses
- [x] T003 [P] [US1] Write unit tests for models in `tests/unit/test_pipeline_models.py` (dataclass defaults, StepResult to dict, PipelineRun status)

## Phase 2: Runner Core -- DAG Resolution and Step Execution

- [x] T004 [US1] Create `src/orchestrator/runner.py` with `run_pipeline()` function: resolve order via `src/transformer/graph.py`, iterate steps, invoke subprocesses, collect `StepResult`s
- [x] T005 [US1] Implement step execution: `subprocess.run(shell=False)` streaming to terminal; on failure re-run with `capture_output=True` to capture ≤2000 chars stderr
- [x] T006 [US1] Implement JSON log emission to stdout: one line per step after completion with `run_id`, `step`, `status`, `duration_s`, `exit_code`, `timestamp`
- [x] T007 [US1] Implement pipeline halt logic: stop on first step failure, mark remaining steps as not run, return `PipelineRun` with `status="failed"`
- [x] T008 [US2,US3] Implement `--step` (single step) and `--from-step` (slice) filtering on the resolved execution order
- [x] T009 [US4] Implement dry-run mode: print execution plan to stderr, return `PipelineRun` with dry-run flag, no subprocess calls, no DuckDB writes
- [x] T010 [US1,US2,US3] Write unit tests for runner in `tests/unit/test_pipeline_runner.py`:
  - DAG resolution returns correct order
  - `--step` filter runs exactly one step (using mock subprocess)
  - `--from-step` filter runs correct slice
  - Halt-on-failure: subsequent steps not executed
  - Dry-run: zero subprocess calls, zero DuckDB writes

## Phase 3: Metadata Writer -- DuckDB pipeline_runs Table

- [x] T011 [US1] Create `src/orchestrator/metadata.py` with `ensure_pipeline_runs_table(db_path)` (idempotent `CREATE TABLE IF NOT EXISTS`)
- [x] T012 [US1] Implement `record_pipeline_run(db_path, run)` in `src/orchestrator/metadata.py`: insert one row with `run_id`, `status`, `started_at`, `finished_at`, `steps_json`, `dry_run`
- [x] T013 [US1] Write integration tests for metadata in `tests/integration/test_pipeline_integration.py`:
  - DDL idempotency (call `ensure_pipeline_runs_table` twice, no error)
  - Successful run: row written with correct status and valid JSON `steps_json`
  - Failed run (mock step failure): row written with `status="failed"`, failed step in `steps_json`
  - Dry-run: no row written

## Phase 4: CLI Entrypoint

- [x] T014 [US1,US2,US3,US4] Create `src/run_pipeline.py` with PEP 723 inline deps (`duckdb`), argparse:
  - `--step <name>` — run one named step
  - `--from-step <name>` — run from named step onwards
  - `--dry-run` — print plan, no execution
  - `--db-path` — DuckDB path (default: `data/warehouse/transactions.duckdb`)
  - `--verbose` — debug logging to stderr
- [x] T015 [US1] Define canonical `PIPELINE_STEPS` list with all 5 `PipelineStep` objects at module level in `src/run_pipeline.py`
- [x] T016 [US2] Add validation: reject unknown `--step` name with list of valid names and exit 1
- [x] T017 [US2,US3] Add validation: reject simultaneous `--step` + `--from-step` with clear error and exit 1
- [x] T018 [US1] Wire `run_pipeline()` and `record_pipeline_run()` into `main()`, setting exit code from `PipelineRun.status`

## Phase 5: Polish

- [x] T019 Update `AGENTS.md` with Feature 006 technology and run command
- [x] T020 Run `ruff check .` and fix any lint errors
- [x] T021 Run `pytest tests/unit/ tests/integration/` and verify all new tests pass
