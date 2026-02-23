# Data Engineer Checklist: Pipeline Orchestration Layer

**Feature**: 006-pipeline-orchestration  
**Date**: 2026-02-23  
**Purpose**: Verify the orchestration layer meets DAMA-DMBOK data engineering standards and the project constitution.

## DAG Correctness

- [ ] CHK001: All 5 pipeline steps are defined as `PipelineStep` nodes with correct `depends_on` lists
- [ ] CHK002: `resolve_execution_order` returns steps in correct topological order (generate→ingest→transforms→checks→dashboard)
- [ ] CHK003: `resolve_execution_order` is called without modification to `src/transformer/graph.py`
- [ ] CHK004: A single-step run (`--step`) executes exactly one step and skips all others
- [ ] CHK005: A `--from-step` run executes the named step and all downstream steps in DAG order

## Metadata & Lineage (Constitution Principle II)

- [ ] CHK006: `pipeline_runs` table is created with correct schema on first run (idempotent DDL)
- [ ] CHK007: Each non-dry-run invocation inserts exactly one row into `pipeline_runs`
- [ ] CHK008: `pipeline_runs.run_id` is a valid UUID
- [ ] CHK009: `pipeline_runs.started_at` and `finished_at` are populated and `finished_at > started_at`
- [ ] CHK010: `pipeline_runs.steps_json` is valid JSON parseable by `json.loads()`
- [ ] CHK011: Each step in `steps_json` contains `step`, `status`, `duration_s`, `exit_code` fields
- [ ] CHK012: Skipped steps appear in `steps_json` with `status: "skipped"` and `exit_code: null`
- [ ] CHK013: Failed step appears in `steps_json` with `status: "failed"` and non-null `error_output`
- [ ] CHK014: Dry-run invocations do NOT write to `pipeline_runs`

## Structured Logging

- [ ] CHK015: One JSON line emitted to stdout per step after it completes
- [ ] CHK016: JSON log line contains `run_id`, `step`, `status`, `duration_s`, `exit_code`
- [ ] CHK017: Each JSON log line is independently parseable by `json.loads()`
- [ ] CHK018: Human-readable progress messages go to stderr (not stdout)
- [ ] CHK019: Dry-run prints execution plan to stderr and produces zero stdout JSON lines

## Pipeline Execution Correctness

- [ ] CHK020: Full pipeline (`uv run src/run_pipeline.py`) runs all 5 steps in order and exits 0 on success
- [ ] CHK021: Pipeline halts immediately on first step failure and does not execute subsequent steps
- [ ] CHK022: Exit code is 0 on full success
- [ ] CHK023: Exit code is non-zero when any step fails
- [ ] CHK024: Exit code is 0 on dry-run
- [ ] CHK025: Step 5 (dashboard) runs `npm run sources` in the `dashboard/` subdirectory
- [ ] CHK026: Subprocess commands are invoked with `shell=False` (argv list, not string)

## CLI Interface

- [ ] CHK027: `--step <name>` with a valid name runs only that step
- [ ] CHK028: `--step foo` with an invalid name prints available names and exits 1 without running
- [ ] CHK029: `--from-step <name>` runs from that step through end of pipeline
- [ ] CHK030: `--step` + `--from-step` together produce a clear error message and exit 1
- [ ] CHK031: `--dry-run` prints execution plan and makes no DuckDB writes or filesystem changes
- [ ] CHK032: `--help` shows all options clearly

## Data Quality First (Constitution Principle I)

- [ ] CHK033: Data quality checks step (step 4) is a required node in the DAG, not optional
- [ ] CHK034: A failed checks step halts the pipeline (dashboard step does not run on checks failure)

## Robustness

- [ ] CHK035: If DuckDB directory doesn't exist, metadata write fails gracefully with a clear error (not a crash traceback)
- [ ] CHK036: If `npm` is not installed, step 5 exits non-zero with a clear error message
- [ ] CHK037: Running orchestrator twice in a row creates two separate `pipeline_runs` rows
- [ ] CHK038: Zero-step run (e.g., `--from-step` beyond last step) exits gracefully with a message

## Test Coverage

- [ ] CHK039: Unit tests cover DAG resolution with mock steps (no subprocess, no DuckDB)
- [ ] CHK040: Unit tests cover `--from-step` and `--step` slice logic
- [ ] CHK041: Unit tests cover dry-run mode produces no side effects
- [ ] CHK042: Integration tests verify `pipeline_runs` DDL idempotency
- [ ] CHK043: Integration tests verify correct row written on success and on failure
