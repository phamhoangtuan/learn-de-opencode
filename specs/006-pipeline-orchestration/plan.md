# Implementation Plan: Pipeline Orchestration Layer

**Branch**: `006-pipeline-orchestration` | **Date**: 2026-02-23 | **Spec**: [spec.md](spec.md)

## Summary

Add a pure-Python pipeline orchestrator (`src/run_pipeline.py`) that wires all 5 existing pipeline steps into a single runnable DAG. Steps are defined declaratively as nodes with `depends_on` lists, resolved via the existing `src/transformer/graph.py` topological sort. The orchestrator invokes each step as a subprocess, streams output in real time, emits one JSON log line per step to stdout, and persists a `pipeline_runs` metadata row to DuckDB after each non-dry-run invocation.

The orchestrator lives in a new `src/orchestrator/` module (models, runner, metadata) following the same file-per-concern pattern established by `src/transformer/` and `src/checker/`.

## Technical Context

**Language/Version**: Python 3.11+  
**Primary Dependencies**: `duckdb` (metadata writes), `subprocess` stdlib (step execution), `json` stdlib (log output)  
**Storage**: DuckDB embedded database at `data/warehouse/transactions.duckdb`  
**Testing**: pytest (unit, integration tiers)  
**Target Platform**: macOS/Linux local development  
**Performance Goals**: Orchestrator overhead < 500ms (excluding step execution time)  
**Constraints**: No external scheduler frameworks; single-user, sequential execution only  
**Scale/Scope**: 5 pipeline steps, extensible DAG structure  

## Constitution Check

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Data Quality First (DAMA-DMBOK Area 11) | PASS | Data quality checks (step 4) are a mandatory pipeline node; pipeline halts on critical check failure |
| II. Metadata & Lineage (DAMA-DMBOK Area 10) | PASS | `pipeline_runs` table records every invocation with step-level lineage in `steps_json`; each step result captures duration, status, exit code |
| III. Security & Privacy by Design (DAMA-DMBOK Area 5) | PASS | Local-only, no network calls from orchestrator itself; subprocesses inherit ambient environment; no credentials handled |
| IV. Integration & Interoperability (DAMA-DMBOK Area 6) | PASS | Step commands are plain CLI invocations (uv run / npm); JSON log output is machine-parseable by any consumer |
| V. Architecture & Modeling Integrity (DAMA-DMBOK Area 2 & 3) | PASS | DAG node structure defined in data-model.md; new module follows established `src/transformer/` naming patterns |

All gates pass. No violations.

## Project Structure

### Documentation (this feature)

```text
specs/006-pipeline-orchestration/
├── spec.md
├── plan.md           (this file)
├── research.md
├── data-model.md
├── checklists/
│   └── data-engineer.md
└── tasks.md
```

### Source Code (repository root)

```text
src/
├── run_pipeline.py                  # CLI entrypoint (PEP 723 inline deps)
└── orchestrator/
    ├── __init__.py
    ├── models.py                    # PipelineStep, StepResult, PipelineRun dataclasses
    ├── runner.py                    # Core execution logic
    └── metadata.py                  # DuckDB pipeline_runs DDL + insert

tests/
├── unit/
│   ├── test_pipeline_models.py
│   └── test_pipeline_runner.py
└── integration/
    └── test_pipeline_integration.py
```

## Implementation Phases

### Phase 0: Research (done — see research.md)

### Phase 1: Domain Models (`src/orchestrator/models.py`)

Define three dataclasses mirroring the pattern in `src/transformer/models.py`:

- `PipelineStep`: `name: str`, `command: list[str]`, `cwd: str | None`, `depends_on: list[str]`
- `StepResult`: `step: str`, `status: str` (success/failed/skipped/dry-run), `duration_s: float`, `exit_code: int | None`, `error_output: str | None`
- `PipelineRun`: `run_id: str`, `status: str`, `started_at: datetime`, `finished_at: datetime | None`, `step_results: list[StepResult]`

`PipelineStep` is designed to be compatible with `TransformModel` interface consumed by `src/transformer/graph.py` — specifically: `.name` attribute and `.depends_on` list.

### Phase 2: DAG Adapter for `graph.py` reuse

`src/transformer/graph.py`'s `resolve_execution_order(models)` takes a list of objects with `.name: str` and `.depends_on: list[str]`. `PipelineStep` satisfies this interface directly, so `resolve_execution_order` is called with the list of `PipelineStep` objects. No wrapper needed.

### Phase 3: Metadata Writer (`src/orchestrator/metadata.py`)

Two functions:
- `ensure_pipeline_runs_table(db_path)` — idempotent `CREATE TABLE IF NOT EXISTS pipeline_runs (...)` 
- `record_pipeline_run(db_path, run)` — `INSERT INTO pipeline_runs VALUES (...)`

The `steps_json` column stores `json.dumps([asdict(r) for r in run.step_results])`.

### Phase 4: Runner (`src/orchestrator/runner.py`)

`run_pipeline(steps, from_step, single_step, dry_run, db_path)` function:

1. Resolve execution order via `resolve_execution_order(steps)`.
2. Compute the active subset (all / from_step / single_step).
3. If `dry_run`: print plan to stderr, return a dry-run `PipelineRun` without executing.
4. For each active step in order:
   a. Start timer.
   b. Run `subprocess.run(step.command, cwd=step.cwd, capture_output=False)` — stdout/stderr stream to terminal.
   c. Re-run with `capture_output=True` only on failure to capture error snippet (≤2000 chars).
   d. Emit JSON log line to stdout.
   e. If exit code != 0: mark failed, stop loop.
5. Write metadata to DuckDB (unless dry-run).
6. Return `PipelineRun`.

**Streaming approach**: Use `subprocess.run()` without capturing for normal operation so users see real-time output. On failure, a second pass with capture extracts the tail of stderr for storage — this avoids buffering large outputs in memory during normal runs.

### Phase 5: CLI entrypoint (`src/run_pipeline.py`)

PEP 723 inline dependencies: `duckdb`. CLI args:
- `--step <name>` — run one named step
- `--from-step <name>` — run from named step onwards
- `--dry-run` — print plan, no execution
- `--db-path` — DuckDB path (default: `data/warehouse/transactions.duckdb`)
- `--verbose` — debug logging to stderr

Validation: reject `--step` + `--from-step` combination before running.

### Phase 6: Tests

**Unit tests** (no DuckDB, no subprocess):
- `test_pipeline_models.py`: dataclass construction, `PipelineRun.to_dict()`, JSON serialization
- `test_pipeline_runner.py`: DAG resolution with mock steps, `from_step` / `single_step` slice logic, dry-run returns correct plan

**Integration tests** (real DuckDB, mock subprocess):
- `test_pipeline_integration.py`: metadata DDL idempotency, pipeline_runs row written correctly, failure halts pipeline

## Step Definitions (Canonical)

```python
PIPELINE_STEPS = [
    PipelineStep(
        name="generate",
        command=["uv", "run", "src/generate_transactions.py"],
        cwd=None,  # project root
        depends_on=[],
    ),
    PipelineStep(
        name="ingest",
        command=["uv", "run", "src/ingest_transactions.py"],
        cwd=None,
        depends_on=["generate"],
    ),
    PipelineStep(
        name="transforms",
        command=["uv", "run", "src/run_transforms.py"],
        cwd=None,
        depends_on=["ingest"],
    ),
    PipelineStep(
        name="checks",
        command=["uv", "run", "src/run_checks.py"],
        cwd=None,
        depends_on=["transforms"],
    ),
    PipelineStep(
        name="dashboard",
        command=["npm", "run", "sources"],
        cwd="dashboard",
        depends_on=["checks"],
    ),
]
```

## Complexity Tracking

No constitution violations. Feature follows established patterns from Features 003 and 004. The subprocess streaming + capture-on-failure approach is a deliberate trade-off: real-time output experience at the cost of needing a second `subprocess.run` call on failure to capture error text for metadata. This is documented here per Governance rule 4.
