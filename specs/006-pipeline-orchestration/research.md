# Research: Pipeline Orchestration Layer

**Feature**: 006-pipeline-orchestration  
**Date**: 2026-02-23

## Decision 1: Subprocess Invocation Strategy

**Question**: How should the orchestrator invoke each pipeline step?

**Options evaluated**:
1. **`subprocess.run(shell=True, cmd_string)`** — simple, but security risk with user-supplied input and harder to test.
2. **`subprocess.run(shell=False, argv_list)`** — explicit, no shell injection risk, argv is easily mocked in tests.
3. **Import and call each step's `main()` function directly** — tight coupling, mixes process isolation with function calls; a step crash could take down the orchestrator.
4. **`subprocess.Popen` with streaming** — gives line-by-line streaming but adds complexity vs. `subprocess.run`.

**Decision**: Option 2. `subprocess.run(shell=False, argv_list, cwd=cwd)`. Steps stream to the terminal naturally (no `capture_output`). On failure, re-invoke with `capture_output=True, text=True` to extract up to 2000 chars of stderr for metadata. This keeps the implementation simple while preserving real-time UX.

## Decision 2: DAG Resolution Reuse Strategy

**Question**: How to reuse `src/transformer/graph.py` without modification for pipeline steps?

**Analysis**: `resolve_execution_order(models: list[TransformModel])` only uses `.name: str` and `.depends_on: list[str]` on its input objects. It does not call any other methods or access other attributes. Python's duck typing means any object with these two attributes will work.

**Decision**: Design `PipelineStep` dataclass with `name: str` and `depends_on: list[str]` fields. Pass the list of `PipelineStep` objects directly to `resolve_execution_order`. No wrapper, no monkey-patching needed. This satisfies FR-009 with zero modifications to Feature 003 code.

## Decision 3: Structured JSON Log Format

**Question**: What JSON schema to emit per step?

**Decision**: One JSON object per line to stdout after each step completes:

```json
{"run_id": "uuid", "step": "generate", "status": "success", "duration_s": 1.23, "exit_code": 0, "timestamp": "2026-02-23T10:00:01Z"}
```

On failure:
```json
{"run_id": "uuid", "step": "ingest", "status": "failed", "duration_s": 0.45, "exit_code": 1, "timestamp": "...", "error_output": "last 2000 chars of stderr"}
```

Skipped steps (in `--from-step` scenarios):
```json
{"run_id": "uuid", "step": "generate", "status": "skipped", "duration_s": 0.0, "exit_code": null, "timestamp": "..."}
```

## Decision 4: `pipeline_runs` Table Schema

**Question**: What DuckDB schema for pipeline run metadata?

**Decision**:

```sql
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id        VARCHAR PRIMARY KEY,
    status        VARCHAR NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL,
    finished_at   TIMESTAMPTZ,
    steps_json    VARCHAR NOT NULL,
    dry_run       BOOLEAN NOT NULL DEFAULT FALSE
);
```

`steps_json` is a JSON string — DuckDB's native JSON type would work but VARCHAR keeps it consistent with how Feature 004 stores `sample_violations`. The `dry_run` boolean allows filtering metadata rows to only real runs.

**Note**: Dry-run invocations do NOT write to `pipeline_runs`.

## Decision 5: No External Scheduler

**Question**: Should we use a lightweight task runner like `invoke`, `make`, or `doit`?

**Decision**: No. The spec explicitly requires pure Python with no external scheduler. The existing `src/transformer/graph.py` provides the only non-stdlib dependency needed (DAG resolution). All other functionality uses stdlib: `subprocess`, `json`, `datetime`, `uuid`, `argparse`.

## Decision 6: Module Location

**Question**: Where should orchestrator code live?

**Options**:
1. Everything in `src/run_pipeline.py` as a single file.
2. `src/orchestrator/` module parallel to `src/transformer/` and `src/checker/`.

**Decision**: Option 2. The project's established pattern separates models, runner, and metadata into distinct files per module. A single-file approach would create a 300+ line monolith. `src/orchestrator/` follows the same convention and keeps each concern independently testable.
