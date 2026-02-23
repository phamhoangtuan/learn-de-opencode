# Feature Specification: Pipeline Orchestration Layer

**Feature Branch**: `006-pipeline-orchestration`  
**Created**: 2026-02-23  
**Status**: Draft  
**Input**: User description: "Pipeline orchestration layer for the learn_de project. Build a Python-based orchestration system that wires all 5 existing pipeline steps into a single runnable DAG. The steps are: (1) generate synthetic data (src/generate_transactions.py), (2) ingest to DuckDB (src/ingest_transactions.py), (3) run SQL transforms (src/run_transforms.py), (4) run data quality checks (src/run_checks.py), (5) refresh Evidence dashboard sources (cd dashboard && npm run sources). The orchestrator should support running the full pipeline end-to-end with one command (uv run src/run_pipeline.py), running individual named steps, running from a specific step onwards, dry-run mode, structured JSON logging per step with duration and status, and a pipeline run metadata record stored in DuckDB. No external scheduler or framework dependency — pure Python with DAG resolution reusing the existing graph module from Feature 003."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Full Pipeline Run (Priority: P1)

A data engineer wants to regenerate all data and refresh the dashboard end-to-end with a single command.

**Why this priority**: This is the core value of the orchestrator — eliminating the need to remember and run 5 separate commands in order. Every other feature is secondary to this working reliably.

**Independent Test**: Run `uv run src/run_pipeline.py` against a clean state and verify all 5 steps execute in order, the warehouse is populated, and the dashboard sources are refreshed.

**Acceptance Scenarios**:

1. **Given** a clean project state, **When** `uv run src/run_pipeline.py` is executed, **Then** all 5 steps run in dependency order, each emits a structured JSON log line with `step`, `status`, `duration_s`, and the process exits 0.
2. **Given** step 2 (ingest) fails, **When** the pipeline runs, **Then** the pipeline halts after step 2, steps 3–5 do not execute, the process exits non-zero, and the failure is recorded in DuckDB.
3. **Given** a successful run, **When** examining DuckDB, **Then** a `pipeline_runs` table contains one row with `run_id`, `status`, `started_at`, `finished_at`, `steps_json` (array of per-step results).

---

### User Story 2 - Run Individual Named Step (Priority: P2)

A developer iterating on a specific SQL transform wants to re-run only the transforms step without regenerating data.

**Why this priority**: Re-running all 5 steps on every code change is wasteful. Step isolation is the second most-used workflow for iterative development.

**Independent Test**: Run `uv run src/run_pipeline.py --step transforms` and verify only step 3 executes.

**Acceptance Scenarios**:

1. **Given** a populated warehouse, **When** `uv run src/run_pipeline.py --step transforms` is run, **Then** only step 3 executes, no other steps run, and the exit code reflects that step's result.
2. **Given** an invalid step name `--step foo`, **When** the command runs, **Then** the CLI prints available step names and exits 1 without running anything.

---

### User Story 3 - Run From a Specific Step Onwards (Priority: P2)

A developer whose generate + ingest steps already ran wants to re-run from transforms onwards after fixing a SQL model.

**Why this priority**: `--from-step` is the natural companion to `--step` — it provides partial pipeline re-runs without starting from scratch.

**Independent Test**: Run `uv run src/run_pipeline.py --from-step transforms` and verify steps 3, 4, 5 execute in order.

**Acceptance Scenarios**:

1. **Given** steps 1–2 already ran, **When** `uv run src/run_pipeline.py --from-step transforms` is executed, **Then** steps 3 (transforms), 4 (checks), 5 (dashboard) run in order; steps 1–2 are skipped.
2. **Given** `--from-step ingest` is specified, **When** the pipeline runs, **Then** steps 2, 3, 4, 5 execute; step 1 is skipped.

---

### User Story 4 - Dry-Run Mode (Priority: P3)

A developer wants to preview which steps would run and in what order without actually executing anything.

**Why this priority**: Dry-run builds confidence before running expensive or destructive operations (e.g., clearing and regenerating data).

**Independent Test**: Run `uv run src/run_pipeline.py --dry-run` and verify it prints the execution plan and exits 0 without modifying any files or DuckDB.

**Acceptance Scenarios**:

1. **Given** any project state, **When** `uv run src/run_pipeline.py --dry-run` is run, **Then** the CLI prints each step that would execute with its command, no files are modified, DuckDB is not written to, and exit code is 0.
2. **Given** `--from-step checks --dry-run`, **When** run, **Then** only steps 4 and 5 appear in the dry-run plan.

---

### Edge Cases

- What happens when `npm` is not installed? Step 5 (dashboard) fails with a clear error message; the pipeline exits non-zero.
- What happens when the DuckDB file does not exist when step 2 runs? Step 2 handles this internally (existing ingest runner creates the file); the orchestrator passes it through.
- What happens when `--step` and `--from-step` are both provided? The CLI rejects the combination and exits 1 with an explanatory message.
- What happens when a step runs but returns a non-zero exit code? The pipeline halts immediately, marks that step as failed in the metadata, and exits non-zero.
- What happens when `pipeline_runs` table doesn't exist yet? The orchestrator creates it on first run (idempotent DDL).

## Clarifications

1. **Subprocess invocation**: Steps are invoked as subprocesses via `subprocess.run()` with `shell=False`. Each step's command is expressed as an argv list (e.g., `["uv", "run", "src/generate_transactions.py"]`). Step 5 uses `["npm", "run", "sources"]` with `cwd=dashboard/`.
2. **DAG structure**: The 5 steps form a linear chain (generate→ingest→transforms→checks→dashboard), but they are modelled as DAG nodes with explicit `depends_on` lists, reusing `src/transformer/graph.py` for resolution. This makes the system extensible.
3. **JSON log destination**: Structured JSON per step is printed to **stdout**. Human-readable progress messages go to **stderr**, keeping stdout clean for machine consumption.
4. **`steps_json` schema**: JSON array of objects stored in DuckDB: `[{"step": "generate", "status": "success", "duration_s": 1.23, "exit_code": 0}, ...]`.
5. **Step canonical names**: `generate`, `ingest`, `transforms`, `checks`, `dashboard`. These identifiers are used consistently in CLI args (`--step`, `--from-step`), JSON log output, and DuckDB metadata.
6. **Step failure capture**: On step failure, up to 2000 characters of combined stdout+stderr are captured and stored in the `steps_json` error field. The full output is still streamed to the terminal in real time.
7. **No parallel execution**: Steps run strictly serially. The DAG module is used for ordering and validation only, not concurrent scheduling.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST provide a single entrypoint `src/run_pipeline.py` runnable via `uv run src/run_pipeline.py`.
- **FR-002**: System MUST execute all 5 pipeline steps in fixed dependency order when no filters are specified.
- **FR-003**: System MUST support `--step <name>` to run exactly one named step.
- **FR-004**: System MUST support `--from-step <name>` to run from the named step through the end of the pipeline.
- **FR-005**: System MUST support `--dry-run` flag that prints the execution plan without executing any step or writing to DuckDB.
- **FR-006**: System MUST emit one structured JSON log line per step to stdout containing at minimum: `run_id`, `step`, `status` (`success`/`failed`/`skipped`), `duration_s`, `exit_code`.
- **FR-007**: System MUST write one `pipeline_runs` metadata row to DuckDB per non-dry-run invocation, containing: `run_id` (UUID), `status`, `started_at`, `finished_at`, `steps_json` (JSON array of per-step results).
- **FR-008**: System MUST halt the pipeline on the first step failure and not execute subsequent steps.
- **FR-009**: System MUST reuse `src/transformer/graph.py` for DAG resolution of pipeline steps.
- **FR-010**: System MUST define pipeline steps in a declarative structure (name, command, dependencies) rather than hardcoding execution order imperatively.
- **FR-011**: System MUST reject simultaneous use of `--step` and `--from-step` with a clear error message.
- **FR-012**: Step 5 (dashboard) MUST be executed as `npm run sources` in the `dashboard/` subdirectory.
- **FR-013**: System MUST create the `pipeline_runs` table if it does not exist (idempotent DDL on every run).
- **FR-014**: Exit code MUST be 0 on full success or dry-run, non-zero on any step failure.

### Key Entities

- **PipelineStep**: A declarative node in the DAG — name, shell command, list of dependency step names.
- **StepResult**: Per-step execution outcome — step name, status, duration, exit code, stdout/stderr snippet.
- **PipelineRun**: Aggregate run record — run_id, status, started_at, finished_at, list of StepResult.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: `uv run src/run_pipeline.py` completes all 5 steps on a clean machine and exits 0.
- **SC-002**: `uv run src/run_pipeline.py --step generate` runs only step 1 and exits 0.
- **SC-003**: `uv run src/run_pipeline.py --from-step transforms` runs exactly steps 3, 4, 5 and exits 0.
- **SC-004**: `uv run src/run_pipeline.py --dry-run` prints the plan and produces zero file system or DuckDB writes.
- **SC-005**: After any non-dry-run, `SELECT * FROM pipeline_runs` returns a row with correct status.
- **SC-006**: Each step's JSON log line is valid JSON parseable by `json.loads()`.
- **SC-007**: If step 3 fails, steps 4 and 5 do not execute and the process exits non-zero.
- **SC-008**: No external Python packages beyond `duckdb` are required (no Airflow, Prefect, Luigi, etc.).
