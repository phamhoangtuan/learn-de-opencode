# Data Model: Pipeline Orchestration Layer

**Feature**: 006-pipeline-orchestration  
**Date**: 2026-02-23

## New DuckDB Table

### pipeline_runs

One row per non-dry-run orchestrator invocation. Records the overall outcome and per-step details.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| run_id | VARCHAR | PRIMARY KEY | UUID for this pipeline run |
| status | VARCHAR | NOT NULL | Overall status: `success`, `failed`, `partial` |
| started_at | TIMESTAMPTZ | NOT NULL | When the orchestrator started |
| finished_at | TIMESTAMPTZ | | When the orchestrator finished (NULL if crashed mid-run) |
| steps_json | VARCHAR | NOT NULL | JSON array of per-step result objects |
| dry_run | BOOLEAN | NOT NULL DEFAULT FALSE | Whether this was a dry-run invocation |

### `steps_json` element schema

Each element in the JSON array:

```json
{
  "step": "generate",
  "status": "success",
  "duration_s": 1.23,
  "exit_code": 0,
  "error_output": null
}
```

| Field | Type | Description |
|-------|------|-------------|
| step | string | Step canonical name |
| status | string | `success`, `failed`, `skipped` |
| duration_s | float | Wall-clock time in seconds |
| exit_code | int or null | Subprocess exit code (null for skipped) |
| error_output | string or null | Up to 2000 chars of stderr on failure |

## Python Domain Models

### PipelineStep (dataclass)

Declarative node in the orchestration DAG.

| Field | Type | Description |
|-------|------|-------------|
| name | str | Canonical step identifier (e.g., `generate`) |
| command | list[str] | Subprocess argv (e.g., `["uv", "run", "src/generate_transactions.py"]`) |
| cwd | str or None | Working directory for subprocess (None = project root) |
| depends_on | list[str] | Names of steps this step depends on |

### StepResult (dataclass)

Outcome of executing one pipeline step.

| Field | Type | Description |
|-------|------|-------------|
| step | str | Step name |
| status | str | `success`, `failed`, `skipped` |
| duration_s | float | Execution duration in seconds |
| exit_code | int or None | Subprocess exit code (None for skipped) |
| error_output | str or None | Captured stderr snippet on failure |

### PipelineRun (dataclass)

Aggregate result for one orchestrator invocation.

| Field | Type | Description |
|-------|------|-------------|
| run_id | str | UUID string |
| status | str | `success`, `failed` |
| started_at | datetime | UTC start time |
| finished_at | datetime or None | UTC end time |
| step_results | list[StepResult] | All step outcomes (including skipped) |
| dry_run | bool | Whether this was a dry-run |

## Relationship to Existing Tables

```
Existing (Features 002–004):
  transactions              (Feature 002)
  quarantine                (Feature 002)
  ingestion_runs            (Feature 002)
  stg_transactions          (VIEW, Feature 003)
  daily_spend_by_category   (TABLE, Feature 003)
  monthly_account_summary   (TABLE, Feature 003)
  transform_runs            (Feature 003)
  check_runs                (Feature 004)
  check_results             (Feature 004)

New (Feature 006):
  pipeline_runs             (orchestration metadata)
```

The `pipeline_runs` table does not foreign-key into the other metadata tables, as the orchestrator invokes existing runners as subprocesses. Cross-run correlation can be done by comparing `started_at` timestamps if needed.
