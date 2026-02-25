# Data Model: GitHub Actions CI/CD

**Feature**: 007-github-actions-cicd  
**Date**: 2026-02-23

## Overview

This feature is primarily a DevOps/CI configuration feature. There is no new data model for the warehouse. The key "data" artifacts are:

1. **Workflow YAML** (`.github/workflows/ci.yml`) — the GitHub Actions workflow definition
2. **Orchestrator CLI extension** — `--skip-steps` flag added to `run_pipeline.py`
3. **Test report artifact** — Markdown test results written to `$GITHUB_STEP_SUMMARY`
4. **Dependency additions** — `pytest-md-report` added to dev extras in `pyproject.toml`

## Workflow Structure

```yaml
# .github/workflows/ci.yml — logical structure

name: CI
on:
  push:    { branches: [main] }
  pull_request: { branches: [main] }

jobs:
  lint:
    runs-on: ubuntu-latest
    steps: [checkout, setup-uv, install deps, ruff check]

  test:
    runs-on: ubuntu-latest
    steps: [checkout, setup-uv, install deps, pytest with md-report, publish summary]

  pipeline:
    runs-on: ubuntu-latest
    needs: [test]          # only run if tests pass
    steps: [checkout, setup-uv, install deps, run_pipeline --skip-steps dashboard]
```

## Orchestrator CLI Extension

```python
# run_pipeline.py — new argument
parser.add_argument(
    "--skip-steps",
    metavar="NAMES",
    default="",
    help="Comma-separated step names to skip (e.g., --skip-steps dashboard)",
)

# runner.py — _resolve_active_steps extended signature
def _resolve_active_steps(
    ordered_steps: list[PipelineStep],
    step_name: str | None,
    from_step_name: str | None,
    skip_step_names: set[str] | None = None,   # NEW
) -> list[PipelineStep]:
```

## Dependency Changes

```toml
# pyproject.toml — dev extras addition
[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-cov>=5.0",
    "ruff>=0.9",
    "scipy>=1.14",
    "pytest-md-report>=0.6",   # NEW: for GitHub Actions job summaries
]
```

## Cache Key Schema

```text
Cache name:  ubuntu-latest-uv-<hash>
Cache path:  ~/.cache/uv
Key:         ubuntu-latest-uv-${{ hashFiles('uv.lock') }}
Restore key: ubuntu-latest-uv-
```

## No Warehouse Schema Changes

This feature adds no new DuckDB tables, views, or schema changes. The existing `pipeline_runs` metadata table (Feature 006) is populated when the pipeline runs in CI, but its schema is unchanged.
