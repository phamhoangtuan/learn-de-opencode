# Implementation Plan: GitHub Actions CI/CD Workflow

**Branch**: `007-github-actions-cicd` | **Date**: 2026-02-23 | **Spec**: [spec.md](./spec.md)  
**Input**: Feature specification from `/specs/007-github-actions-cicd/spec.md`

## Summary

Add a GitHub Actions CI/CD workflow (`.github/workflows/ci.yml`) that automatically runs lint, tests, and the full data pipeline on every push and PR to `main`. Extend the orchestrator CLI with a `--skip-steps` flag so the dashboard step (npm-dependent) can be excluded in CI. Add `pytest-md-report` for GitHub Actions job summaries.

## Technical Context

**Language/Version**: Python 3.11, YAML (GitHub Actions)  
**Primary Dependencies**: `uv` (package manager), `ruff` (linter), `pytest` (test runner), `pytest-md-report` (test summaries), `astral-sh/setup-uv@v4` (GitHub Action)  
**Storage**: DuckDB (created from scratch in CI workspace; `data/` not in VCS)  
**Testing**: pytest (existing suite: 18 unit + 7 integration test files)  
**Target Platform**: GitHub Actions `ubuntu-latest`, Python 3.11  
**Project Type**: Single Python project  
**Performance Goals**: Full CI (lint + test + pipeline) under 3 minutes on warm cache  
**Constraints**: No secrets/credentials; no Node.js/npm in CI; all DuckDB files ephemeral  
**Scale/Scope**: Single workflow file, ~2 changes to existing Python files

## Constitution Check

| Principle | Applicable? | Assessment |
|-----------|-------------|------------|
| I. Data Quality First | Yes | Pipeline job runs quality checks (step 4 of 5). CI fails if checks fail. |
| II. Metadata & Lineage | Yes | Orchestrator records pipeline run metadata to DuckDB even in CI. |
| III. Security & Privacy by Design | Yes | No secrets in workflow YAML. No credentials required. |
| IV. Integration & Interoperability | Partial | CI uses standard uv toolchain matching local dev. Dashboard excluded by design. |
| V. Architecture & Modeling Integrity | N/A | No schema changes. |

All applicable principles satisfied. No violations to justify.

## Project Structure

### Documentation (this feature)

```text
specs/007-github-actions-cicd/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
└── tasks.md             # Phase 2 output (from /speckit.tasks)
```

### Source Code Changes

```text
.github/
└── workflows/
    └── ci.yml                      # NEW: GitHub Actions workflow

src/
├── run_pipeline.py                 # MODIFY: add --skip-steps argument
└── orchestrator/
    └── runner.py                   # MODIFY: extend _resolve_active_steps()

pyproject.toml                      # MODIFY: add pytest-md-report to dev extras

tests/
└── unit/
    └── test_pipeline_runner.py     # MODIFY: add tests for --skip-steps behavior
```

## Implementation Phases

### Phase A: Orchestrator Extension (`--skip-steps`)

**Goal**: Allow callers to exclude named steps from a pipeline run.

**Changes to `src/orchestrator/runner.py`**:
- Extend `_resolve_active_steps()` with `skip_step_names: set[str] | None = None` parameter
- After existing step/from-step filtering, filter out any step in `skip_step_names`
- Skipped steps are emitted as `StepResult(status="skipped")` (existing pattern)

**Changes to `src/run_pipeline.py`**:
- Add `--skip-steps` argument (comma-separated, default `""`)
- Parse into a set of step names
- Validate each skip name against `_VALID_STEP_NAMES`
- Pass `skip_step_names` to `run_pipeline()`
- Update `run_pipeline()` signature to accept `skip_step_names`

**Changes to `src/orchestrator/models.py`** (if needed):
- None expected — `StepResult` already has `status="skipped"` support

### Phase B: Test Coverage for `--skip-steps`

**Changes to `tests/unit/test_pipeline_runner.py`**:
- Add test: `--skip-steps dashboard` excludes dashboard from active steps
- Add test: skip all steps → pipeline succeeds with no runs
- Add test: skip unknown step name → `SystemExit(1)` with helpful error message
- Add test: `--skip-steps` combined with `--step` → correct behavior (skip takes precedence? or error? — design: skip silently removes from active set)

### Phase C: Dev Dependency Addition (`pytest-md-report`)

**Changes to `pyproject.toml`**:
- Add `"pytest-md-report>=0.6"` to `[project.optional-dependencies].dev`
- Run `uv lock` to update `uv.lock`

### Phase D: GitHub Actions Workflow

**New file `.github/workflows/ci.yml`**:

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  lint:
    name: Lint
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
        with:
          enable-cache: true
          cache-dependency-glob: "uv.lock"
      - name: Install dependencies
        run: uv sync --extra dev
      - name: Ruff lint check
        run: uv run ruff check .

  test:
    name: Test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
        with:
          enable-cache: true
          cache-dependency-glob: "uv.lock"
      - name: Install dependencies
        run: uv sync --extra dev
      - name: Run pytest with report
        run: |
          uv run pytest \
            --md-report \
            --md-report-output=test-report.md \
            --md-report-verbose=0
      - name: Publish test summary
        if: always()
        run: cat test-report.md >> $GITHUB_STEP_SUMMARY

  pipeline:
    name: Pipeline (E2E)
    runs-on: ubuntu-latest
    needs: [test]
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
        with:
          enable-cache: true
          cache-dependency-glob: "uv.lock"
      - name: Install dependencies
        run: uv sync --extra dev
      - name: Run pipeline end-to-end (skip dashboard)
        run: uv run src/run_pipeline.py --skip-steps dashboard
```

**Key design decisions**:
- `lint` and `test` run in parallel (no `needs:` dependency between them)
- `pipeline` depends on `test` to avoid running expensive pipeline on broken code
- `astral-sh/setup-uv@v4` with `enable-cache: true` handles uv binary install + dependency cache (keyed on `uv.lock`)
- `uv sync --extra dev` installs all dependencies including pytest, ruff, pytest-md-report
- `--md-report-verbose=0` suppresses individual test lines in summary (shows counts only); `1` for per-test breakdown
- `if: always()` on summary publish ensures results appear even on test failure
- No `python-version` pin needed — uv reads `requires-python = ">=3.11"` from `pyproject.toml` and downloads the correct Python automatically via `setup-uv`

## Complexity Tracking

No constitution violations. All changes are additive or minimal modifications to existing files.
