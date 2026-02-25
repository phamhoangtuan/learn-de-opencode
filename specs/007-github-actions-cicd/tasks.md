# Tasks: GitHub Actions CI/CD Workflow

**Feature**: 007-github-actions-cicd
**Date**: 2026-02-23
**Spec**: `specs/007-github-actions-cicd/spec.md`
**Plan**: `specs/007-github-actions-cicd/plan.md`

## Phase A: Orchestrator Extension (`--skip-steps`)

- [x] T001 Extend `_resolve_active_steps()` in `src/orchestrator/runner.py` to accept `skip_step_names: set[str] | None` and filter those steps out as skipped
- [x] T002 Extend `run_pipeline()` in `src/orchestrator/runner.py` to accept and pass through `skip_step_names`
- [x] T003 Add `--skip-steps` CLI argument to `src/run_pipeline.py` (comma-separated step names, default `""`)
- [x] T004 Validate each name in `--skip-steps` against `_VALID_STEP_NAMES` in `src/run_pipeline.py`
- [x] T005 Pass parsed `skip_step_names` set from `main()` to `run_pipeline()`

## Phase B: Tests for `--skip-steps`

- [x] T006 Add test: `skip_step_names={"dashboard"}` excludes dashboard from active steps
- [x] T007 Add test: skipped step appears in `run.step_results` with `status="skipped"`
- [x] T008 Add test: skipping all steps → pipeline succeeds with zero executed steps
- [x] T009 Add test: unknown name in `--skip-steps` raises `SystemExit(1)` with helpful error
- [x] T010 Add test: `--skip-steps` combined with `--step` → only the named step runs, skip is applied to the active set

## Phase C: Add `pytest-md-report` Dev Dependency

- [x] T011 Add `"pytest-md-report>=0.6"` to `[project.optional-dependencies].dev` in `pyproject.toml`
- [x] T012 Run `uv lock` to regenerate `uv.lock` with the new dependency

## Phase D: GitHub Actions Workflow

- [x] T013 Create `.github/workflows/` directory
- [x] T014 Create `.github/workflows/ci.yml` with `lint` job: checkout → setup-uv (enable-cache) → `uv sync --extra dev` → `uv run ruff check .`
- [x] T015 Add `test` job to `ci.yml`: checkout → setup-uv (enable-cache) → `uv sync --extra dev` → `uv run pytest --md-report --md-report-output=test-report.md` → publish `test-report.md` to `$GITHUB_STEP_SUMMARY` with `if: always()`
- [x] T016 Add `pipeline` job to `ci.yml` with `needs: [test]`: checkout → setup-uv (enable-cache) → `uv sync --extra dev` → `uv run src/run_pipeline.py --skip-steps dashboard`
- [x] T017 Verify `on:` trigger covers `push` and `pull_request` to `main` branch

## Phase E: Polish & Docs

- [x] T018 Update `README.md` with a CI badge and brief CI section
- [x] T019 Update `AGENTS.md` with feature 007 entry
