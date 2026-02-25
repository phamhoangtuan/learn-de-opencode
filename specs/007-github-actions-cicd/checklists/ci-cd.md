# CI/CD Checklist: GitHub Actions CI/CD Workflow

**Purpose**: Quality gates for the GitHub Actions workflow, orchestrator extension, and test reporting implementation  
**Created**: 2026-02-23  
**Feature**: [spec.md](../spec.md)

## Workflow Triggers & Configuration

- [ ] CHK001 Workflow triggers on `push` to `main` branch
- [ ] CHK002 Workflow triggers on `pull_request` targeting `main`
- [ ] CHK003 Workflow uses `ubuntu-latest` runner for all jobs
- [ ] CHK004 Python version is pinned or managed via uv's `requires-python` (no floating version)
- [ ] CHK005 Workflow file located at `.github/workflows/ci.yml`
- [ ] CHK006 Workflow has a descriptive `name:` field (e.g., "CI")
- [ ] CHK007 No secrets or credentials are referenced anywhere in the workflow YAML

## Job: Lint

- [ ] CHK008 Lint job checks out the repository (`actions/checkout@v4`)
- [ ] CHK009 Lint job installs uv via `astral-sh/setup-uv@v4`
- [ ] CHK010 Lint job installs all dev dependencies (`uv sync --extra dev`)
- [ ] CHK011 Lint job runs `uv run ruff check .` (or equivalent)
- [ ] CHK012 Lint job fails (non-zero exit) when ruff finds violations
- [ ] CHK013 Lint job has a meaningful step name (e.g., "Ruff lint check")

## Job: Test

- [ ] CHK014 Test job checks out repository and installs uv + dependencies
- [ ] CHK015 Test job runs `uv run pytest` (respects `pyproject.toml` config)
- [ ] CHK016 Test job fails if any pytest test fails
- [ ] CHK017 Test job generates a Markdown test report (`pytest-md-report`)
- [ ] CHK018 Test job publishes the Markdown report to `$GITHUB_STEP_SUMMARY`
- [ ] CHK019 Summary publish step uses `if: always()` so it runs even on test failure
- [ ] CHK020 Test job has a meaningful step name (e.g., "Run pytest with report")

## Job: Pipeline (E2E)

- [ ] CHK021 Pipeline job depends on test job (`needs: [test]`)
- [ ] CHK022 Pipeline job runs `uv run src/run_pipeline.py --skip-steps dashboard`
- [ ] CHK023 Pipeline job fails if `run_pipeline.py` exits non-zero
- [ ] CHK024 Pipeline job does NOT require Node.js or npm
- [ ] CHK025 Pipeline job executes all 4 data steps: generate, ingest, transforms, checks
- [ ] CHK026 Pipeline job has a meaningful name (e.g., "Pipeline (E2E)")

## Dependency Caching

- [ ] CHK027 `astral-sh/setup-uv@v4` is used with `enable-cache: true`
- [ ] CHK028 Cache is keyed on `uv.lock` file hash (`cache-dependency-glob: "uv.lock"`)
- [ ] CHK029 Cache restore key includes a prefix for partial matches (handled by setup-uv)
- [ ] CHK030 Cache is shared across all jobs (same key = same cache hit)

## Orchestrator Extension (`--skip-steps`)

- [ ] CHK031 `--skip-steps` argument added to `run_pipeline.py` argument parser
- [ ] CHK032 Each step name in `--skip-steps` is validated against known step names
- [ ] CHK033 Invalid step name in `--skip-steps` exits with code 1 and clear error message
- [ ] CHK034 Skipped steps are recorded as `status="skipped"` in metadata (consistent with existing behavior)
- [ ] CHK035 `_resolve_active_steps()` in `runner.py` accepts `skip_step_names: set[str] | None`
- [ ] CHK036 `run_pipeline()` in `runner.py` accepts and passes `skip_step_names`
- [ ] CHK037 `--skip-steps` can be combined with `--step` or `--from-step` without conflict

## Dependency Changes (`pytest-md-report`)

- [ ] CHK038 `pytest-md-report>=0.6` added to `[project.optional-dependencies].dev` in `pyproject.toml`
- [ ] CHK039 `uv.lock` is updated to include `pytest-md-report` and its transitive dependencies
- [ ] CHK040 `uv sync --extra dev` installs `pytest-md-report` successfully

## Test Coverage

- [ ] CHK041 Unit tests cover `--skip-steps` with a known step (dashboard) — step excluded from active
- [ ] CHK042 Unit tests cover `--skip-steps` with unknown step name — `SystemExit(1)`
- [ ] CHK043 Unit tests cover `--skip-steps` with all steps — pipeline completes with all skipped
- [ ] CHK044 Unit tests cover `--skip-steps` combined with `--from-step` — correct intersection behavior
- [ ] CHK045 Existing orchestrator tests remain passing after `runner.py` signature change

## Security & Quality Gates

- [ ] CHK046 No credentials, tokens, or API keys appear in the workflow YAML
- [ ] CHK047 All `uses:` actions are pinned to a specific version tag (e.g., `@v4`, not `@latest`)
- [ ] CHK048 Workflow does not grant excessive permissions (no `permissions: write-all`)
- [ ] CHK049 CI fails fast on first job failure (pipeline job skipped if test fails)
- [ ] CHK050 Ruff check covers the entire project directory (`.` scope, not just `src/`)

## Notes

- Check items off as completed: `[x]`
- Items CHK031–CHK037 must be completed before CHK022 in the workflow can function correctly
- Items CHK038–CHK040 must be completed before CHK017 in the workflow can function correctly
- The `astral-sh/setup-uv@v4` action's built-in caching is preferred over manual `actions/cache@v4` for simplicity
