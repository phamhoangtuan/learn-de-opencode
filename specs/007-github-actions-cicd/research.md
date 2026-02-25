# Research: GitHub Actions CI/CD for learn_de

**Feature**: 007-github-actions-cicd  
**Date**: 2026-02-23

## Existing Codebase Analysis

### Orchestrator Architecture
- `src/run_pipeline.py`: CLI entrypoint with `--step`, `--from-step`, `--dry-run`, `--db-path`, `--verbose` flags
- `src/orchestrator/runner.py`: `_resolve_active_steps()` filters steps; `run_pipeline()` halts on first failure
- `src/orchestrator/models.py`: `PipelineStep`, `StepResult`, `PipelineRun` dataclasses
- Dashboard step uses `npm run sources` in `dashboard/` cwd — incompatible with lean CI runner

### Existing `--step`/`--from-step` Support
The orchestrator already handles filtering, but has no way to exclude the last step (dashboard) while running all others. Options evaluated:
1. Call `--step` four times in CI — loses single-command E2E semantics
2. Add `--skip-steps dashboard` flag — minimal code change, cleanest UX
3. Check `CI=true` env var in `run_pipeline.py` — implicit, harder to test
4. Add `--until-step checks` — semantically clearest, run up to and including a named step

**Decision**: Add `--skip-steps` (comma-separated) to `run_pipeline.py`. Filters in `_resolve_active_steps()` in runner. Minimal diff, fully tested, no implicit behavior.

### Test Suite
- `tests/unit/` — 18 unit test files, fast, no external I/O
- `tests/integration/` — 7 integration test files, use `tmp_path`, create real DuckDB instances
- `pyproject.toml`: `testpaths = ["tests"]`, `addopts = "-v --tb=short"`
- All tests run with `uv run pytest` (installs dev extras via `pyproject.toml`)

### Dependency Management
- `pyproject.toml` + `uv.lock` (lockfile present, ensures reproducible installs)
- `uv sync --extra dev` installs dev dependencies (pytest, ruff, pytest-cov, scipy)
- uv installation in CI: `astral-sh/setup-uv@v4` action — installs uv binary, handles PATH
- Cache strategy: uv caches downloaded packages in `~/.cache/uv`; can also cache `.venv`

### Ruff Configuration
- `[tool.ruff]` in `pyproject.toml`: target `py311`, line-length 99
- Checks: E, W, F, I (isort), N (naming), UP (pyupgrade), B (bugbear), SIM, RUF
- Run: `uv run ruff check .` — exits 0 on clean, non-zero on violations

### Job Summary / Test Reporting
- GitHub Actions supports writing Markdown to `$GITHUB_STEP_SUMMARY`
- `pytest-md-report` plugin: adds `--md-report` flag, writes Markdown summary
  - Install: add to dev extras in `pyproject.toml`
  - Usage: `pytest --md-report --md-report-output=report.md`
  - Then: `cat report.md >> $GITHUB_STEP_SUMMARY`
- Alternative: `pytest --junitxml=results.xml` + `EnricoMi/publish-unit-test-result-action` (heavier, Node.js based)
- **Decision**: Use `pytest-md-report` (pure Python, matches uv-only toolchain, no JS runner)

### Data Directory
- `data/` is in `.gitignore`, not checked out in CI
- Pipeline creates `data/raw/` (Parquet), `data/warehouse/` (DuckDB) from scratch
- No pre-seeding needed; synthetic data generator is deterministic with a seed
- DuckDB file path: `data/warehouse/transactions.duckdb` — created by ingest step

### GitHub Actions Cache
- `actions/cache@v4` with key `ubuntu-latest-uv-${{ hashFiles('uv.lock') }}`
- Cache path: `~/.cache/uv` (uv download cache)
- Restore keys for partial matches: `ubuntu-latest-uv-`
- `astral-sh/setup-uv@v4` has built-in cache support via `enable-cache: true`

## External References

- uv GitHub Actions: https://docs.astral.sh/uv/guides/integration/github/
- `astral-sh/setup-uv` action: https://github.com/astral-sh/setup-uv
- GitHub Actions job summaries: https://docs.github.com/en/actions/writing-workflows/choosing-what-your-workflow-does/workflow-commands-for-github-actions#adding-a-job-summary
- pytest-md-report: https://pypi.org/project/pytest-md-report/

## Risk Assessment

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Integration tests slow due to DuckDB I/O | Medium | Tests use `tmp_path` (tmpfs on Linux, fast) |
| uv cache miss on first run | High (expected) | Cold run still < 3 min; cache hits from run 2 onward |
| Dashboard step fails if npm unavailable | Low (skipped) | `--skip-steps dashboard` prevents execution |
| DuckDB file permissions in workspace | Low | GitHub Actions workspace is writable by default |
| `uv.lock` drift (pyproject.toml changed, lock not updated) | Low | `uv sync` detects and fails; developer must update lock |
