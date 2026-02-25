# Feature Specification: GitHub Actions CI/CD Workflow

**Feature Branch**: `007-github-actions-cicd`  
**Created**: 2026-02-23  
**Status**: Draft  
**Input**: User description: "GitHub Actions CI/CD workflow for the learn_de data pipeline. Add a GitHub Actions workflow at .github/workflows/ci.yml that runs automatically on every push and pull request to main. The workflow must: (1) run the full pytest suite (uv run pytest) and fail the PR if any test fails, (2) run ruff linting (uv run ruff check .) and fail on violations, (3) run the pipeline end-to-end in CI using the orchestrator (uv run src/run_pipeline.py) — generate synthetic data, ingest to DuckDB, run transforms, run quality checks — and fail if any step fails, (4) cache uv/pip dependencies between runs for speed, (5) run on ubuntu-latest with Python 3.11, (6) use uv for dependency management (matching local dev), (7) skip the dashboard sources step in CI (no Node.js/npm needed), (8) report test results as GitHub Actions job summaries. No secrets or credentials needed — everything runs locally with DuckDB files in the workflow workspace."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Developer Pushes Code and Gets Immediate Feedback (Priority: P1)

A developer pushes a commit or opens a PR against `main`. GitHub Actions automatically triggers the CI workflow, which runs linting, tests, and the full pipeline end-to-end. If any step fails, the PR is blocked and the developer sees exactly which job failed in the GitHub UI.

**Why this priority**: This is the core value of CI — automated gatekeeping that prevents broken code from merging into main. Without this, all other stories are moot.

**Independent Test**: Push a commit with a deliberate ruff violation and verify the "Lint" job fails with a non-zero exit code and the PR is marked as failing.

**Acceptance Scenarios**:

1. **Given** a push or PR to `main`, **When** the workflow triggers, **Then** all three jobs (lint, test, pipeline) run automatically on `ubuntu-latest` with Python 3.11.
2. **Given** a ruff linting violation in the code, **When** the Lint job runs, **Then** `uv run ruff check .` exits non-zero and the job is marked failed.
3. **Given** a failing pytest test, **When** the Test job runs, **Then** `uv run pytest` exits non-zero and the PR is blocked from merging.
4. **Given** a pipeline step failure (e.g., data generation error), **When** the Pipeline job runs, **Then** `uv run src/run_pipeline.py` exits non-zero and the job is marked failed.

---

### User Story 2 - Fast CI via Dependency Caching (Priority: P2)

A developer's second push in a day benefits from cached Python/uv dependencies, reducing CI run time significantly compared to a cold install.

**Why this priority**: Developer experience depends on fast feedback loops. Cold installs on every run waste time and GitHub Actions minutes. Caching is a baseline expectation for professional CI.

**Independent Test**: Run the workflow twice on the same dependency lockfile and verify the second run shows a cache hit in the "Setup uv" or "Cache dependencies" step logs.

**Acceptance Scenarios**:

1. **Given** dependencies are unchanged between two pushes, **When** the second CI run executes, **Then** the cache restore step reports a cache hit and the install step is skipped or near-instant.
2. **Given** a change to `pyproject.toml` or `uv.lock`, **When** the next CI run executes, **Then** the cache is invalidated and dependencies are re-installed from scratch.

---

### User Story 3 - Test Results Visible as Job Summary (Priority: P3)

After a CI run, the developer can see a test results summary (pass/fail counts, which tests failed) directly in the GitHub Actions job summary tab — without downloading artifacts or reading raw logs.

**Why this priority**: Job summaries reduce the friction of diagnosing failures. Developers can see results at a glance without scrolling through raw log output.

**Independent Test**: Run the workflow with at least one test passing and verify the GitHub Actions job summary page for the Test job shows a formatted summary (e.g., pytest results with counts).

**Acceptance Scenarios**:

1. **Given** pytest runs successfully, **When** the Test job completes, **Then** the GitHub Actions job summary contains a Markdown-formatted report of test results (pass/fail counts, duration).
2. **Given** one or more tests fail, **When** the Test job completes, **Then** the job summary lists the failing test names, and the job exits with a non-zero code.

---

### User Story 4 - Pipeline Runs End-to-End Without Dashboard Step (Priority: P2)

The full data pipeline (data generation → DuckDB ingestion → SQL transforms → quality checks) runs in CI without requiring Node.js/npm or the Evidence.dev dashboard. The CI environment stays lean.

**Why this priority**: The pipeline is the core product. Validating it end-to-end in CI ensures data correctness. The dashboard is UI-only and has no business logic to test in CI.

**Independent Test**: Run `uv run src/run_pipeline.py` in a clean Ubuntu environment without Node.js installed and verify it completes successfully without errors related to npm or the dashboard.

**Acceptance Scenarios**:

1. **Given** a clean `ubuntu-latest` runner with no Node.js, **When** `uv run src/run_pipeline.py` runs, **Then** it completes all pipeline steps (generate, ingest, transform, quality checks) successfully.
2. **Given** the orchestrator is configured to skip the dashboard step in CI, **When** `CI=true` environment variable is set or a `--skip-dashboard` flag is passed, **Then** the dashboard step is skipped without error.
3. **Given** any pipeline step fails (non-zero exit), **When** the Pipeline job runs, **Then** the job exits non-zero and subsequent steps do not execute.

---

### Edge Cases

- What happens when the `uv.lock` file is missing or corrupted? The cache key should fall back gracefully and re-run a full install.
- What happens when DuckDB file permissions are incorrect in the workspace? The pipeline should fail fast with a clear error message.
- What happens when a new dependency is added to `pyproject.toml` but `uv.lock` is not updated? The CI should detect the inconsistency and fail during the install step.
- What happens when the synthetic data generation produces an empty dataset? The ingestion step should fail with a validation error (per Feature 002 requirements).
- What happens if `run_pipeline.py` exits 0 but a quality check produced critical violations? This edge case is a product concern — the orchestrator must surface quality check failures as non-zero exits.

## Clarifications

**Q1: How should the dashboard step be skipped in CI without changing the GitHub Actions YAML per step?**  
The orchestrator (`run_pipeline.py`) already accepts `--from-step` and `--step` flags. The cleanest approach is to extend the orchestrator with a `--skip-steps` CLI argument (comma-separated step names) so that the CI workflow can call `uv run src/run_pipeline.py --skip-steps dashboard`. This requires a minimal code change to `run_pipeline.py` and `runner.py`, keeps the single-command interface, and avoids the brittleness of calling 4 separate `--step` invocations in CI (which would lose end-to-end pipeline validation).

**Q2: How should pytest results be published as a job summary?**  
Use `pytest-md-report` or the `--md-report` plugin to generate a Markdown file, then write it to `$GITHUB_STEP_SUMMARY`. Alternatively, use `pytest --junitxml=test-results.xml` and a community action like `EnricoMi/publish-unit-test-result-action` to publish results. The simpler approach (no extra dependency) is to pipe pytest output to a Markdown-formatted summary using `tee` and GitHub Actions shell multiline syntax. Decision: use `pytest-md-report` (lightweight, pure Python, no JS runner needed) as an optional dev dependency.

**Q3: What is the cache key strategy for uv?**  
Cache the uv download cache (typically `~/.cache/uv`) keyed on `hashFiles('uv.lock')`. This is the standard approach for uv in GitHub Actions as recommended by the `astral-sh/setup-uv` action. A secondary cache for the virtual environment (`.venv`) keyed on `uv.lock` + `pyproject.toml` hash avoids re-running `uv sync` on every run.

**Q4: Should jobs run sequentially or in parallel?**  
Lint and test can run in parallel (no data dependency). Pipeline must run after test (to ensure code is correct before running the expensive pipeline). This reduces wall-clock time for green runs while ensuring correct ordering.

**Q5: How should the workflow handle the `data/` directory in CI?**  
The `data/` directory (DuckDB warehouse, Parquet files) is in `.gitignore` and will not be checked out. The pipeline must create it from scratch. The orchestrator's `generate` step creates `data/raw/`, and `ingest` creates `data/warehouse/transactions.duckdb`. No special setup needed — just ensure the runner has write permissions to the workspace (default on GitHub Actions).

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The workflow MUST trigger on every `push` and `pull_request` event targeting the `main` branch.
- **FR-002**: The workflow MUST run on `ubuntu-latest` with Python 3.11.
- **FR-003**: The workflow MUST use `uv` for all Python dependency installation and script execution, matching local development setup.
- **FR-004**: The workflow MUST run `uv run ruff check .` and fail (non-zero exit) if any linting violation is found.
- **FR-005**: The workflow MUST run `uv run pytest` and fail (non-zero exit) if any test fails.
- **FR-006**: The workflow MUST run `uv run src/run_pipeline.py` end-to-end and fail if any pipeline step returns non-zero.
- **FR-007**: The pipeline execution in CI MUST skip the Evidence.dev dashboard step (no Node.js/npm dependency).
- **FR-008**: The workflow MUST cache uv/pip dependencies keyed on the `uv.lock` file hash (or equivalent) to reduce install time on subsequent runs.
- **FR-009**: The workflow MUST publish pytest results as a GitHub Actions job summary using Markdown.
- **FR-010**: The workflow MUST NOT require any secrets, credentials, or environment variables beyond what is available by default in GitHub Actions.
- **FR-011**: The workflow file MUST be located at `.github/workflows/ci.yml`.
- **FR-012**: Each logical phase (lint, test, pipeline) MUST be a separate job or clearly labeled step so failures are attributable.

### Key Entities

- **Workflow File** (`.github/workflows/ci.yml`): YAML definition of the CI pipeline. Triggers, jobs, steps.
- **Job: lint**: Runs `uv run ruff check .`. Fails PR on violations.
- **Job: test**: Runs `uv run pytest`. Publishes results to job summary.
- **Job: pipeline**: Runs `uv run src/run_pipeline.py` with dashboard step skipped.
- **Dependency Cache**: uv/pip cache keyed on `uv.lock` hash, stored as a GitHub Actions cache artifact.
- **CI Environment Variable**: A mechanism (env var `CI=true` or orchestrator flag) to signal skip-dashboard behavior.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A push to `main` triggers the workflow within 30 seconds of the push event.
- **SC-002**: On a warm cache (no dependency changes), the total CI wall-clock time is under 3 minutes.
- **SC-003**: A commit with a ruff violation causes the lint job to fail with exit code 1 and the PR to be blocked.
- **SC-004**: A commit with a failing test causes the test job to fail and the PR to be blocked.
- **SC-005**: A commit that breaks the pipeline (e.g., invalid SQL transform) causes the pipeline job to fail.
- **SC-006**: The GitHub Actions job summary for the test job contains formatted test results (counts, names of failures).
- **SC-007**: The full CI pipeline (lint + test + pipeline) completes without errors on a clean commit, producing a green check on the PR.
- **SC-008**: The CI environment never attempts to install Node.js or run npm commands.
