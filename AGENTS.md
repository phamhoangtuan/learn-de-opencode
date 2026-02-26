# learn_de Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-02-23

## Active Technologies
- GitHub Actions CI/CD (lint + test + E2E pipeline on push/PR to main) (007-github-actions-cicd)
- Workflow: `.github/workflows/ci.yml` — jobs: lint (ruff), test (pytest + md-report), pipeline (uv run src/run_pipeline.py --skip-steps dashboard) (007-github-actions-cicd)

- Python 3.11+ + DuckDB (pipeline orchestration, DAG resolution, subprocess execution, metadata tracking) (006-pipeline-orchestration)
- Pipeline orchestrator: `src/orchestrator/` module, `pipeline_runs` metadata table in `data/warehouse/transactions.duckdb` (006-pipeline-orchestration)

- Evidence.dev + DuckDB (interactive dashboard, read-only connection to warehouse) (005-evidence-dashboard)
- Dashboard pages: pipeline overview, financial analytics, ingestion health, data quality in `dashboard/` (005-evidence-dashboard)

- Python 3.11+ + DuckDB (SQL-based data quality checks, severity levels, metadata tracking) (004-data-quality-checks)
- DuckDB SQL checks: violation-based convention, check_runs + check_results metadata in `data/warehouse/transactions.duckdb` (004-data-quality-checks)

- Python 3.11+ + DuckDB (SQL transforms, DAG resolution, metadata tracking) (003-sql-transformations)
- DuckDB SQL transforms: staging views + mart aggregate tables in `data/warehouse/transactions.duckdb` (003-sql-transformations)

- Python 3.11+ + Polars (Parquet read, validation, dedup), DuckDB (warehouse storage, SQL queries), NumPy (inherited from Feature 001) (002-duckdb-ingestion)
- DuckDB embedded database at `data/warehouse/transactions.duckdb` (002-duckdb-ingestion)

- Python 3.11+ + Polars (data generation/output), Faker (names/merchants), NumPy (distributions) (001-synthetic-financial-data)

## Project Structure

```text
src/
tests/
```

## Commands

cd src [ONLY COMMANDS FOR ACTIVE TECHNOLOGIES][ONLY COMMANDS FOR ACTIVE TECHNOLOGIES] pytest [ONLY COMMANDS FOR ACTIVE TECHNOLOGIES][ONLY COMMANDS FOR ACTIVE TECHNOLOGIES] ruff check .

## Code Style

Python 3.11+: Follow standard conventions

## Recent Changes
- 009-scd2-accounts: Added [if applicable, e.g., PostgreSQL, CoreData, files or N/A]
- 007-github-actions-cicd: Added GitHub Actions CI/CD workflow. Workflow at `.github/workflows/ci.yml` — triggers on push/PR to main, runs lint + test + E2E pipeline.

- 006-pipeline-orchestration: Added Python 3.11+ + DuckDB (pipeline orchestration, DAG resolution, subprocess execution, metadata tracking). Run pipeline: `uv run src/run_pipeline.py`






<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
