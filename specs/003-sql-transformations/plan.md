# Implementation Plan: SQL Transformations Layer

**Branch**: `003-sql-transformations` | **Date**: 2026-02-19 | **Spec**: [spec.md](spec.md)

## Summary

Build a lightweight dbt-inspired SQL transformation layer that executes `.sql` files in dependency order against the existing DuckDB warehouse (populated by Feature 002). The system creates a staging view (`stg_transactions`) standardizing the raw ingestion table, and two mart aggregate tables (`daily_spend_by_category`, `monthly_account_summary`). A Python runner resolves dependencies via Kahn's topological sort, executes each model as a CREATE OR REPLACE statement for full-refresh idempotency, and tracks executions in a `transform_runs` metadata table. SQL files declare dependencies via `-- depends_on:` comment headers.

## Technical Context

**Language/Version**: Python 3.11+  
**Primary Dependencies**: DuckDB (SQL execution, warehouse connection), argparse (CLI)  
**Storage**: DuckDB embedded database at `data/warehouse/transactions.duckdb` (shared with Feature 002)  
**Testing**: pytest with unit, integration, and quality tiers  
**Target Platform**: macOS/Linux local development machine  
**Project Type**: Single project (extending existing `src/` layout)  
**Performance Goals**: Full transform pipeline under 10 seconds for 100K records  
**Constraints**: Single-user, local-only, no concurrent writes  
**Scale/Scope**: 3 SQL models (1 staging view, 2 mart tables), extensible to more

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Data Quality First (DAMA-DMBOK Area 11) | PASS | Staging view standardizes raw data; mart tables filter to completed transactions only; tests validate output correctness |
| II. Metadata & Lineage (DAMA-DMBOK Area 10) | PASS | `transform_runs` metadata table tracks every execution; staging view preserves full lineage columns (source_file, ingested_at, run_id) from raw data |
| III. Security & Privacy by Design (DAMA-DMBOK Area 5) | PASS | Local-only pipeline, no network exposure. No PII transformation. SQL files are read-only inputs. |
| IV. Integration & Interoperability (DAMA-DMBOK Area 6) | PASS | Pure SQL transforms (no vendor-specific extensions beyond standard DuckDB SQL). Outputs are standard DuckDB tables/views queryable by any SQL client. |
| V. Architecture & Modeling Integrity (DAMA-DMBOK Area 2 & 3) | PASS | Explicit data model (data-model.md), staging/mart layer separation follows medallion architecture pattern, naming conventions consistent with project |

All gates pass. No violations to justify.

**Post-Phase 1 re-check**: All gates still pass. Data model defines clear entity schemas (Principle V), staging view preserves lineage (Principle II), mart tables enforce data quality filters (Principle I).

## Project Structure

### Documentation (this feature)

```text
specs/003-sql-transformations/
├── spec.md              # Feature specification
├── plan.md              # This file
├── research.md          # Technology decisions
├── data-model.md        # Entity schemas and relationships
├── checklists/
│   └── data-quality.md  # Data quality checklist
└── tasks.md             # Task list
```

### Source Code (repository root)

```text
src/
├── run_transforms.py          # CLI entrypoint (uv run, PEP 723 inline deps)
├── transforms/                # SQL model files (new)
│   ├── staging__stg_transactions.sql
│   ├── mart__daily_spend_by_category.sql
│   └── mart__monthly_account_summary.sql
└── transformer/               # Python runner module (new)
    ├── __init__.py
    ├── runner.py              # Transform orchestration (dependency resolution + execution)
    ├── parser.py              # SQL file parsing (model name, dependencies, SQL content)
    ├── graph.py               # DAG resolution (Kahn's algorithm, cycle detection)
    └── models.py              # Domain models (TransformModel, TransformRun)

tests/
├── conftest.py                # Extend with transform fixtures
├── unit/
│   ├── test_parser.py         # SQL file parsing tests
│   ├── test_graph.py          # DAG resolution tests
│   └── test_runner.py         # Runner logic tests (mocked DB)
├── integration/
│   └── test_transforms.py     # End-to-end transform pipeline tests
└── quality/
    └── test_transform_quality.py  # Output data quality assertions
```

**Structure Decision**: Extends the existing single-project layout. New transform code lives in `src/transformer/` as a distinct module, parallel to `src/ingestion/`. SQL files live in `src/transforms/` separate from Python code. The CLI entrypoint follows the same PEP 723 pattern as `src/generate_transactions.py` and `src/ingest_transactions.py`.

## Complexity Tracking

No violations. All design choices align with constitution principles.
