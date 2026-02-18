# Implementation Plan: DuckDB Ingestion Pipeline

**Branch**: `002-duckdb-ingestion` | **Date**: 2026-02-18 | **Spec**: [spec.md](spec.md)

## Summary

Build a data ingestion pipeline that reads raw Parquet files from `data/raw/` (produced by Feature 001's synthetic financial data generator) and loads them into a local DuckDB warehouse. The pipeline implements two-tier schema validation with quarantine, deduplication by `transaction_id` via anti-join, per-record lineage tracking with inline metadata columns, and date-based query optimization using a computed `transaction_date` column. The architecture uses Polars for read/validate and DuckDB's Python API for write, leveraging zero-copy Arrow interchange.

## Technical Context

**Language/Version**: Python 3.11+  
**Primary Dependencies**: Polars (Parquet read, validation, dedup), DuckDB (warehouse storage, SQL queries), NumPy (inherited from Feature 001)  
**Storage**: DuckDB embedded database at `data/warehouse/transactions.duckdb`  
**Testing**: pytest with unit, integration, and quality tiers  
**Target Platform**: macOS/Linux local development machine  
**Project Type**: Single project (extending existing `src/` layout)  
**Performance Goals**: 1M records ingested in under 60 seconds  
**Constraints**: Single-user, local-only, no concurrent writes  
**Scale/Scope**: Up to millions of transaction records from multiple Parquet files

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Data Quality First (DAMA-DMBOK Area 11) | PASS | Two-tier validation (FR-004/FR-005), quarantine (FR-006/FR-007/FR-008), data quality metrics in run summary (FR-016) |
| II. Metadata & Lineage (DAMA-DMBOK Area 10) | PASS | Per-record lineage columns (FR-012), unique run IDs (FR-013), ingestion_runs tracking table |
| III. Security & Privacy by Design (DAMA-DMBOK Area 5) | PASS | Local-only pipeline, no network exposure. No PII beyond what exists in source files. DuckDB file permissions follow OS-level access control |
| IV. Integration & Interoperability (DAMA-DMBOK Area 6) | PASS | Standard Parquet input format (FR-001), DuckDB SQL interface for consumers, no vendor lock-in |
| V. Architecture & Modeling Integrity (DAMA-DMBOK Area 2 & 3) | PASS | Explicit data model with constraints (data-model.md), naming conventions follow existing project patterns, schema versioning documented |

All gates pass. No violations to justify.

**Post-Phase 1 re-check**: All gates still pass. Data model adds lineage columns (Principle II), validation rules enforce quality gates (Principle I), Parquet input preserves interoperability (Principle IV).

## Project Structure

### Documentation (this feature)

```text
specs/002-duckdb-ingestion/
├── spec.md              # Feature specification
├── plan.md              # This file
├── research.md          # Phase 0: technology decisions
├── data-model.md        # Phase 1: entity schemas and relationships
├── quickstart.md        # Phase 1: usage guide
├── checklists/
│   └── requirements.md  # Spec quality checklist
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code (repository root)

```text
src/
├── ingest_transactions.py     # CLI entrypoint (uv run, PEP 723 inline deps)
├── ingestion/                 # New module for Feature 002
│   ├── __init__.py
│   ├── pipeline.py            # Pipeline orchestration (run coordinator)
│   ├── validator.py           # Two-tier schema + value validation
│   ├── loader.py              # DuckDB connection, table creation, inserts
│   ├── dedup.py               # Anti-join deduplication logic
│   └── models.py              # Ingestion domain models (RunResult, etc.)
├── models/                    # (Feature 001) Transaction, Account, Merchant
│   └── transaction.py         # TransactionSchema used for validation reference
├── lib/                       # (Feature 001) Shared utilities
│   ├── logging_config.py      # Reused for pipeline logging
│   └── validators.py          # May be extended for ingestion validation
└── data/
    └── merchants.json         # (Feature 001) Reference data

data/
├── raw/                       # Input: Parquet files from Feature 001 (gitignored)
└── warehouse/                 # Output: DuckDB database file (gitignored)
    └── transactions.duckdb

tests/
├── conftest.py                # Shared fixtures (extend with ingestion fixtures)
├── unit/
│   ├── test_validator.py      # Schema and value validation tests
│   ├── test_loader.py         # DuckDB operations tests
│   └── test_dedup.py          # Deduplication logic tests
├── integration/
│   └── test_pipeline.py       # End-to-end pipeline tests
└── quality/
    └── test_data_quality.py   # Data quality assertion tests
```

**Structure Decision**: Extends the existing single-project layout from Feature 001. New ingestion code lives in `src/ingestion/` as a distinct module, keeping separation from the generator code. The CLI entrypoint follows the same PEP 723 pattern as `src/generate_transactions.py`. Test structure mirrors the three-tier pattern (unit/integration/quality) established in Feature 001.

## Complexity Tracking

No violations. All design choices align with constitution principles.
