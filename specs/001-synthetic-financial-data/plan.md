# Implementation Plan: Synthetic Financial Transaction Data Generator

**Branch**: `001-synthetic-financial-data` | **Date**: 2026-02-18 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-synthetic-financial-data/spec.md`

## Summary

Build a standalone Python script that generates realistic synthetic financial
transaction data, runnable via `uv run` with zero setup. The generator produces
configurable volumes of multi-currency transactions with realistic distributions,
merchant/category mappings, and deterministic seed support. Output targets
Parquet (primary) and CSV (secondary) formats in the `data/raw/` directory.

## Technical Context

**Language/Version**: Python 3.11+
**Primary Dependencies**: Polars (data generation/output), Faker (names/merchants), NumPy (distributions)
**Storage**: Local filesystem — Parquet files in `data/raw/`
**Testing**: Pytest with pytest-cov (>= 80% coverage)
**Target Platform**: macOS / Linux (local development)
**Project Type**: Single project
**Performance Goals**: 10,000 records in < 10 seconds
**Constraints**: Must run via `uv run` with inline script dependencies (PEP 723)
**Scale/Scope**: Up to 10M records with streaming writes

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Data Quality First | PASS | Schema validation on output, 6Cs dimensions tested via SC-002/SC-004 |
| II. Metadata & Lineage | PASS | FR-009 logs generation metadata (seed, count, duration, path) to stdout |
| III. Security & Privacy by Design | PASS | Synthetic data only, no real PII. No credentials needed. |
| IV. Integration & Interoperability | PASS | Parquet primary format, CSV secondary. Standard columnar schema. |
| V. Architecture & Modeling Integrity | PASS | Clear entity model (Transaction, Account, Merchant). Naming conventions defined. |

All gates pass. No violations to justify.

## Project Structure

### Documentation (this feature)

```text
specs/001-synthetic-financial-data/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code (repository root)

```text
src/
├── generate_transactions.py   # Main generator script (uv run entrypoint)
├── models/
│   ├── __init__.py
│   ├── transaction.py         # Transaction schema & validation
│   ├── account.py             # Account generation logic
│   └── merchant.py            # Merchant/category reference data
├── lib/
│   ├── __init__.py
│   ├── distributions.py       # Amount distribution logic (log-normal)
│   ├── validators.py          # Input parameter validation
│   └── logging_config.py      # Structured JSON logging
└── data/
    └── merchants.json         # Pre-defined merchant/category reference data

data/
└── raw/                       # Generated output files (gitignored)

tests/
├── conftest.py                # Shared fixtures
├── unit/
│   ├── test_distributions.py  # Amount distribution tests
│   ├── test_validators.py     # Input validation tests
│   ├── test_account.py        # Account generation tests
│   └── test_merchant.py       # Merchant selection tests
├── integration/
│   └── test_generator.py      # End-to-end generation tests
└── quality/
    └── test_data_quality.py   # 6Cs validation on generated data
```

**Structure Decision**: Single project layout. The generator is a standalone
CLI tool with modular components under `src/`. Test suite mirrors the source
structure with unit, integration, and quality test directories.

## Complexity Tracking

> No violations. All constitution gates pass.
