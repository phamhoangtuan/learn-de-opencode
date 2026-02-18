# Tasks: Synthetic Financial Transaction Data Generator

**Input**: Design documents from `/specs/001-synthetic-financial-data/`
**Prerequisites**: plan.md (required), spec.md (required), research.md, data-model.md, quickstart.md

**Tests**: Included — plan.md specifies Pytest with pytest-cov (>= 80% coverage) and defines a test directory structure.

**Organization**: Tasks grouped by user story to enable independent implementation and testing.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization, directory structure, and dependency configuration

- [ ] T001 Create project directory structure per plan.md: `src/models/`, `src/lib/`, `src/data/`, `tests/unit/`, `tests/integration/`, `tests/quality/`, `data/raw/`
- [ ] T002 Create `src/generate_transactions.py` entrypoint with PEP 723 inline script metadata block declaring `polars`, `numpy`, `faker` dependencies and `requires-python = ">=3.11"`
- [ ] T003 [P] Create `tests/conftest.py` with shared fixtures (default seed, small record count, tmp output directory)
- [ ] T004 [P] Create `pyproject.toml` with pytest and ruff configuration (test discovery, coverage settings, linting rules)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**CRITICAL**: No user story work can begin until this phase is complete

- [ ] T005 Create merchant/category reference data in `src/data/merchants.json` with 50-100 merchants across 18 spending categories with category weights per data-model.md
- [ ] T006 [P] Implement structured JSON logging configuration in `src/lib/logging_config.py` with generation metadata output support per FR-009
- [ ] T007 [P] Implement input parameter validation in `src/lib/validators.py` — validate record count (>= 0), date range (start < end), account count, format, seed; return clear error messages per FR-010
- [ ] T008 [P] Implement log-normal amount distribution in `src/lib/distributions.py` with mu=3.0, sigma=1.5 base parameters and currency-specific scaling factors (USD=1.0, EUR=0.9, GBP=0.8, JPY=110.0) per research.md decision #3
- [ ] T009 Implement Merchant model and weighted selection logic in `src/models/merchant.py` — load from `merchants.json`, select merchant by category weight then uniform within category per data-model.md
- [ ] T010 [P] Implement Account generation logic in `src/models/account.py` — generate account_id (ACC-XXXXX format), account_holder (via Faker), account_type (weighted: checking 50%, savings 20%, credit 30%) per data-model.md
- [ ] T011 Implement Transaction schema definition and row-building logic in `src/models/transaction.py` — combine account, merchant, amount, timestamp, currency, type, status fields per data-model.md; create `__init__.py` files for `src/models/` and `src/lib/`

**Checkpoint**: Foundation ready — user story implementation can now begin

---

## Phase 3: User Story 1 — Generate Realistic Financial Transactions (Priority: P1) MVP

**Goal**: A single `uv run` command generates configurable N synthetic financial transactions with all required fields, realistic distributions, and Parquet output.

**Independent Test**: Run `uv run src/generate_transactions.py --count 1000 --seed 42` and verify output file contains exactly 1000 records with all 9 fields, realistic amount distribution, and correct currency weights.

### Tests for User Story 1

> **Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T012 [P] [US1] Unit test for amount distribution in `tests/unit/test_distributions.py` — verify log-normal shape (skewness > 1.0, median < mean), currency scaling, positive values only
- [ ] T013 [P] [US1] Unit test for merchant selection in `tests/unit/test_merchant.py` — verify weighted category selection, merchant-category mapping consistency, all categories reachable
- [ ] T014 [P] [US1] Unit test for account generation in `tests/unit/test_account.py` — verify ACC-XXXXX format, unique IDs, account_type distribution weights
- [ ] T015 [P] [US1] Unit test for input validation in `tests/unit/test_validators.py` — verify rejection of negative count, invalid date range, invalid format; verify clear error messages
- [ ] T016 [P] [US1] Integration test for end-to-end generation in `tests/integration/test_generator.py` — verify 1000 records output, all fields present, no nulls, correct Parquet schema
- [ ] T017 [P] [US1] Data quality test in `tests/quality/test_data_quality.py` — verify currency weight distribution (~70/15/10/5% within tolerance), transaction_type weights, status weights, amount skewness per SC-004

### Implementation for User Story 1

- [ ] T018 [US1] Implement core generation orchestrator in `src/generate_transactions.py` — wire together account generation, merchant selection, amount distribution, timestamp generation (uniform within date range), currency/type/status weighted selection; build Polars DataFrame with all 9 fields per FR-001
- [ ] T019 [US1] Implement Parquet and CSV output writing in `src/generate_transactions.py` — write to `data/raw/` with timestamped filename (`transactions_YYYYMMDD_HHMMSS.{format}`), auto-create output directory if missing per FR-006
- [ ] T020 [US1] Implement CLI argument parsing in `src/generate_transactions.py` — `--count` (default 10000), `--start-date`/`--end-date` (default last 90 days), `--accounts` (default 100), `--format` (parquet/csv, default parquet), `--seed` (optional) per FR-002/FR-003/FR-004/FR-006/FR-007
- [ ] T021 [US1] Implement generation metadata output in `src/generate_transactions.py` — log JSON to stdout with record count, seed, date range, account count, format, output path, duration per FR-009
- [ ] T022 [US1] Implement edge case handling in `src/generate_transactions.py` — zero records produces empty file with valid schema, accounts capped at transaction count with warning per spec edge cases

**Checkpoint**: User Story 1 fully functional — `uv run src/generate_transactions.py` produces valid Parquet output with realistic data

---

## Phase 4: User Story 2 — Configure Generation Parameters (Priority: P2)

**Goal**: All CLI parameters (count, date range, accounts, format) are respected and validated, with clear error messages for invalid input.

**Independent Test**: Run with various parameter combinations (`--count 50000 --accounts 50 --format csv --start-date 2025-01-01 --end-date 2025-12-31`) and verify each parameter is correctly applied in output.

### Tests for User Story 2

- [ ] T023 [P] [US2] Integration test for parameter combinations in `tests/integration/test_generator.py` — verify date range filtering, account count accuracy, CSV format output, large count handling
- [ ] T024 [P] [US2] Unit test for edge case validation in `tests/unit/test_validators.py` — verify end-date-before-start-date error, accounts-exceeding-count warning, boundary values (count=1, same-day range)

### Implementation for User Story 2

- [ ] T025 [US2] Enhance date range validation in `src/lib/validators.py` and `src/generate_transactions.py` — verify all timestamps fall within range, handle same-day range, validate date format per FR-003
- [ ] T026 [US2] Implement account count enforcement in `src/generate_transactions.py` — verify exactly N distinct account_ids in output, cap at transaction count with logged warning per FR-004
- [ ] T027 [US2] Implement CSV output path in `src/generate_transactions.py` — ensure `--format csv` writes valid CSV with same schema, timestamped filename per FR-006

**Checkpoint**: All CLI parameters work correctly, validated with clear error messages

---

## Phase 5: User Story 3 — Reproducible Data Generation (Priority: P3)

**Goal**: Same seed + same parameters = byte-identical output files. Random seed logged when not provided.

**Independent Test**: Run twice with `--seed 42 --count 1000` and diff the output files — they must be identical.

### Tests for User Story 3

- [ ] T028 [P] [US3] Integration test for reproducibility in `tests/integration/test_generator.py` — verify two runs with same seed produce identical output, verify different seeds produce different output
- [ ] T029 [P] [US3] Unit test for seed propagation in `tests/unit/test_distributions.py` — verify seed controls NumPy RNG and Faker output deterministically

### Implementation for User Story 3

- [ ] T030 [US3] Implement unified seed management in `src/generate_transactions.py` — single seed initializes NumPy `default_rng()` and Faker/random seed; generate and log random seed when none provided per research.md decision #6
- [ ] T031 [US3] Ensure deterministic output ordering in `src/generate_transactions.py` — verify Polars DataFrame construction order is stable, no non-deterministic shuffling, UUID generation uses seeded RNG per SC-003

**Checkpoint**: Reproducibility verified — identical seeds produce byte-identical files

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] T032 [P] Implement streaming/chunked writes for large datasets (>1M records) in `src/generate_transactions.py` to prevent memory exhaustion per spec edge case for 10M records
- [ ] T033 [P] Add `--help` output with usage examples, parameter descriptions, and defaults in `src/generate_transactions.py` per FR-008
- [ ] T034 Run full test suite with coverage: `uv run --with pytest --with pytest-cov pytest tests/ -v --cov=src --cov-report=term-missing` — verify >= 80% coverage per plan.md
- [ ] T035 Run ruff linting: `ruff check src/ tests/` — fix any style violations
- [ ] T036 Validate quickstart.md examples end-to-end — run each command from `specs/001-synthetic-financial-data/quickstart.md` and verify expected output

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational — MVP delivery
- **User Story 2 (Phase 4)**: Depends on US1 (builds on CLI parsing and core generation)
- **User Story 3 (Phase 5)**: Depends on US1 (adds seed management to existing generation)
- **Polish (Phase 6)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) — no story dependencies
- **User Story 2 (P2)**: Extends US1 CLI and validation — depends on US1 core being functional
- **User Story 3 (P3)**: Adds seed management to US1 — depends on US1 core being functional
- **US2 and US3**: Independent of each other — can proceed in parallel after US1

### Within Each User Story

- Tests written FIRST and verified to FAIL before implementation
- Models/lib before orchestrator
- Core generation before edge cases
- Story complete before moving to next priority

### Parallel Opportunities

- T003, T004 can run in parallel (Setup phase)
- T005, T006, T007, T008, T010 can run in parallel (Foundational phase — different files)
- T012-T017 can all run in parallel (US1 tests — different test files)
- T023, T024 can run in parallel (US2 tests)
- T028, T029 can run in parallel (US3 tests)
- T032, T033 can run in parallel (Polish — different concerns)

---

## Parallel Example: User Story 1

```bash
# Launch all US1 tests together (write first, verify they fail):
Task: "Unit test for amount distribution in tests/unit/test_distributions.py"
Task: "Unit test for merchant selection in tests/unit/test_merchant.py"
Task: "Unit test for account generation in tests/unit/test_account.py"
Task: "Unit test for input validation in tests/unit/test_validators.py"
Task: "Integration test for end-to-end generation in tests/integration/test_generator.py"
Task: "Data quality test in tests/quality/test_data_quality.py"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL — blocks all stories)
3. Complete Phase 3: User Story 1
4. **STOP and VALIDATE**: Run `uv run src/generate_transactions.py --count 1000 --seed 42` and verify output
5. Run test suite — verify >= 80% coverage on US1 code

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → MVP!
3. Add User Story 2 → Test parameter combinations
4. Add User Story 3 → Test reproducibility (byte-identical outputs)
5. Polish → Coverage, linting, quickstart validation

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Verify tests fail before implementing
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Total tasks: 36 (T001-T036)
