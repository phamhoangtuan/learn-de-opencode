# Data Quality Checklist: Synthetic Financial Transaction Data Generator

**Purpose**: Validate the completeness, clarity, consistency, and coverage of requirements for the synthetic financial data generator across all spec dimensions — data quality, output/interoperability, reproducibility, CLI/UX, error handling, and recovery flows.
**Created**: 2026-02-18
**Feature**: [spec.md](../spec.md)
**Depth**: Standard | **Scope**: Full breadth | **Audience**: Reviewer (pre-implementation gate)

## Requirement Completeness

- [ ] CHK001 - Are all nine transaction fields (transaction_id, timestamp, amount, currency, merchant_name, category, account_id, transaction_type, status) documented with types, constraints, and nullability? [Completeness, Spec §FR-001, Data Model §Transaction]
- [ ] CHK002 - Are distribution parameters for amount (mu, sigma), currency weights, transaction_type weights, and status weights explicitly specified with numeric values? [Completeness, Spec §FR-005, Data Model §Distribution rules]
- [ ] CHK003 - Are all 18 spending categories listed with their approximate percentage weights? [Completeness, Spec §FR-011, Data Model §Merchant]
- [ ] CHK004 - Is the total merchant count (50-100) specified with per-category allocation rules? [Completeness, Spec §FR-012]
- [ ] CHK005 - Are Account entity fields (account_id, account_holder, account_type) and their distributions fully specified? [Completeness, Data Model §Account]
- [ ] CHK006 - Are default values documented for all configurable parameters (record count, date range, account count, output format, seed)? [Completeness, Spec §FR-002, FR-003, FR-004, FR-007]

## Requirement Clarity

- [ ] CHK007 - Is "realistic distributions" in FR-005 quantified beyond "log-normal or similar skewed" — are mu and sigma values prescribed or guidance-only? [Clarity, Spec §FR-005]
- [ ] CHK008 - Is "structured format" for generation metadata output (FR-009) defined — JSON, key=value, or another format? [Clarity, Spec §FR-009]
- [ ] CHK009 - Is the "clear validation error" requirement in edge cases quantified — should errors include exit codes, error message format, or specific exception types? [Clarity, Spec §Edge Cases]
- [ ] CHK010 - Is "currency-scaled" for amounts in the data model defined — what scaling factors apply to EUR, GBP, JPY relative to USD? [Clarity, Data Model §Distribution rules]
- [ ] CHK011 - Is the timestamp distribution within the date range specified — uniform, business-hours weighted, or another pattern? [Clarity, Spec §FR-003]
- [ ] CHK012 - Are the PEP 723 inline dependency version constraints specified (e.g., polars>=1.0, numpy>=1.26) or left unbound? [Clarity, Spec §FR-008]

## Requirement Consistency

- [ ] CHK013 - Do the 18 category weights in the data model sum to 100%? [Consistency, Data Model §Merchant]
- [ ] CHK014 - Are currency codes consistently referenced as ISO 4217 3-character codes across spec, data model, and quickstart? [Consistency, Spec §FR-001, Data Model §Transaction]
- [ ] CHK015 - Is the account_id format (ACC-XXXXX) consistently defined in both the Account and Transaction entity definitions? [Consistency, Data Model §Account, §Transaction]
- [ ] CHK016 - Do the category names in the merchant reference data match the category field constraints on the Transaction entity? [Consistency, Data Model §Merchant, §Transaction]

## Acceptance Criteria Quality

- [ ] CHK017 - Is SC-001 ("10,000 records in under 10 seconds") specific about the measurement environment — cold start with `uv run`, or after dependency caching? [Measurability, Spec §SC-001]
- [ ] CHK018 - Is SC-004's skewness threshold (> 1.0) and median-mean relationship sufficient to validate "realistic distribution" — are additional statistical tests required? [Measurability, Spec §SC-004]
- [ ] CHK019 - Is SC-003 ("byte-identical output") achievable given Parquet metadata may include timestamps or UUIDs — is file-level or data-level identity intended? [Measurability, Spec §SC-003]
- [ ] CHK020 - Can SC-002 ("zero field-level errors") be objectively measured — is a schema validation tool or method specified? [Measurability, Spec §SC-002]

## Scenario Coverage

- [ ] CHK021 - Are requirements defined for how transactions are distributed across accounts — uniform, proportional, or realistic skew (some accounts more active)? [Coverage, Gap]
- [ ] CHK022 - Are requirements defined for the relationship between account_type and transaction patterns (e.g., credit accounts only have debit transactions)? [Coverage, Gap]
- [ ] CHK023 - Are requirements specified for JPY amount rounding (JPY has no decimal subdivision)? [Coverage, Spec §FR-001, Edge Case]
- [ ] CHK024 - Is the behavior specified when the output directory (`data/raw/`) does not exist — auto-create or error? [Coverage, Spec §FR-006, Gap]

## Edge Case & Boundary Coverage

- [ ] CHK025 - Are boundary conditions specified for date range parameters — same start and end date, single-day range, very long ranges (years)? [Edge Case, Spec §FR-003]
- [ ] CHK026 - Is behavior defined for a record count of 1 — should it produce a valid single-record file? [Edge Case, Spec §FR-002]
- [ ] CHK027 - Are requirements defined for the maximum reasonable merchant_name and category string lengths? [Edge Case, Data Model §Transaction]
- [ ] CHK028 - Is the streaming/chunked write threshold for large requests (10M records) specified — at what record count does streaming activate? [Edge Case, Spec §Edge Cases]

## Recovery & Failure Flows

- [ ] CHK029 - Are requirements defined for handling disk-full or I/O errors during file writes — partial file cleanup, retry, or error reporting? [Recovery, Gap]
- [ ] CHK030 - Are requirements defined for output directory permission errors — clear error message, suggested remediation? [Recovery, Gap]
- [ ] CHK031 - Are requirements defined for interrupted generation (e.g., Ctrl+C) — should partial output be cleaned up or preserved? [Recovery, Gap]
- [ ] CHK032 - Is the behavior specified when Parquet write fails but CSV was also requested — should the system attempt the alternative format? [Recovery, Gap]

## Non-Functional Requirements

- [ ] CHK033 - Are memory consumption requirements specified for large generation runs — maximum RSS, or "must not exceed Nx input size"? [Non-Functional, Gap]
- [ ] CHK034 - Is logging behavior defined beyond FR-009 completion metadata — should progress be reported during generation (e.g., every N records)? [Non-Functional, Gap]
- [ ] CHK035 - Are requirements specified for the CLI help output (`--help`) — should it include usage examples, parameter descriptions, defaults? [Non-Functional, Spec §FR-008, Gap]

## Notes

- Check items off as completed: `[x]`
- Add comments or findings inline
- Items are numbered sequentially for easy reference
- This checklist validates the *requirements quality*, not the implementation
- Total items: 35 | Traceability: 33/35 items (94%) include spec/data-model references or [Gap] markers
