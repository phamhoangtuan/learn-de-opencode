# Data Engineering Checklist: SCD Type 2 — Accounts Dimension

**Purpose**: Validate the quality, completeness, and consistency of requirements for the SCD2 dimension build — testing the spec/plan as written, not the implementation.
**Created**: 2026-02-26
**Feature**: [spec.md](../spec.md) | [plan.md](../plan.md) | [data-model.md](../data-model.md)
**Depth**: Standard | **Audience**: PR Reviewer | **Focus**: Data Modeling Integrity + Pipeline Integration

---

## Requirement Completeness

- [ ] CHK001 — Are requirements specified for all 6 tracked attributes (`primary_currency`, `primary_category`, `transaction_count`, `total_spend`, `first_seen`, `last_seen`) with their derivation logic? [Completeness, Spec §FR-002]
- [ ] CHK002 — Is the computation source for attributes (full history vs. current-run-only) explicitly defined in requirements? [Completeness, Spec §Assumptions]
- [ ] CHK003 — Are DDL requirements for the surrogate key sequence (`dim_accounts_sk_seq`) documented alongside `dim_accounts`? [Completeness, Data-Model]
- [ ] CHK004 — Are requirements defined for the `mart__account_history` transform, including all computed columns (`version_number`, `version_duration_days`)? [Completeness, Spec §FR-007]
- [ ] CHK005 — Are pipeline DAG integration requirements specified (where in the sequence the dim build step runs and what it depends on)? [Completeness, Spec §FR-009]
- [ ] CHK006 — Is the metadata emitted by the dim build step (accounts processed, new versions, unchanged count) documented as a requirement? [Completeness, Spec §FR-010]
- [ ] CHK007 — Are requirements defined for `create_dim_tables()` idempotency (safe to call on an already-initialised warehouse)? [Completeness, Contracts §dim_builder.md]

---

## Requirement Clarity

- [ ] CHK008 — Is "primary currency" (and "primary category") defined precisely enough to be unambiguously implemented — including the tie-breaking rule and null handling? [Clarity, Spec §FR-002, Research §R-003]
- [ ] CHK009 — Is the sentinel value for `valid_to` on current rows (`9999-12-31T23:59:59+00:00`) specified as a constant, not left to implementor discretion? [Clarity, Spec §Assumptions]
- [ ] CHK010 — Is "closed" (expired) row defined unambiguously — i.e., the exact value of `valid_to` and `is_current` for an expired row? [Clarity, Spec §FR-004, Data-Model]
- [ ] CHK011 — Is "version_duration_days" computation for the current row specified clearly (days from `valid_from` to today, not to sentinel)? [Clarity, Spec §US3-AC2]
- [ ] CHK012 — Is the MD5 hash construction formula (field order, delimiter, null coalescing) specified as a deterministic contract rather than an implementation suggestion? [Clarity, Research §R-002]
- [ ] CHK013 — Is the term "accounts processed" in `DimBuildResult` precisely defined — does it mean distinct `account_id`s seen in transactions, or rows written to `dim_accounts`? [Clarity, Contracts §dim_builder.md, Gap]

---

## Requirement Consistency

- [ ] CHK014 — Are `valid_from`/`valid_to` types consistent across the spec, data-model DDL, and contract docs (all TIMESTAMPTZ, not DATE)? [Consistency, Data-Model, Spec §FR-001]
- [ ] CHK015 — Is the `run_id` lineage column on `dim_accounts` consistent with how `run_id` is used on `transactions` and `quarantine` tables? [Consistency, Spec §FR-001, loader.py]
- [ ] CHK016 — Does the spec's FR-004 (close old row, insert new row) align with the two-statement MERGE pattern documented in research R-002 without contradictions? [Consistency, Spec §FR-004, Research §R-002]
- [ ] CHK017 — Are the 3 quality check names in `contracts/quality_checks.md` consistent with the naming convention used by existing checks in `src/checks/`? [Consistency, Contracts §quality_checks.md]
- [ ] CHK018 — Is the `is_current` flag semantics (TRUE = current, FALSE = expired) consistent across spec, data-model, and contracts — no document uses the inverse? [Consistency]

---

## Acceptance Criteria Quality

- [ ] CHK019 — Is SC-001 ("exactly N `is_current=TRUE` rows after a run with N accounts") objectively verifiable without ambiguity about what "distinct accounts" means? [Measurability, Spec §SC-001]
- [ ] CHK020 — Is SC-005 ("dim build adds < 5 seconds") measurable given that elapsed time depends on warehouse size — is a reference dataset size specified? [Measurability, Spec §SC-005]
- [ ] CHK021 — Do all P1 user story acceptance scenarios have at least one negative test case (e.g., unchanged account should NOT produce a new row)? [Acceptance Criteria, Spec §US2-AC2]
- [ ] CHK022 — Is SC-003 ("all integrity checks pass on every run") specific enough — does it define what "pass" means (zero violation rows returned)? [Clarity, Spec §SC-003]

---

## Scenario Coverage

- [ ] CHK023 — Are requirements defined for the first-ever pipeline run when `dim_accounts` does not yet exist (cold start)? [Coverage, Spec §Edge Cases]
- [ ] CHK024 — Are requirements defined for the scenario where all accounts change attributes simultaneously in a single run? [Coverage, Spec §Edge Cases]
- [ ] CHK025 — Are requirements defined for the scenario where the transactions table is empty (no records at all)? [Coverage, Gap]
- [ ] CHK026 — Are requirements defined for the scenario where `stg_transactions` view is missing or stale at dim-build time? [Coverage, Gap — pre-condition failure]
- [ ] CHK027 — Are requirements for the `mart__account_history` view defined for the single-version case (an account that has never changed)? [Coverage, Spec §US3]

---

## Edge Case Coverage

- [ ] CHK028 — Is the tie-breaking behavior for mode computation (equal-frequency values) specified as a requirement, not only as a research decision? [Edge Case, Spec §Assumptions, Research §R-003]
- [ ] CHK029 — Is the behavior when `valid_from` equals `valid_to` (zero-duration version) defined — can this occur, and if so, what does it mean? [Edge Case, Gap]
- [ ] CHK030 — Is null handling for derived attributes (`primary_currency`, `primary_category`) specified — what if an account has transactions with all-null currencies? [Edge Case, Gap]
- [ ] CHK031 — Is the behavior defined when `build_dim_accounts()` is called while a transaction is already open on the connection? [Edge Case, Contracts §dim_builder.md, FR-011]

---

## Non-Functional Requirements

- [ ] CHK032 — Is idempotency of the full dim build explicitly stated as a requirement — re-running the step twice should produce identical dim_accounts state? [Non-Functional, Gap]
- [ ] CHK033 — Is the atomicity requirement (FR-011) specific about which operations are in scope — does it cover only dim_accounts mutations, or also metadata writes? [Non-Functional, Spec §FR-011, Clarification §Q1]
- [ ] CHK034 — Are observability requirements defined for the dim build step — what should be logged on success, on partial change, and on failure? [Non-Functional, Gap]
- [ ] CHK035 — Is the performance requirement (SC-005, <5s for 100K transactions) specified as a p50/p95/p99 target or worst-case? [Non-Functional, Clarity, Spec §SC-005]

---

## Dependencies & Assumptions

- [ ] CHK036 — Is the dependency on `stg_transactions` (not raw `transactions`) formally stated as a pre-condition in the contracts? [Dependencies, Contracts §dim_builder.md, Spec §Dependencies]
- [ ] CHK037 — Is the assumption that the dim build runs after transforms (which create `stg_transactions`) encoded as a hard pipeline dependency, not just a documentation note? [Dependencies, Spec §FR-009, Plan §Pipeline Integration]
- [ ] CHK038 — Is the assumption that DuckDB does not support `with conn.transaction():` documented as a known constraint, given it drives the `conn.begin()` pattern choice? [Assumptions, Research §R-004]
- [ ] CHK039 — Are the 4 feature dependencies (002, 003, 004, 006) specified with the minimum version or capability required, not just the feature number? [Dependencies, Spec §Dependencies, Gap]

---

## Notes

- Check items off as completed: `[x]`
- `[Gap]` = requirement is missing and should be added to spec before implementation
- `[Ambiguity]` = requirement exists but is underspecified
- Items marked with `[Gap]` in CHK025, CHK026, CHK029, CHK030 represent the highest-risk missing requirements — address these before proceeding to `/speckit.tasks`
