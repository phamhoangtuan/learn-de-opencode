---

description: "Task list template for feature implementation"
---

# Tasks: [FEATURE NAME]

**Input**: Design documents from `/specs/[###-feature-name]/`
**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Per Constitution v2.0.0 Principle III (Test-First Development), tests are MANDATORY and must be written BEFORE implementation. Tests must FAIL before writing implementation code. 80% minimum test coverage required.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Single project**: `src/`, `tests/` at repository root
- **Web app**: `backend/src/`, `frontend/src/`
- **Mobile**: `api/src/`, `ios/src/` or `android/src/`
- Paths shown below assume single project - adjust based on plan.md structure

<!-- 
  ============================================================================
  IMPORTANT: The tasks below are SAMPLE TASKS for illustration purposes only.
  
  The /speckit.tasks command MUST replace these with actual tasks based on:
  - User stories from spec.md (with their priorities P1, P2, P3...)
  - Feature requirements from plan.md
  - Entities from data-model.md
  - Endpoints from contracts/
  
  Tasks MUST be organized by user story so each story can be:
  - Implemented independently
  - Tested independently
  - Delivered as an MVP increment
  
  DO NOT keep these sample tasks in the generated tasks.md file.
  ============================================================================
-->

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [ ] T001 Create project structure per implementation plan
- [ ] T002 Initialize [language] project with [framework] dependencies
- [ ] T003 [P] Configure linting and formatting tools (PEP 8 for Python per Constitution v2.0.0)
- [ ] T004 [P] Configure testing framework (Pytest with 80% coverage requirement)
- [ ] T005 [P] Setup CI/CD pipeline (GitHub Actions per Constitution v2.0.0)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

Examples of foundational tasks (adjust based on your project):

- [ ] T006 Setup database schema and migrations framework
- [ ] T007 [P] Implement authentication/authorization framework
- [ ] T008 [P] Setup API routing and middleware structure
- [ ] T009 Create base models/entities that all stories depend on
- [ ] T010 Configure error handling and logging infrastructure
- [ ] T011 Setup environment configuration management
- [ ] T012 [P] Initialize metadata capture framework (Constitution v2.0.0 Principle IV)
- [ ] T013 [P] Setup data quality validation framework (Constitution v2.0.0 Principle II)

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - [Title] (Priority: P1) 🎯 MVP

**Goal**: [Brief description of what this story delivers]

**Independent Test**: [How to verify this story works on its own]

### Tests for User Story 1 (MANDATORY per Constitution v2.0.0 III) ⚠️

> **CRITICAL: Write these tests FIRST, ensure they FAIL before implementation**
> Per Constitution v2.0.0 Principle III: Test-First Development is NON-NEGOTIABLE
> Target: 80% minimum test coverage

- [ ] T014 [P] [US1] Contract test for [endpoint] in tests/contract/test_[name].py
- [ ] T015 [P] [US1] Integration test for [user journey] in tests/integration/test_[name].py
- [ ] T016 [P] [US1] Data quality test for [data validation] in tests/quality/test_[name].py

### Implementation for User Story 1

- [ ] T017 [P] [US1] Create [Entity1] model in src/models/[entity1].py
- [ ] T018 [P] [US1] Create [Entity2] model in src/models/[entity2].py
- [ ] T019 [US1] Implement [Service] in src/services/[service].py (depends on T017, T018)
- [ ] T020 [US1] Implement [endpoint/feature] in src/[location]/[file].py
- [ ] T021 [US1] Add data validation per Constitution v2.0.0 Principle II (schemas + quality checks)
- [ ] T022 [US1] Add metadata capture per Constitution v2.0.0 Principle IV (lineage tracking)
- [ ] T023 [US1] Add security controls per Constitution v2.0.0 Principle V (PII handling, encryption)
- [ ] T024 [US1] Add observability per Constitution v2.0.0 Principle IV (logging, metrics)

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - [Title] (Priority: P2)

**Goal**: [Brief description of what this story delivers]

**Independent Test**: [How to verify this story works on its own]

### Tests for User Story 2 (MANDATORY per Constitution v2.0.0 III) ⚠️

- [ ] T025 [P] [US2] Contract test for [endpoint] in tests/contract/test_[name].py
- [ ] T026 [P] [US2] Integration test for [user journey] in tests/integration/test_[name].py
- [ ] T027 [P] [US2] Data quality test for [data validation] in tests/quality/test_[name].py

### Implementation for User Story 2

- [ ] T028 [P] [US2] Create [Entity] model in src/models/[entity].py
- [ ] T029 [US2] Implement [Service] in src/services/[service].py
- [ ] T030 [US2] Implement [endpoint/feature] in src/[location]/[file].py
- [ ] T031 [US2] Integrate with User Story 1 components (if needed)
- [ ] T032 [US2] Add DMBOK compliance checks (data quality, metadata, security)

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently

---

## Phase 5: User Story 3 - [Title] (Priority: P3)

**Goal**: [Brief description of what this story delivers]

**Independent Test**: [How to verify this story works on its own]

### Tests for User Story 3 (MANDATORY per Constitution v2.0.0 III) ⚠️

- [ ] T033 [P] [US3] Contract test for [endpoint] in tests/contract/test_[name].py
- [ ] T034 [P] [US3] Integration test for [user journey] in tests/integration/test_[name].py
- [ ] T035 [P] [US3] Data quality test for [data validation] in tests/quality/test_[name].py

### Implementation for User Story 3

- [ ] T036 [P] [US3] Create [Entity] model in src/models/[entity].py
- [ ] T037 [US3] Implement [Service] in src/services/[service].py
- [ ] T038 [US3] Implement [endpoint/feature] in src/[location]/[file].py
- [ ] T039 [US3] Add DMBOK compliance checks (data quality, metadata, security)

**Checkpoint**: All user stories should now be independently functional

---

[Add more user story phases as needed, following the same pattern]

---

## Phase N: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] TXXX [P] Documentation updates in docs/ (Google docstring format per Constitution v2.0.0)
- [ ] TXXX Code cleanup and refactoring (maintain 80% test coverage)
- [ ] TXXX Performance optimization across all stories
- [ ] TXXX [P] Additional unit tests (if requested) in tests/unit/
- [ ] TXXX Security hardening (review Constitution v2.0.0 Principle V)
- [ ] TXXX DMBOK compliance verification (run dmbok-checklist.md)
- [ ] TXXX Metadata completeness check (Constitution v2.0.0 Principle IV)
- [ ] TXXX Data quality validation (Constitution v2.0.0 Principle II)
- [ ] TXXX Run quickstart.md validation

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3+)**: All depend on Foundational phase completion
  - User stories can then proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P2 → P3)
- **Polish (Final Phase)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (P2)**: Can start after Foundational (Phase 2) - May integrate with US1 but should be independently testable
- **User Story 3 (P3)**: Can start after Foundational (Phase 2) - May integrate with US1/US2 but should be independently testable

### Within Each User Story

- Tests MUST be written and FAIL before implementation (Constitution v2.0.0 Principle III)
- Data quality tests required for data engineering features (Constitution v2.0.0 Principle II)
- Models before services
- Services before endpoints
- Core implementation before integration
- DMBOK compliance checks (metadata, security, quality)
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel
- All Foundational tasks marked [P] can run in parallel (within Phase 2)
- Once Foundational phase completes, all user stories can start in parallel (if team capacity allows)
- All tests for a user story marked [P] can run in parallel
- Models within a story marked [P] can run in parallel
- Different user stories can be worked on in parallel by different team members

---

## Parallel Example: User Story 1

```bash
# Launch all tests for User Story 1 together (MANDATORY per Constitution v2.0.0 III):
Task: "Contract test for [endpoint] in tests/contract/test_[name].py"
Task: "Integration test for [user journey] in tests/integration/test_[name].py"
Task: "Data quality test for [data validation] in tests/quality/test_[name].py"

# Launch all models for User Story 1 together:
Task: "Create [Entity1] model in src/models/[entity1].py"
Task: "Create [Entity2] model in src/models/[entity2].py"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 1
4. **STOP and VALIDATE**: Test User Story 1 independently
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → Deploy/Demo (MVP!)
3. Add User Story 2 → Test independently → Deploy/Demo
4. Add User Story 3 → Test independently → Deploy/Demo
5. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1
   - Developer B: User Story 2
   - Developer C: User Story 3
3. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Verify tests fail before implementing (Constitution v2.0.0 Principle III)
- Target 80% minimum test coverage (Code Quality Standards)
- Include DMBOK compliance tasks: metadata capture, data quality validation, security controls
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence
