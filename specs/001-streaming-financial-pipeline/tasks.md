# Tasks: Streaming Financial Transaction Pipeline

**Input**: Design documents from `/specs/001-streaming-financial-pipeline/`
**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Per Constitution v2.0.0 Principle III (Test-First Development), tests are MANDATORY and must be written BEFORE implementation. Tests must FAIL before writing implementation code. 80% minimum test coverage required.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Generator (Python)**: `generator/src/` with subdirectories `models/`, `generators/`, `kafka/`, `lib/`
- **Flink Jobs (Java)**: `flink-jobs/src/main/java/com/learnde/pipeline/` with subdirectories `models/`, `functions/`, `rules/`, `serialization/`
- **Flink Tests (Java)**: `flink-jobs/src/test/java/com/learnde/pipeline/`
- **Python Tests**: `tests/` with subdirectories `unit/`, `integration/`, `contract/`
- **Config**: `config/`
- **Scripts**: `scripts/`
- **Infrastructure**: `docker-compose.yml` at repo root

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization, directory structure, build tooling, and base Docker Compose skeleton

- [x] T001 Create full project directory structure per plan.md (generator/, flink-jobs/, config/, scripts/, tests/ with all subdirectories and __init__.py files)
- [x] T002 [P] Initialize Python project: create generator/requirements.txt with confluent-kafka, faker, pyyaml, and generator/Dockerfile based on python:3.11-slim
- [x] T003 [P] Initialize Java Maven project: create flink-jobs/pom.xml with Flink 1.20, Iceberg connector (iceberg-flink-runtime-1.20-1.7.0), Jackson JSON dependencies
- [x] T004 [P] Create config/generator.yaml with default settings (rate: 10, fraud_percentage: 3, account_pool_size: 1000, currency weights, merchant category weights)
- [x] T005 [P] Create config/alerting-rules.yaml with 3 default rules (high-value-transaction threshold $10K, rapid-activity 5 tx/1 min, unusual-hour 01:00-05:00)
- [x] T006 [P] Configure Python testing framework: create tests/conftest.py with shared fixtures, add pytest and pytest-cov to dev dependencies
- [x] T007 [P] Configure Python linting: add ruff configuration (PEP 8 compliance per Constitution v2.0.0) in pyproject.toml or ruff.toml
- [x] T008 Create docker-compose.yml skeleton with all 5 services (kafka, iceberg-rest, flink-jobmanager, flink-taskmanager, generator), networks, volumes, memory limits per research.md R5, and dependency ordering

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented. Kafka topics, Iceberg catalog/tables, and shared data models.

**CRITICAL**: No user story work can begin until this phase is complete

- [x] T009 Configure Kafka service in docker-compose.yml: apache/kafka:3.9.0 KRaft mode, KAFKA_HEAP_OPTS -Xmx256m, mem_limit 512m, ports 9092/29092, healthcheck
- [x] T010 Configure Iceberg REST Catalog service in docker-compose.yml: tabulario/iceberg-rest, mem_limit 256m, port 8181, warehouse volume mount, healthcheck
- [x] T011 Configure Flink services in docker-compose.yml: flink:1.20-java17 for JobManager (768m) and TaskManager (1536m), checkpoint volume, depends_on kafka and iceberg-rest
- [x] T012 [P] Create Kafka topic initialization script or docker-compose entrypoint to create topics: raw-transactions (1 partition, RF 1), alerts (1 partition, RF 1), dead-letter (1 partition, RF 1)
- [x] T013 [P] Create Iceberg table initialization script scripts/init_iceberg.py using PyIceberg REST catalog client: create namespace `financial` and transactions table with schema from data-model.md (10 base + 3 enrichment fields), partitioned by days(timestamp), sort by transaction_id. Script must be idempotent (skip if table exists). Add pyiceberg to a scripts/requirements.txt.
- [x] T014 [P] Add alerts table creation to scripts/init_iceberg.py using PyIceberg REST catalog client: alerts table with schema from data-model.md (6 fields), partitioned by days(alert_timestamp). Idempotent (skip if table exists).
- [x] T015 [P] Add pipeline_session_metadata table creation to scripts/init_iceberg.py using PyIceberg REST catalog client: pipeline_session_metadata table with schema from data-model.md (13 fields), no partitioning. Idempotent (skip if table exists). Add an init-iceberg service to docker-compose.yml that runs this script after iceberg-rest is healthy, then exits.
- [x] T016 Configure structured JSON logging framework for Python generator in generator/src/lib/logging_config.py (log levels, correlation IDs, JSON format per Constitution v2.0.0 IV)
- [x] T017 [P] Create shared constants/enums file for Python: generator/src/models/enums.py with transaction_type, merchant_category, currency, status, severity enums matching contracts/

**Checkpoint**: Foundation ready — Kafka broker running, Iceberg catalog with tables created, Flink cluster ready, topic creation automated. User story implementation can now begin.

---

## Phase 3: User Story 1 — Generate and Stream Financial Transactions (Priority: P1) MVP

**Goal**: Build a Python data generator that produces realistic synthetic financial transactions and publishes them to a Kafka topic at a configurable rate with circuit breaker retry logic.

**Independent Test**: Start the generator, verify transactions appear in the Kafka raw-transactions topic with correct schema, confirm the generation rate matches configuration. Delivers value as a standalone synthetic data generator.

### Tests for User Story 1 (MANDATORY per Constitution v2.0.0 III)

> **CRITICAL: Write these tests FIRST, ensure they FAIL before implementation**
> Per Constitution v2.0.0 Principle III: Test-First Development is NON-NEGOTIABLE
> Target: 80% minimum test coverage

- [x] T018 [P] [US1] Contract test: validate generated transactions against raw-transactions.schema.json in tests/contract/test_raw_transaction_schema.py
- [x] T019 [P] [US1] Unit test: Account model pool generation and sampling in tests/unit/test_account_model.py
- [x] T020 [P] [US1] Unit test: Transaction model dataclass creation and JSON serialization in tests/unit/test_transaction_model.py
- [x] T021 [P] [US1] Unit test: transaction_generator realistic distributions (amounts, types, categories) in tests/unit/test_transaction_generator.py
- [x] T022 [P] [US1] Unit test: fraud_injector anomalous pattern injection at configurable percentage in tests/unit/test_fraud_injector.py
- [x] T023 [P] [US1] Unit test: rate_limiter token bucket algorithm (rate enforcement, burst handling, rate changes) in tests/unit/test_rate_limiter.py
- [x] T024 [P] [US1] Unit test: config_loader YAML loading, watch/reload, validation in tests/unit/test_config_loader.py
- [x] T025 [P] [US1] Unit test: Kafka producer wrapper with circuit breaker, delivery callbacks, buffer drain in tests/unit/test_producer.py
- [x] T026 [US1] Integration test: generator produces valid messages to Kafka topic end-to-end in tests/integration/test_generator_to_kafka.py

### Implementation for User Story 1

- [x] T027 [P] [US1] Create Account model with pool management (~1000 accounts) in generator/src/models/account.py
- [x] T028 [P] [US1] Create Transaction dataclass with JSON serialization and validation in generator/src/models/transaction.py
- [x] T029 [US1] Implement transaction_generator with realistic distributions (log-normal amounts, weighted types/categories, account sampling) in generator/src/generators/transaction_generator.py
- [x] T030 [US1] Implement fraud_injector with configurable anomalous pattern injection (high amounts, rapid activity, unusual hours) in generator/src/generators/fraud_injector.py
- [x] T031 [P] [US1] Implement rate_limiter token bucket algorithm with time.monotonic() in generator/src/lib/rate_limiter.py
- [x] T032 [P] [US1] Implement config_loader with YAML loading, 5-second file watch, and hot reload in generator/src/lib/config_loader.py
- [x] T033 [US1] Implement Kafka producer wrapper with confluent-kafka, delivery callbacks, circuit breaker (10 consecutive failures), deque buffer (max 10K), exponential backoff in generator/src/kafka/producer.py
- [x] T034 [US1] Implement main.py entry point: initialize account pool, start config watch, token bucket loop (generate → serialize → produce → poll) in generator/src/main.py
- [x] T035 [US1] Add structured JSON logging throughout generator modules per Constitution v2.0.0 Principle IV (startup, rate changes, delivery stats, errors)
- [x] T036 [US1] Configure generator service in docker-compose.yml: custom Dockerfile build, depends_on kafka, volume mount for config/generator.yaml, mem_limit 256m

**Checkpoint**: US1 complete — Generator produces valid transactions to Kafka at configurable rate. Can be tested independently by running `docker compose up kafka generator` and consuming from raw-transactions topic.

---

## Phase 4: User Story 2 — Real-Time Fraud and Anomaly Alerting (Priority: P2)

**Goal**: Build a Java Flink DataStream job that consumes transactions from Kafka, validates them, evaluates configurable alerting rules, generates alerts to a Kafka topic, routes invalid records to DLQ, and writes all valid transactions + alerts to Iceberg.

**Independent Test**: Send known transaction patterns (normal and suspicious) through Flink, verify alerts are generated for rule-matching transactions within 5 seconds, no alerts for normal transactions, and malformed records routed to DLQ.

### Tests for User Story 2 (MANDATORY per Constitution v2.0.0 III)

> **CRITICAL: Write these tests FIRST, ensure they FAIL before implementation**

- [x] T037 [P] [US2] Contract test: validate generated alerts against alerts.schema.json in tests/contract/test_alert_schema.py
- [x] T038 [P] [US2] Contract test: validate dead letter records against dead-letter.schema.json in tests/contract/test_dead_letter_schema.py
- [x] T039 [P] [US2] Java unit test: TransactionValidator schema validation (valid/invalid/missing fields) in flink-jobs/src/test/java/com/learnde/pipeline/functions/TransactionValidatorTest.java
- [x] T040 [P] [US2] Java unit test: AlertEvaluator rule evaluation with KeyedProcessFunction test harness in flink-jobs/src/test/java/com/learnde/pipeline/functions/AlertEvaluatorTest.java — MUST include test case for multi-rule triggering: a single transaction matching both HighValueRule and UnusualHourRule emits two independent alerts each referencing the same transaction_id (spec.md edge case line 105)
- [x] T041 [P] [US2] Java unit test: HighValueRule threshold detection in flink-jobs/src/test/java/com/learnde/pipeline/rules/HighValueRuleTest.java
- [x] T042 [P] [US2] Java unit test: RapidActivityRule velocity windowed detection in flink-jobs/src/test/java/com/learnde/pipeline/rules/RapidActivityRuleTest.java — MUST include test case for event-time semantics: send 5 transactions with event timestamps within 1 minute but arriving out of order (processing-time shuffled), verify alert triggers based on event-time window not processing-time (FR-015)
- [x] T043 [P] [US2] Java unit test: UnusualHourRule time-based detection in flink-jobs/src/test/java/com/learnde/pipeline/rules/UnusualHourRuleTest.java
- [x] T044 [US2] Integration test: end-to-end pipeline flow (generator → Kafka → Flink → Iceberg + alerts) in tests/integration/test_pipeline_end_to_end.py

### Implementation for User Story 2

- [x] T045 [P] [US2] Create Transaction POJO with Jackson JSON annotations in flink-jobs/src/main/java/com/learnde/pipeline/models/Transaction.java
- [x] T046 [P] [US2] Create Alert POJO with Jackson JSON annotations in flink-jobs/src/main/java/com/learnde/pipeline/models/Alert.java
- [x] T047 [P] [US2] Create DeadLetterRecord POJO with Jackson JSON annotations in flink-jobs/src/main/java/com/learnde/pipeline/models/DeadLetterRecord.java
- [x] T048 [P] [US2] Implement TransactionDeserializer (JSON bytes → Transaction) with error handling in flink-jobs/src/main/java/com/learnde/pipeline/serialization/TransactionDeserializer.java
- [x] T049 [P] [US2] Implement AlertSerializer (Alert → JSON bytes) in flink-jobs/src/main/java/com/learnde/pipeline/serialization/AlertSerializer.java
- [x] T050 [US2] Implement TransactionValidator as ProcessFunction: validate all fields per data-model.md rules, split output to valid stream + DLQ side output in flink-jobs/src/main/java/com/learnde/pipeline/functions/TransactionValidator.java
- [x] T051 [P] [US2] Create AlertingRule interface with evaluate(Transaction) method in flink-jobs/src/main/java/com/learnde/pipeline/rules/AlertingRule.java
- [x] T052 [P] [US2] Implement HighValueRule (threshold check on amount) in flink-jobs/src/main/java/com/learnde/pipeline/rules/HighValueRule.java
- [x] T053 [P] [US2] Implement RapidActivityRule (velocity check using ValueState + event-time timer, 5 tx in 1 min window) in flink-jobs/src/main/java/com/learnde/pipeline/rules/RapidActivityRule.java
- [x] T054 [P] [US2] Implement UnusualHourRule (time-based check against configurable quiet hours) in flink-jobs/src/main/java/com/learnde/pipeline/rules/UnusualHourRule.java
- [x] T055 [US2] Implement RuleConfigLoader to load alerting rules from YAML config with hot-reload support in flink-jobs/src/main/java/com/learnde/pipeline/rules/RuleConfigLoader.java
- [x] T056 [US2] Implement AlertEvaluator as KeyedProcessFunction: evaluate transaction against all enabled rules, emit alerts per matching rule, set is_flagged on transaction in flink-jobs/src/main/java/com/learnde/pipeline/functions/AlertEvaluator.java
- [x] T057 [US2] Implement IcebergSinkBuilder: configure Iceberg table writer for transactions and alerts tables (Parquet, ZSTD, 128MB target file size, checkpoint interval 5-10 min) in flink-jobs/src/main/java/com/learnde/pipeline/functions/IcebergSinkBuilder.java
- [x] T058 [US2] Implement TransactionPipeline main job: wire Kafka source → deserialize → validate → split (valid/DLQ) → alert evaluate → Iceberg sinks + Kafka alert sink in flink-jobs/src/main/java/com/learnde/pipeline/TransactionPipeline.java
- [x] T059 [US2] Create Flink job Dockerfile or configure Flink containers to build and submit TransactionPipeline JAR on startup, mount config/alerting-rules.yaml as volume
- [x] T060 [US2] Add structured logging throughout Flink job (SLF4J + JSON layout): validation failures, alerts generated, DLQ routing, checkpoint completions

**Checkpoint**: US2 complete — Flink processes transactions in real time, generates alerts for suspicious patterns within 5 seconds, routes invalid records to DLQ, writes to Iceberg. Test with known fraud patterns.

---

## Phase 5: User Story 3 — Store Transactions for Historical Analysis (Priority: P3)

**Goal**: Enable analytical queries over Iceberg tables using DuckDB + PyIceberg, support time-travel queries, schema evolution, session metadata capture, and compaction.

**Independent Test**: Write known transactions to Iceberg, query with DuckDB/PyIceberg to verify records exist, partitioning is correct, time-travel works, and analytical queries return expected results within 10 seconds for 1M records.

### Tests for User Story 3 (MANDATORY per Constitution v2.0.0 III)

> **CRITICAL: Write these tests FIRST, ensure they FAIL before implementation**

- [x] T061 [P] [US3] Unit test: query.py CLI argument parsing and query construction in tests/unit/test_query_script.py
- [x] T062 [P] [US3] Integration test: DuckDB/PyIceberg reads transactions from Iceberg REST catalog in tests/integration/test_iceberg_queries.py
- [x] T063 [US3] Integration test: time-travel query returns historical snapshot data in tests/integration/test_iceberg_queries.py

### Implementation for User Story 3

- [x] T064 [US3] Implement scripts/query.py: DuckDB + PyIceberg analytics CLI with subcommands (--table transactions/alerts, --limit, --date-range, --account-id, --show-alerts, --join-alerts)
- [x] T065 [US3] Add time-travel query support to scripts/query.py: --snapshot-id and --as-of-timestamp flags using PyIceberg snapshot resolution
- [x] T066 [US3] Add schema evolution demonstration: script or documentation showing how to add a new column to the transactions Iceberg table without rewriting existing data
- [x] T067 [US3] Implement session metadata capture: write pipeline_session_metadata records to Iceberg table (session_id, start/end timestamps, record counts, latency percentiles) from generator or a lightweight aggregator
- [x] T068 [US3] Implement compaction script: daily rewrite_data_files, expire snapshots older than 3 days (keep min 10), remove orphan files (weekly) using PyIceberg API
- [x] T069 [US3] Add structured logging for analytics queries (query type, execution time, row count returned) in scripts/query.py

**Checkpoint**: US3 complete — Analysts can query stored transactions and alerts via DuckDB/PyIceberg CLI. Time-travel, schema evolution, and compaction all functional.

---

## Phase 6: User Story 4 — One-Command Local Environment Setup (Priority: P4)

**Goal**: Polish the Docker Compose environment for single-command startup/shutdown with health checks, memory validation, graceful shutdown, restart recovery, and 24-hour stability.

**Independent Test**: Run `docker compose up -d`, verify all services reach healthy state within 3 minutes, confirm end-to-end data flow, run `docker compose down` and verify graceful shutdown within 1 minute.

### Tests for User Story 4 (MANDATORY per Constitution v2.0.0 III)

> **CRITICAL: Write these tests FIRST, ensure they FAIL before implementation**

- [x] T070 [P] [US4] Integration test: all services reach healthy state within 3 minutes after docker compose up in tests/integration/test_environment_setup.py
- [x] T071 [P] [US4] Integration test: pipeline resumes from checkpoint after forced service restart with zero data loss in tests/integration/test_restart_recovery.py
- [x] T072 [US4] Integration test: total memory consumption stays under 6GB during sustained 10 tx/sec operation in tests/integration/test_resource_limits.py

### Implementation for User Story 4

- [x] T073 [US4] Implement scripts/health-check.sh: query all service health endpoints, display status table (service, status, uptime), show key metrics (throughput, latency, error counts, Kafka consumer lag) from structured logs and Flink REST API
- [x] T074 [US4] Add Docker healthchecks for all services in docker-compose.yml: Kafka (kafka-broker-api-versions), Iceberg REST (curl /v1/namespaces), Flink (curl /overview), Generator (process check)
- [x] T075 [US4] Configure persistent Docker volumes for Iceberg warehouse data and Flink checkpoints to survive restarts (FR-013)
- [x] T076 [US4] Add graceful shutdown handling: generator SIGTERM handler (flush Kafka buffer, log final stats), Flink cancel-with-savepoint via REST API
- [x] T077 [US4] Validate memory limits: add mem_limit to all services in docker-compose.yml per research.md R5 budget (total ~3.4GB), add docker stats verification to health-check.sh
- [x] T078 [US4] Add service dependency ordering in docker-compose.yml with depends_on + healthcheck conditions (kafka → iceberg-rest → flink → generator)

**Checkpoint**: US4 complete — `docker compose up -d` starts everything, health-check.sh confirms status, data survives restarts, memory stays under 6GB.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Documentation, DMBOK compliance verification, performance validation, and cross-cutting improvements

- [ ] T079 [P] Add Google-style docstrings to all public Python functions per Constitution v2.0.0 Code Quality Standards
- [ ] T080 [P] Add Javadoc comments to all public Java classes and methods in flink-jobs/
- [ ] T081 [P] Create data dictionary documentation: business definitions for all entities (Transaction, Alert, Account, Alerting Rule) linking to data-model.md
- [ ] T082 Verify DMBOK compliance across all knowledge areas: run through quality criteria checklists from constitution.md (Data Governance, Architecture, Modeling, Storage, Security, Integration, Metadata, Quality)
- [ ] T083 [P] Performance baseline validation: measure and log throughput (10 tx/sec sustained), end-to-end latency (p50/p95/p99), alert generation latency (<5s p95), query response time (<10s for 1M records)
- [ ] T084 Run quickstart.md validation: execute all quickstart steps on clean environment, verify all commands work as documented
- [ ] T085 Verify 80% test coverage for Python components (pytest-cov), document coverage report
- [ ] T086 Verify Java test coverage for Flink jobs (JaCoCo or Maven Surefire), document coverage report
- [ ] T087 [P] Add .gitignore entries for generated files: Python __pycache__, Java target/, Docker volumes, .env files, IDE files
- [ ] T088 Final end-to-end validation: 1-hour sustained run at 10 tx/sec, verify all success criteria (SC-001 through SC-010) from spec.md

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **US1 (Phase 3)**: Depends on Foundational (Phase 2) — No dependencies on other stories
- **US2 (Phase 4)**: Depends on Foundational (Phase 2) — Can start after Phase 2, but integrates with US1's Kafka output for end-to-end testing
- **US3 (Phase 5)**: Depends on Foundational (Phase 2) — Can start after Phase 2, but needs Iceberg data from US2 for meaningful queries
- **US4 (Phase 6)**: Depends on Foundational (Phase 2) — Integrates all services, best done after US1-US3 are functional
- **Polish (Phase 7)**: Depends on all desired user stories being complete

### User Story Dependencies

- **US1 (P1)**: Independent after Phase 2. Delivers standalone value as a synthetic data generator.
- **US2 (P2)**: Independent after Phase 2 for unit tests. End-to-end integration test (T044) requires US1 running to produce transactions.
- **US3 (P3)**: Independent after Phase 2 for query tooling. Meaningful queries require data written by US2.
- **US4 (P4)**: Infrastructure polish. Best done after US1-US3 are functional so health checks cover all services.

### Within Each User Story

- Tests MUST be written and FAIL before implementation (Constitution v2.0.0 Principle III)
- Data quality tests required for data engineering features (Constitution v2.0.0 Principle II)
- Models before services/generators
- Core logic before integration (e.g., rules before evaluator, producer before main.py)
- DMBOK compliance checks: metadata capture, data quality validation, security controls
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks T002-T007 marked [P] can run in parallel
- Foundational tasks T012-T015 and T016-T017 can run in parallel (within Phase 2)
- All US1 tests (T018-T025) can run in parallel
- US1 models T027-T028 can run in parallel; T031-T032 can run in parallel
- All US2 tests (T037-T043) can run in parallel
- US2 POJOs T045-T047 can run in parallel; serializers T048-T049 can run in parallel; rules T051-T054 can run in parallel
- US3 tests T061-T062 can run in parallel
- US4 tests T070-T071 can run in parallel
- Polish tasks T079-T081, T083, T087 can run in parallel

---

## Parallel Example: User Story 1

```bash
# Launch all tests for US1 together (TDD — write tests FIRST):
Task: "T018 Contract test for raw-transactions schema in tests/contract/test_raw_transaction_schema.py"
Task: "T019 Unit test for Account model in tests/unit/test_account_model.py"
Task: "T020 Unit test for Transaction model in tests/unit/test_transaction_model.py"
Task: "T021 Unit test for transaction_generator in tests/unit/test_transaction_generator.py"
Task: "T022 Unit test for fraud_injector in tests/unit/test_fraud_injector.py"
Task: "T023 Unit test for rate_limiter in tests/unit/test_rate_limiter.py"
Task: "T024 Unit test for config_loader in tests/unit/test_config_loader.py"
Task: "T025 Unit test for Kafka producer in tests/unit/test_producer.py"

# Then launch parallelizable models:
Task: "T027 Create Account model in generator/src/models/account.py"
Task: "T028 Create Transaction model in generator/src/models/transaction.py"

# Then launch parallelizable lib modules:
Task: "T031 Implement rate_limiter in generator/src/lib/rate_limiter.py"
Task: "T032 Implement config_loader in generator/src/lib/config_loader.py"
```

## Parallel Example: User Story 2

```bash
# Launch all Java unit tests together (TDD — write tests FIRST):
Task: "T039 TransactionValidatorTest in flink-jobs/src/test/.../TransactionValidatorTest.java"
Task: "T040 AlertEvaluatorTest in flink-jobs/src/test/.../AlertEvaluatorTest.java"
Task: "T041 HighValueRuleTest in flink-jobs/src/test/.../HighValueRuleTest.java"
Task: "T042 RapidActivityRuleTest in flink-jobs/src/test/.../RapidActivityRuleTest.java"
Task: "T043 UnusualHourRuleTest in flink-jobs/src/test/.../UnusualHourRuleTest.java"

# Then launch parallelizable POJOs:
Task: "T045 Transaction POJO in flink-jobs/src/main/.../models/Transaction.java"
Task: "T046 Alert POJO in flink-jobs/src/main/.../models/Alert.java"
Task: "T047 DeadLetterRecord POJO in flink-jobs/src/main/.../models/DeadLetterRecord.java"

# Then launch parallelizable rules:
Task: "T051 AlertingRule interface in flink-jobs/src/main/.../rules/AlertingRule.java"
Task: "T052 HighValueRule in flink-jobs/src/main/.../rules/HighValueRule.java"
Task: "T053 RapidActivityRule in flink-jobs/src/main/.../rules/RapidActivityRule.java"
Task: "T054 UnusualHourRule in flink-jobs/src/main/.../rules/UnusualHourRule.java"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (T001-T008)
2. Complete Phase 2: Foundational (T009-T017) — CRITICAL, blocks all stories
3. Complete Phase 3: User Story 1 (T018-T036)
4. **STOP and VALIDATE**: Start generator + Kafka, consume from raw-transactions topic, verify schema compliance and rate
5. Deploy/demo if ready — you have a working synthetic data generator

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → Generator streaming to Kafka (MVP!)
3. Add User Story 2 → Test independently → Real-time fraud alerting with Iceberg persistence
4. Add User Story 3 → Test independently → Historical analytics with DuckDB/PyIceberg
5. Add User Story 4 → Test independently → Polished one-command environment
6. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (Python generator)
   - Developer B: User Story 2 (Java Flink jobs)
   - Developer C: User Story 3 (Python analytics tooling)
3. Stories complete and integrate independently
4. Developer D (or any) handles User Story 4 after US1-US3 merge

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
- Constitutional waiver applied: no TLS (Principle V Learning Context — local-only, synthetic data, 6GB memory constraint)
