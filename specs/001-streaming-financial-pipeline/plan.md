# Implementation Plan: Streaming Financial Transaction Pipeline

**Branch**: `001-streaming-financial-pipeline` | **Date**: 2026-02-16 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-streaming-financial-pipeline/spec.md`

## Summary

Build a local streaming data pipeline on Docker Compose for MacBook Air M1 that generates synthetic financial transactions (Python), streams them through Apache Kafka, processes them in real time with Apache Flink for rule-based fraud/anomaly alerting, and persists all data to Apache Iceberg tables for historical analysis. The architecture follows a Kappa-style streaming pattern with event-time semantics, dead letter routing for malformed records, and a CLI health monitoring script. All services must run within 6GB RAM with single-command startup/shutdown.

## Technical Context

**Language/Version**: Python 3.11+ (data generator, CLI tools), Java/Scala for Flink jobs (or PyFlink)  
**Primary Dependencies**: Apache Kafka (message broker), Apache Flink (stream processing), Apache Iceberg (table format), Apache Arrow (columnar in-memory format — used implicitly by PyIceberg and DuckDB for zero-copy table reads; no direct application code required), confluent-kafka (Python Kafka client), Docker Compose  
**Storage**: Apache Iceberg tables on local filesystem (Parquet file format), Kafka topics for streaming  
**Testing**: pytest (Python components), Flink test harness (stream processor), docker compose integration tests  
**Target Platform**: macOS ARM64 (MacBook Air M1), Docker containers (linux/arm64)  
**Project Type**: single (multi-container data pipeline, single repo)  
**Performance Goals**: 10 tx/sec sustained generation, <5s alert latency (p95), <30s storage latency (p95), 10s query response for 1M records  
**Constraints**: <6GB total memory, ARM64 compatible images, local-only networking, no TLS (constitutional waiver), 20GB disk budget  
**Scale/Scope**: ~864K transactions/day, ~500MB-1GB/day storage, single-node local execution, 24-hour continuous operation

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Verify compliance with core principles from `.specify/memory/constitution.md` (v2.0.0):

### Core Principles (7 Principles)

- **I. Modularity & Reusability**: PASS — Pipeline is decomposed into 4 independent components (generator, Kafka broker, Flink processor, Iceberg storage). Each component is self-contained in its own Docker container with clear interfaces (Kafka topics as contracts). Configuration externalized via YAML files and environment variables. CLI interfaces planned for health monitoring script. Components are independently testable.

- **II. Data Quality & Governance**: PASS — Input/output schemas explicitly defined (spec lines 141-143). Six quality dimensions addressed: uniqueness (transaction_id), validity (ISO formats, enums), completeness (non-null required fields), timeliness (30s SLA), accuracy (realistic synthetic patterns), consistency (referential integrity alerts→transactions). Quality metrics defined (99.9% validation pass rate). Dead letter routing for invalid records with error details.

- **III. Test-First Development (NON-NEGOTIABLE)**: PASS — TDD workflow will be followed. Unit tests for generator data quality, Flink rule evaluation logic. Integration tests for end-to-end pipeline flow. Contract tests for Kafka message schemas. Data quality tests for validation rules. 80% coverage target acknowledged. Test naming convention: `test_<function>_<scenario>_<expected_outcome>()`.

- **IV. Metadata & Lineage**: PASS — Technical metadata captured per session (session_id, record counts, latency percentiles). Operational metadata via health endpoints and structured JSON logs. Lineage fully documented: Generator → Kafka → Flink → Iceberg/Kafka DLQ. Correlation IDs via transaction_id. Deterministic rules ensure reproducibility.

- **V. Data Security & Privacy**: PASS (with documented waiver) — Encryption at rest and TLS in transit waived per constitutional Learning Context clause (spec line 169). Justification: local-only Docker network, synthetic data, 6GB memory constraint. PII identified (account_id as pseudonymous). Access control via private container network. Secrets via environment variables. Audit logging for all pipeline operations.

- **VI. Data Architecture & Integration**: PASS — Kappa architecture (streaming-only) chosen, justified by real-time alerting requirement. Layered: ingestion (generator) → streaming (Kafka) → processing (Flink) → quality (validation) → storage (Iceberg). Error handling: retry with backoff, dead letter queues, checkpoint recovery. Event-driven integration via Kafka topics. Technology choices documented.

- **VII. Simplicity & Performance**: PASS (with justified complexity) — Streaming architecture (Kafka + Flink) is more complex than batch, but justified by the core real-time alerting requirement (spec FR-003: <5s latency). Performance baselines defined (10 tx/sec, 6GB memory). Docker Compose keeps deployment simple. No premature optimization — measure-first approach.

### DMBOK Knowledge Areas (if applicable)

- **Data Governance**: PASS — Data classified as confidential. Ownership implicit (single developer learning project). Business glossary terms defined in spec Key Entities. Compliance awareness documented (PCI-DSS, SOX concepts).
- **Data Architecture**: PASS — Kappa streaming pattern chosen. Layers clearly defined. Data flow diagram in spec. Architecture decision recorded in research.md.
- **Data Modeling**: PASS — 4 entities defined (Transaction, Alert, Alerting Rule, Account). State transitions documented. Schema versioning via Iceberg schema evolution.
- **Data Storage**: PASS — Iceberg on local filesystem chosen. Parquet columnar format. Date-based partitioning. Persistent Docker volumes for restart survival. ~500MB-1GB/day storage estimate within 20GB budget.
- **Data Security**: PASS (waiver) — Constitutional waiver documented. Architecture demonstrates security awareness. Secrets not hardcoded.
- **Data Integration**: PASS — Streaming pattern via Kafka. Event-time semantics for windowed operations. Idempotency via Flink checkpointing. Dead letter queue for unprocessable records.
- **Metadata Management**: PASS — Technical, business, and operational metadata requirements defined. Structured JSON logs. Health endpoints. Metadata Iceberg table for session metrics.
- **Data Quality**: PASS — 6 quality dimensions addressed. Validation rules explicit. Quality SLA: 99.9% pass rate. Dead letter routing with error context. Quality metrics captured.

**Compliance Status**: PASS

**Post-Design Re-check (Phase 1 gate)**: PASS — All 7 principles and 8 DMBOK areas re-verified after data model, contracts, project structure, and quickstart were finalized. No new violations introduced. Data contracts (JSON Schema) strengthen Principle II (Data Quality) and DMBOK Data Modeling. Explicit test directory structure strengthens Principle III (TDD). Memory budget unchanged at ~3.4 GB.

**Complexity Justification**:

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Kafka + Flink streaming stack | Real-time alerting requirement (FR-003: <5s latency) | Batch processing cannot meet sub-5-second alerting SLA |
| Encryption waiver (Principle V) | Local-only learning environment with synthetic data | Full TLS would consume ~500MB+ additional memory, exceeding 6GB budget on 8GB M1 |

## Project Structure

### Documentation (this feature)

```text
specs/[###-feature]/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
docker-compose.yml                  # All services: Kafka, Flink, Iceberg, Generator
config/
├── generator.yaml                  # Generator rate, patterns, fraud injection %
└── alerting-rules.yaml             # Flink alerting rule definitions (YAML)

generator/                          # Python data generator (Docker: python:3.11-slim)
├── Dockerfile
├── requirements.txt                # confluent-kafka, faker, pyyaml
└── src/
    ├── __init__.py
    ├── main.py                     # Entry point: token bucket loop, config watch
    ├── models/
    │   ├── __init__.py
    │   ├── transaction.py          # Transaction dataclass + JSON serialization
    │   └── account.py              # Account pool management
    ├── generators/
    │   ├── __init__.py
    │   ├── transaction_generator.py  # Realistic distribution logic
    │   └── fraud_injector.py       # Anomalous pattern injection
    ├── kafka/
    │   ├── __init__.py
    │   └── producer.py             # confluent-kafka producer + circuit breaker
    └── lib/
        ├── __init__.py
        ├── rate_limiter.py         # Token bucket implementation
        └── config_loader.py        # YAML config watch + reload

flink-jobs/                         # Java Flink stream processor (Docker: flink:1.20-java17)
├── pom.xml                         # Maven build (Flink 1.20, Iceberg connector)
└── src/
    ├── main/java/com/learnde/pipeline/
    │   ├── TransactionPipeline.java      # Main Flink job entry point
    │   ├── models/
    │   │   ├── Transaction.java          # Transaction POJO
    │   │   ├── Alert.java                # Alert POJO
    │   │   └── DeadLetterRecord.java     # DLQ record POJO
    │   ├── functions/
    │   │   ├── TransactionValidator.java # Schema validation → valid / DLQ
    │   │   ├── AlertEvaluator.java       # Rule evaluation (KeyedProcessFunction)
    │   │   └── IcebergSinkBuilder.java   # Iceberg table writer config
    │   ├── rules/
    │   │   ├── AlertingRule.java          # Rule interface
    │   │   ├── HighValueRule.java         # Threshold rule
    │   │   ├── RapidActivityRule.java     # Velocity rule (windowed)
    │   │   ├── UnusualHourRule.java       # Time-based rule
    │   │   └── RuleConfigLoader.java     # YAML rule config loader
    │   └── serialization/
    │       ├── TransactionDeserializer.java  # JSON → Transaction
    │       └── AlertSerializer.java         # Alert → JSON
    └── test/java/com/learnde/pipeline/
        ├── functions/
        │   ├── TransactionValidatorTest.java
        │   └── AlertEvaluatorTest.java
        └── rules/
            ├── HighValueRuleTest.java
            ├── RapidActivityRuleTest.java
            └── UnusualHourRuleTest.java

scripts/                            # Operational CLI tools (run from host or container)
├── health-check.sh                 # Pipeline health + metrics CLI
└── query.py                        # DuckDB/PyIceberg analytics queries

tests/                              # Python tests (generator, integration, contracts)
├── conftest.py                     # Shared fixtures
├── unit/
│   ├── test_transaction_generator.py
│   ├── test_fraud_injector.py
│   ├── test_rate_limiter.py
│   ├── test_producer.py
│   └── test_config_loader.py
├── integration/
│   ├── test_generator_to_kafka.py       # Generator → Kafka end-to-end
│   ├── test_pipeline_end_to_end.py      # Full pipeline flow
│   └── test_restart_recovery.py         # Checkpoint recovery
└── contract/
    ├── test_raw_transaction_schema.py   # Validate against JSON Schema
    ├── test_alert_schema.py
    └── test_dead_letter_schema.py
```

**Structure Decision**: Multi-component layout with separate directories per service (`generator/`, `flink-jobs/`), shared `config/`, `scripts/`, and `tests/` at the root. This reflects the 4-container Docker Compose architecture where each service has its own build context. Python tests cover the generator, integration flows, and contract validation. Java tests live inside the Maven project under `flink-jobs/src/test/`.

## Complexity Tracking

> Violations documented and justified in Constitution Check above.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Kafka + Flink streaming stack | Real-time alerting requirement (FR-003: <5s latency) | Batch processing cannot meet sub-5-second alerting SLA |
| Encryption waiver (Principle V) | Local-only learning environment with synthetic data | Full TLS would consume ~500MB+ additional memory, exceeding 6GB budget on 8GB M1 |
