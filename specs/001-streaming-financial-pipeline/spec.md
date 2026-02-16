# Feature Specification: Streaming Financial Transaction Pipeline

**Feature Branch**: `001-streaming-financial-pipeline`  
**Created**: 2026-02-16  
**Status**: Draft  
**Input**: User description: "setup a local environment to run the data pipeline in macbook air m1. The scenario is the streaming pipeline to gather financial transactions record, alert real time based on rules and do analyze later. Store data in local with delta lake iceberg. The whole pipeline can be up by docker compose, I like to use apache kafka, apache arrow, apache flink, apache iceberg stack. Can use python to generate data instead of calling API"

## Clarifications

### Session 2026-02-16

- Q: How should encryption requirements be handled for this local learning environment? → A: Document explicit constitutional waiver with justification (local-only, synthetic data)
- Q: What state transition model should the generator follow for Transaction status? → A: Linear: pending → completed OR pending → failed; completed → reversed (no other transitions)
- Q: Should the spec normalize to specific technology names throughout? → A: Yes, use specific names (Kafka topic, Iceberg table, Flink job, etc.) instead of generic terms
- Q: What observability approach should the pipeline use? → A: Structured JSON logs plus a simple script/CLI to query pipeline health and key metrics
- Q: Should the spec document explicit volume targets for the 24-hour stability run? → A: Yes, ~864K transactions/day at default rate, estimated ~500MB-1GB/day storage in Iceberg/Parquet format

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Generate and Stream Financial Transactions (Priority: P1)

As a data engineer, I want to generate realistic financial transaction data and stream it continuously so that the pipeline has a reliable data source for processing without needing external API access.

A data generator produces synthetic financial transactions (purchases, withdrawals, transfers, refunds) with realistic patterns including varying amounts, merchant categories, geographic locations, and timestamps. The generator publishes transactions to a Kafka topic at a configurable rate so downstream consumers can process them in real time.

**Why this priority**: Without a working data source streaming into the pipeline, no other component can function. This is the foundation that everything else depends on.

**Independent Test**: Can be fully tested by starting the generator, verifying transactions appear in the Kafka topic with correct schema, and confirming the generation rate matches configuration. Delivers value as a standalone synthetic data generator for any streaming pipeline.

**Acceptance Scenarios**:

1. **Given** the pipeline environment is running, **When** the data generator starts, **Then** synthetic financial transactions are published to the Kafka topic at the configured rate (default: 10 transactions/second)
2. **Given** the generator is running, **When** a transaction is published, **Then** it contains all required fields: transaction_id, timestamp, account_id, amount, currency, merchant_name, merchant_category, transaction_type, location_country, and status
3. **Given** the generator is running, **When** 1000 transactions are produced, **Then** the data distribution reflects realistic patterns (multiple transaction types, varying amounts from $0.50 to $50,000, multiple currencies, multiple merchant categories)
4. **Given** the generator is configured with a specific rate, **When** rate is changed, **Then** the generator adjusts throughput without restart

---

### User Story 2 - Real-Time Fraud and Anomaly Alerting (Priority: P2)

As a fraud analyst, I want the system to evaluate every incoming transaction against a set of configurable rules in real time and generate alerts for suspicious activity so that potentially fraudulent transactions can be investigated promptly.

The Flink stream processor evaluates each transaction against predefined rules such as: transactions exceeding a threshold amount, multiple transactions from the same account within a short window, transactions from unusual geographic locations, or transactions at unusual hours. When a rule triggers, an alert record is generated containing the original transaction details, the rule that triggered, a severity level, and a timestamp.

**Why this priority**: Real-time alerting is the core value proposition of a streaming pipeline. Without it, a simple batch pipeline would suffice. This story validates the streaming architecture's purpose.

**Independent Test**: Can be tested by sending known transaction patterns (both normal and suspicious) through the Flink stream processor and verifying alerts are generated for rule-matching transactions and not generated for normal ones.

**Acceptance Scenarios**:

1. **Given** a transaction exceeding $10,000 is published, **When** the Flink stream processor evaluates it, **Then** a high-severity alert is generated within 5 seconds of the transaction timestamp
2. **Given** the same account produces 5 or more transactions within 1 minute, **When** the Flink stream processor detects this pattern, **Then** a medium-severity alert is generated for rapid-activity detection
3. **Given** a normal transaction (under thresholds, normal patterns), **When** the Flink stream processor evaluates it, **Then** no alert is generated and the transaction is stored for analytics
4. **Given** alerting rules are defined in a configuration file, **When** a rule threshold is updated, **Then** the Flink stream processor applies the new threshold without pipeline restart
5. **Given** the Flink stream processor is running, **When** an alert is generated, **Then** the alert includes: original transaction details, rule_name, severity (low/medium/high/critical), alert_timestamp, and description

---

### User Story 3 - Store Transactions for Historical Analysis (Priority: P3)

As a data analyst, I want all processed transactions and alerts to be stored in a queryable local Iceberg lakehouse so that I can run analytical queries for trend analysis, reporting, and investigation.

All transactions (both normal and flagged) are persisted to a local Iceberg lakehouse in a columnar table format. The storage supports time-travel queries (viewing data as it existed at a previous point), schema evolution, and efficient analytical queries. Data is partitioned by date for query performance. Alerts are stored in a separate Iceberg table linked to transactions.

**Why this priority**: Storage and analytics complete the pipeline's value chain. While streaming and alerting deliver immediate value, historical analysis enables pattern discovery, rule refinement, and business intelligence.

**Independent Test**: Can be tested by writing a batch of known transactions to storage, then querying the Iceberg lakehouse to verify records exist, partitioning is correct, and analytical queries return expected results.

**Acceptance Scenarios**:

1. **Given** transactions are flowing through the pipeline, **When** they are processed, **Then** 100% of transactions are persisted to the transactions Iceberg table within 30 seconds
2. **Given** transactions are stored, **When** an analyst queries by date range, **Then** results are returned within 10 seconds for up to 1 million records
3. **Given** transactions are stored with time-travel capability, **When** an analyst queries a historical snapshot, **Then** the data reflects the state at that point in time
4. **Given** alerts are stored in a separate Iceberg table, **When** an analyst queries alerts joined with transactions, **Then** the full context (transaction + alert details) is available
5. **Given** the Iceberg lakehouse is running, **When** the schema needs a new column, **Then** the schema can evolve without rewriting existing data

---

### User Story 4 - One-Command Local Environment Setup (Priority: P4)

As a data engineer, I want to bring up the entire streaming pipeline on my MacBook Air M1 with a single command so that I can start developing and experimenting without complex manual setup.

The entire pipeline (data generator, Kafka broker, Flink stream processor, Iceberg lakehouse, and any supporting services) runs locally via Docker Compose. A single command starts all services with proper networking, health checks, and dependency ordering. The environment is resource-conscious to run comfortably on a MacBook Air M1 (8-16GB RAM).

**Why this priority**: While critical for usability, the environment setup is infrastructure that supports the other stories. The pipeline components (stories 1-3) define what gets deployed; this story defines how.

**Independent Test**: Can be tested by running the single startup command on a MacBook Air M1, verifying all services reach healthy state, and confirming end-to-end data flow (generator -> stream -> processor -> lakehouse).

**Acceptance Scenarios**:

1. **Given** the repository is cloned on a MacBook Air M1, **When** the user runs the single startup command, **Then** all pipeline services start and reach healthy state within 3 minutes
2. **Given** all services are running, **When** the user checks resource usage, **Then** total memory consumption is under 6GB to leave headroom on an 8GB machine
3. **Given** all services are running, **When** the user runs a shutdown command, **Then** all services stop gracefully and release resources within 1 minute
4. **Given** a service fails during operation, **When** it is restarted, **Then** the pipeline resumes processing without data loss (at-least-once delivery)
5. **Given** the environment was previously running, **When** the user starts it again, **Then** previously stored data in the Iceberg lakehouse is preserved across restarts

---

### Edge Cases

- What happens when the data generator produces a transaction with a negative amount? The system should treat refunds as valid negative amounts but reject amounts of exactly zero.
- What happens when the Kafka broker is temporarily unavailable? The generator should buffer messages and retry with exponential backoff, and the Flink stream processor should resume from its last checkpoint.
- What happens when the Flink stream processor receives a malformed transaction (missing required fields)? It should route the record to a dead letter Kafka topic and log the error without crashing.
- What happens when the Iceberg lakehouse storage reaches disk capacity? The system should emit a warning alert when storage exceeds 80% capacity and stop writes gracefully at 95% with clear error messages.
- What happens when two alerting rules trigger on the same transaction? Both alerts should be generated independently, each referencing the same transaction.
- What happens when the system clock skews between containers? Transactions should use event-time (embedded timestamp) rather than processing-time for all windowed operations.
- What happens when the M1 Mac goes to sleep during pipeline operation? On wake, services should recover automatically via health checks and resume processing from checkpoints.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST generate synthetic financial transactions with realistic data patterns including varying amounts ($0.50 to $50,000), multiple currencies (USD, EUR, GBP, JPY at minimum), multiple merchant categories (retail, dining, travel, online, groceries, entertainment, utilities), and multiple transaction types (purchase, withdrawal, transfer, refund)
- **FR-002**: System MUST publish generated transactions to a Kafka topic at a configurable rate (default: 10 transactions/second, configurable from 1 to 1000 transactions/second)
- **FR-003**: System MUST process each incoming transaction via Flink against configurable alerting rules in real time with end-to-end latency under 5 seconds from generation to alert
- **FR-004**: System MUST support at minimum these alerting rules: high-value transaction threshold, rapid successive transactions from same account (velocity check), and unusual transaction hour detection (configurable quiet hours)
- **FR-005**: System MUST persist all transactions to a local Iceberg lakehouse with date-based partitioning
- **FR-006**: System MUST persist all generated alerts to a separate Iceberg alerts table linked to transactions by transaction_id
- **FR-007**: System MUST support time-travel queries on stored Iceberg data (query data as of a specific timestamp)
- **FR-008**: System MUST support Iceberg schema evolution (adding new columns) without requiring data rewrite
- **FR-009**: System MUST be deployable on a MacBook Air M1 (ARM64 architecture) with total memory usage under 6GB
- **FR-010**: System MUST start all services via Docker Compose with a single command and shut down gracefully with a single command
- **FR-011**: System MUST handle Kafka broker unavailability with automatic retry and exponential backoff
- **FR-012**: System MUST route malformed records to a dead letter Kafka topic with error details
- **FR-013**: System MUST preserve data across pipeline restarts (persistent storage volumes)
- **FR-014**: System MUST provide health check endpoints for all services to enable automatic recovery
- **FR-015**: System MUST use event-time semantics (transaction timestamp) for all windowed operations, not processing-time
- **FR-016**: Alerting rules MUST be configurable via external configuration without code changes or pipeline restart
- **FR-017**: System MUST provide a simple CLI script to query pipeline health status and key operational metrics (throughput, latency, error counts, Kafka consumer lag) from structured logs and health endpoints
- **FR-018**: System MUST support sustained operation producing ~864,000 transactions per 24-hour run at default rate (10 tx/sec), with estimated storage consumption of 500MB-1GB/day in Iceberg/Parquet format

### Key Entities

- **Transaction**: A financial transaction event. Key attributes: transaction_id (unique identifier), timestamp (when the transaction occurred), account_id (the account initiating the transaction), amount (monetary value, positive for charges, negative for refunds), currency (ISO 4217 code), merchant_name, merchant_category (categorical classification), transaction_type (purchase/withdrawal/transfer/refund), location_country (ISO 3166 country code), status (pending/completed/failed/reversed). **State transitions follow a linear model**: pending → completed OR pending → failed; completed → reversed. No other transitions are valid.
- **Alert**: A notification generated when a transaction matches an alerting rule. Key attributes: alert_id (unique identifier), transaction_id (reference to triggering transaction), rule_name (which rule triggered), severity (low/medium/high/critical), alert_timestamp (when the alert was generated), description (human-readable explanation of why the alert fired)
- **Alerting Rule**: A configurable condition that evaluates transactions. Key attributes: rule_name (unique identifier), rule_type (threshold/velocity/time-based), parameters (threshold values, time windows, etc.), severity (default severity when triggered), enabled (active/inactive toggle)
- **Account**: A financial account generating transactions. Key attributes: account_id (unique identifier), account_type (checking/savings/credit), creation_date, country. Used by the data generator to create realistic transaction patterns per account.

### Data Quality Requirements *(mandatory for data engineering features)*

- **Input Schema**: Each transaction record MUST contain: transaction_id (string, UUID format), timestamp (ISO 8601 datetime with timezone), account_id (string, alphanumeric), amount (decimal, non-zero), currency (string, 3-letter ISO 4217), merchant_name (string, non-empty), merchant_category (string, from predefined enum), transaction_type (string, one of: purchase/withdrawal/transfer/refund), location_country (string, 2-letter ISO 3166), status (string, one of: pending/completed/failed/reversed)
- **Output Schema - Transactions Table**: All input fields plus: processing_timestamp (when the record was processed), partition_date (date extracted from timestamp for partitioning), is_flagged (boolean, whether any alert was triggered)
- **Output Schema - Alerts Table**: alert_id (string, UUID), transaction_id (string, foreign key), rule_name (string), severity (string, one of: low/medium/high/critical), alert_timestamp (ISO 8601 datetime), description (string)
- **Validation Rules**:
  - transaction_id MUST be unique (uniqueness dimension)
  - timestamp MUST be valid ISO 8601 and not in the future by more than 1 minute (validity)
  - amount MUST be non-zero (validity)
  - currency MUST be valid ISO 4217 code (validity)
  - merchant_category MUST be from predefined enum (validity)
  - transaction_type MUST be one of the allowed values (validity)
  - All required fields MUST be present and non-null (completeness)
  - Records should arrive within 30 seconds of generation (timeliness)
- **Quality Metrics**: Records generated per second, records processed per second, validation pass rate (target: 99.9% for generated data), end-to-end latency (generation to storage), alert generation latency, dead letter queue depth
- **Error Handling**: Malformed records are routed to a dead letter topic with error details (field name, expected format, actual value). Valid records continue processing. System logs validation failure counts per rule per minute.

### Metadata Requirements *(mandatory for data engineering features)*

- **Technical Metadata**: For each pipeline run/session: session_id, start_timestamp, record counts per stage (generated, published, processed, stored, alerts_generated, dead_lettered), throughput rates, end-to-end latency percentiles (p50, p95, p99)
- **Business Metadata**: Transaction field definitions and business meaning documented in a data dictionary. Alerting rule descriptions with business context (why each rule exists). Merchant category taxonomy with examples.
- **Operational Metadata**: Per-service health status, resource utilization (CPU, memory), Kafka consumer lag (consumer offset vs. latest offset), Flink checkpoint positions, error counts by type, dead letter queue depth
- **Lineage Information**: End-to-end data flow: Generator -> Kafka (raw-transactions topic) -> Flink Stream Processor (evaluate rules) -> Kafka (alerts topic) + Iceberg Lakehouse (transactions table, alerts table). Dead letter flow: Flink Stream Processor -> Kafka (dead-letter topic)
- **Metadata Storage**: Operational metadata exposed via service health endpoints and structured JSON logs. Business metadata in documentation files within the repository. Technical metadata logged per session to a metadata Iceberg table.

### Security & Privacy Requirements *(mandatory for data engineering features)*

- **Data Classification**: Transaction data is classified as **confidential** (contains account_id, financial amounts, merchant details). Alert data inherits the same classification. Generated data is synthetic but should be treated as real for security practice purposes.
- **PII/Sensitive Data**: account_id is pseudonymous PII (can identify account holder with external mapping). No real names, emails, or direct identifiers are generated. If account_id masking is needed for external sharing, SHA-256 hashing should be applied.
- **Access Control**: All services communicate within a private container network. No services are exposed to the public internet. Management UIs (if any) are accessible only on localhost.
- **Encryption**: **Constitutional waiver (Principle V — Data Security & Privacy)**: The constitution requires encryption at rest and TLS 1.2+ in transit. For this local-only learning environment with synthetic data, these requirements are waived with justification: (1) all services run within a private Docker Compose network on localhost with no public exposure, (2) all data is synthetic with no real PII, (3) the 6GB memory budget on M1 does not accommodate TLS termination overhead across all services. The architecture documents awareness of production encryption requirements. Container-to-container communication uses Docker internal network (no TLS). Data at rest in Iceberg relies on local filesystem permissions. Secrets (if any) are passed via environment variables, never hardcoded.
- **Audit Logging**: All pipeline operations are logged with timestamps, component names, and operation types. Alert generation events are logged with full context. Dead letter routing events include the reason for rejection.
- **Compliance**: This is a learning environment with synthetic data. Architecture demonstrates awareness of financial data compliance concepts (PCI-DSS awareness for card data, SOX awareness for financial reporting). No real regulatory requirements apply to synthetic data.

### Data Lineage Documentation *(mandatory for data engineering features)*

- **Source Systems**: Synthetic financial transaction data generator
- **Transformation Steps**:
  1. Generator creates transaction records with realistic patterns -> publishes to Kafka raw-transactions topic
  2. Flink stream processor consumes from raw-transactions topic -> validates schema and required fields
  3. Valid transactions evaluated against alerting rules -> alerts published to Kafka alerts topic
  4. Valid transactions enriched with processing metadata -> written to transactions Iceberg table
  5. Alerts enriched with context -> written to alerts Iceberg table
  6. Invalid transactions -> routed to Kafka dead-letter topic with error details
- **Output Destinations**:
  - Transactions Iceberg table (partitioned by date)
  - Alerts Iceberg table (linked to transactions)
  - Dead letter Kafka topic (for manual inspection)
  - Operational logs (structured JSON, per service)
- **Dependencies**:
  - Alerting rules configuration file (defines thresholds and rule parameters)
  - Merchant category taxonomy (enum of valid categories)
  - Currency code reference (ISO 4217 valid codes)
- **Data Flow Diagram**:

```
Data Flow:

  [Data Generator] ──> [Kafka: raw-transactions topic]
                                        |
                                        v
                              [Flink Stream Processor]
                              /       |        \
                             /        |         \
                            v         v          v
           [Kafka: alerts  [Iceberg:     [Kafka: dead-letter
            topic]          transactions  topic]
                |           table]        (malformed records)
                v
          [Iceberg:
           alerts table]

  Lineage per record:
  Generator -> Kafka raw-transactions -> Flink -> Iceberg transactions table + (optional) Kafka alerts topic -> Iceberg alerts table
  Generator -> Kafka raw-transactions -> Flink -> Kafka dead-letter topic (if validation fails)
```

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: The entire pipeline environment starts and reaches healthy state within 3 minutes from a single command on a MacBook Air M1
- **SC-002**: The data generator sustains a throughput of at least 10 transactions per second for a continuous 1-hour run without errors
- **SC-003**: End-to-end latency from transaction generation to alert generation is under 5 seconds for 95% of alerting transactions
- **SC-004**: End-to-end latency from transaction generation to Iceberg storage is under 30 seconds for 95% of transactions
- **SC-005**: The alerting system correctly identifies 100% of transactions that match defined rules (no false negatives) with a false positive rate of 0% (deterministic rules, not ML-based)
- **SC-006**: Total memory consumption of all pipeline services combined stays under 6GB during sustained operation at 10 transactions/second
- **SC-007**: The Iceberg lakehouse supports analytical queries over 1 million stored transactions with response times under 10 seconds
- **SC-008**: After a forced service restart, the pipeline resumes processing within 60 seconds with zero data loss (all generated transactions eventually reach storage)
- **SC-009**: 99.9% of generated transactions pass schema validation (measuring generator data quality)
- **SC-010**: The pipeline runs continuously for 24 hours without manual intervention, memory leaks, or service crashes

### Assumptions

- The target machine is a MacBook Air M1 with at least 8GB RAM and 20GB free disk space
- Container runtime compatible with ARM64 is pre-installed and configured
- No external network access is required during pipeline operation (all data is generated locally)
- The learning environment uses synthetic data only; no real financial data is processed
- Default alerting rules are provided; users can customize rules via configuration without code changes
- Data retention is limited by available disk space; no automated archival or deletion is implemented in initial version
- The pipeline targets single-node local execution; distributed deployment is out of scope
