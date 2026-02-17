# Data Dictionary: Streaming Financial Transaction Pipeline

**Date**: 2026-02-16  
**Source**: `spec.md`, `data-model.md`, `contracts/`  
**Owner**: Data Engineering Team  
**Classification**: Confidential (synthetic data, treated as real for security practice)

---

## Overview

This data dictionary provides business definitions, field-level descriptions, and business context for all entities in the streaming financial transaction pipeline. It serves as the authoritative reference for understanding what each data element means, how it is used, and how it relates to other entities.

**Lineage**: Generator → Kafka (`raw-transactions`) → Flink → Iceberg (`transactions`, `alerts`) + Kafka (`alerts`, `dead-letter`)

---

## Entity: Transaction

**Business Purpose**: Represents a single financial operation (purchase, withdrawal, transfer, or refund) initiated by an account holder. Transactions are the primary unit of data flowing through the pipeline.

**Storage**: Kafka topic `raw-transactions` (JSON), Iceberg table `financial.transactions` (Parquet/ZSTD)  
**Partitioning**: `days(timestamp)` — one partition per calendar day  
**Sort Order**: `transaction_id` ascending within each partition  
**Volume**: ~10 records/second (default), ~864,000/day at sustained default rate  
**Retention**: Unlimited (bounded by disk space)

### Fields

| Field | Type | Nullable | Business Definition | Valid Values / Constraints | Example |
|-------|------|----------|---------------------|---------------------------|---------|
| `transaction_id` | string | No | Globally unique identifier for this transaction. Used as the primary key and for deduplication across the pipeline. | UUID v4 format | `550e8400-e29b-41d4-a716-446655440000` |
| `timestamp` | string (ISO 8601) | No | When the transaction occurred in the real world (event time). This is the authoritative time used for all windowed operations, partitioning, and latency measurement. | ISO 8601 with timezone, not future by >1 minute | `2026-02-16T14:30:00.000Z` |
| `account_id` | string | No | Identifier of the financial account that initiated this transaction. Pseudonymous PII — can identify an account holder with external mapping. | Alphanumeric, 10-12 characters | `ACC1234567` |
| `amount` | decimal | No | Monetary value of the transaction in the specified currency. Positive for charges (purchases, withdrawals, transfers), negative for refunds. Zero is invalid. | Non-zero, range: -50,000.00 to 50,000.00 | `1249.99`, `-45.00` |
| `currency` | string | No | ISO 4217 currency code indicating the denomination of the transaction amount. | One of: `USD`, `EUR`, `GBP`, `JPY` | `USD` |
| `merchant_name` | string | No | Name of the merchant or counterparty involved in the transaction. Synthetically generated for realism. | Non-empty, max 100 characters | `Walmart`, `Amazon.com` |
| `merchant_category` | string | No | Classification of the merchant's business type. Used for spend analysis and fraud pattern detection. | One of: `retail`, `dining`, `travel`, `online`, `groceries`, `entertainment`, `utilities` | `retail` |
| `transaction_type` | string | No | The type of financial operation performed. Determines valid amount sign and business rules. | One of: `purchase`, `withdrawal`, `transfer`, `refund` | `purchase` |
| `location_country` | string | No | Country where the transaction physically occurred. Used for geographic anomaly detection. | 2-letter ISO 3166 country code | `US`, `GB`, `JP` |
| `status` | string | No | Current processing state of the transaction. Follows a linear state machine model. | One of: `pending`, `completed`, `failed`, `reversed` | `completed` |
| `processing_timestamp` | timestamp_tz | No* | When the record was processed by the Flink stream processor. Added during enrichment (not present in Kafka source). | ISO 8601, auto-set by Flink | `2026-02-16T14:30:02.500Z` |
| `partition_date` | date | No* | Calendar date extracted from `timestamp` for Iceberg partitioning. Added during enrichment. | Derived from `timestamp` | `2026-02-16` |
| `is_flagged` | boolean | No* | Whether any alerting rule triggered on this transaction. `true` if one or more alerts were generated. | `true` or `false`, default: `false` | `false` |

*\* Enrichment fields — present only in the Iceberg table, not in the Kafka source topic.*

### State Transitions

```
pending ──→ completed ──→ reversed
   │
   └──→ failed
```

- `pending → completed`: Normal successful transaction
- `pending → failed`: Transaction declined or processing error
- `completed → reversed`: Chargeback or manual reversal
- No other transitions are valid

### Distribution Patterns (Default Configuration)

| Dimension | Distribution | Weights |
|-----------|-------------|---------|
| Currency | Weighted random | USD 60%, EUR 20%, GBP 12%, JPY 8% |
| Merchant Category | Weighted random | retail 25%, dining 20%, online 20%, groceries 15%, entertainment 8%, travel 7%, utilities 5% |
| Transaction Type | Weighted random | purchase 85%, withdrawal 8%, refund 5%, transfer 2% |
| Amount | Log-normal | Range $0.50–$50,000; most transactions $10–$500 |
| Fraud Rate | Configurable | Default 3% of transactions are anomalous |

---

## Entity: Alert

**Business Purpose**: Represents a notification generated when a transaction matches one or more configurable alerting rules. Alerts enable real-time investigation of potentially fraudulent or anomalous activity. A single transaction can trigger multiple alerts (one per matching rule).

**Storage**: Kafka topic `alerts` (JSON), Iceberg table `financial.alerts` (Parquet/ZSTD)  
**Partitioning**: `days(alert_timestamp)` — one partition per calendar day  
**Relationship**: Many-to-one with Transaction (multiple alerts can reference the same `transaction_id`)  
**Volume**: ~3% of transaction volume (at default fraud rate), ~26,000/day  
**Latency Target**: Generated within 5 seconds of the triggering transaction (SC-003)

### Fields

| Field | Type | Nullable | Business Definition | Valid Values / Constraints | Example |
|-------|------|----------|---------------------|---------------------------|---------|
| `alert_id` | string | No | Globally unique identifier for this alert. Each alert is independently identifiable even when multiple alerts reference the same transaction. | UUID v4 format | `7c9e6679-7425-40de-944b-e07fc1f90ae7` |
| `transaction_id` | string | No | Foreign key referencing the transaction that triggered this alert. Enables joining alert context with full transaction details. | UUID v4 format, must exist in transactions | `550e8400-e29b-41d4-a716-446655440000` |
| `rule_name` | string | No | Identifier of the alerting rule that triggered. Maps to the `rule_name` field in the alerting rules configuration. | Non-empty, slug format | `high-value-transaction` |
| `severity` | string | No | Severity level of the alert, inherited from the rule configuration. Used for prioritization and routing of investigations. | One of: `low`, `medium`, `high`, `critical` | `high` |
| `alert_timestamp` | timestamp_tz | No | When the alert was generated by the Flink stream processor. Used for partitioning and latency measurement (alert_timestamp - transaction.timestamp = alert latency). | ISO 8601, auto-set by Flink | `2026-02-16T14:30:01.200Z` |
| `description` | string | No | Human-readable explanation of why the alert was generated, including the specific values that triggered the rule. | Non-empty, max 500 characters | `Transaction amount $15,000.00 exceeds threshold of $10,000` |

### Multi-Rule Triggering

A single transaction can match multiple alerting rules simultaneously. For example, a $15,000 transaction at 3:00 AM would trigger both `high-value-transaction` (amount > $10,000) and `unusual-hour` (between 01:00–05:00). Each matching rule produces an independent alert record with its own `alert_id`, both referencing the same `transaction_id`.

---

## Entity: Account (Generator-Internal)

**Business Purpose**: Represents a financial account that initiates transactions. Accounts are used by the data generator to create realistic transaction patterns — the same account generates multiple transactions over time, enabling velocity-based fraud detection (e.g., rapid-activity rule).

**Storage**: Generator memory only — not persisted to Kafka or Iceberg  
**Pool Size**: ~1,000 pre-generated accounts at startup (configurable via `account_pool_size`)  
**Lifecycle**: Created at generator startup, sampled randomly for each generated transaction

### Fields

| Field | Type | Nullable | Business Definition | Valid Values / Constraints | Example |
|-------|------|----------|---------------------|---------------------------|---------|
| `account_id` | string | No | Unique identifier for the financial account. Used as the grouping key for velocity-based alerting rules (rapid-activity). | Alphanumeric, 10-12 characters | `ACC1234567` |
| `account_type` | string | No | Type of financial account, which influences typical transaction patterns and amounts. | One of: `checking`, `savings`, `credit` | `checking` |
| `creation_date` | date | No | When the account was opened. Used for account age calculations in generator logic. | ISO 8601, past date | `2024-06-15` |
| `country` | string | No | Country of the account holder. Influences the `location_country` of generated transactions (most transactions occur in the account's home country). | 2-letter ISO 3166 country code | `US` |

---

## Entity: Alerting Rule (Configuration)

**Business Purpose**: Defines a configurable condition that evaluates every incoming transaction for suspicious or anomalous patterns. Rules are the core business logic of the real-time fraud detection system. They can be added, modified, or disabled without code changes or pipeline restart (FR-016).

**Storage**: YAML configuration file `config/alerting-rules.yaml` — mounted as a Docker volume  
**Hot Reload**: Flink reloads rule configuration on file change  
**Extensibility**: New rules can be added by implementing the `AlertingRule` Java interface and adding configuration

### Fields

| Field | Type | Nullable | Business Definition | Valid Values / Constraints | Example |
|-------|------|----------|---------------------|---------------------------|---------|
| `rule_name` | string | No | Unique identifier for the rule. Used in alert records to trace which rule triggered. Must be a URL-safe slug. | Unique, slug format | `high-value-transaction` |
| `rule_type` | string | No | Category of detection logic. Determines which parameters are expected and how the rule evaluates transactions. | One of: `threshold`, `velocity`, `time_based` | `threshold` |
| `parameters` | map | No | Rule-specific configuration values. Keys and expected values vary by `rule_type`. | Required, rule-type-specific | `{"amount_threshold": "10000"}` |
| `severity` | string | No | Default severity assigned to alerts generated by this rule. Determines investigation priority. | One of: `low`, `medium`, `high`, `critical` | `high` |
| `enabled` | boolean | No | Active/inactive toggle. Disabled rules are loaded but not evaluated, allowing temporary deactivation without removal. | `true` or `false`, default: `true` | `true` |
| `description` | string | Yes | Human-readable explanation of the rule's purpose and business context. | Max 500 characters | `Flags transactions with amount exceeding the configured threshold` |

### Default Rules

| Rule Name | Type | Business Context | Parameters | Severity |
|-----------|------|-----------------|------------|----------|
| `high-value-transaction` | `threshold` | Detects unusually large transactions that may indicate fraud, money laundering, or unauthorized access. Financial regulations often require reporting transactions above certain thresholds. | `amount_threshold: 10000` (USD equivalent) | `high` |
| `rapid-activity` | `velocity` | Detects burst activity from a single account that may indicate automated fraud, credential compromise, or card testing attacks. Legitimate users rarely make 5+ transactions within a 1-minute window. | `max_count: 5`, `window_minutes: 1` | `medium` |
| `unusual-hour` | `time_based` | Detects transactions during atypical hours (01:00–05:00 UTC) when legitimate activity is statistically rare. Fraudulent activity often occurs during off-hours when victims are asleep and less likely to notice. | `quiet_start: 01:00`, `quiet_end: 05:00` | `low` |

### Rule Type Parameter Reference

| Rule Type | Required Parameters | Description |
|-----------|-------------------|-------------|
| `threshold` | `amount_threshold` (numeric) | Triggers when `transaction.amount` exceeds the threshold value |
| `velocity` | `max_count` (integer), `window_minutes` (integer) | Triggers when the same `account_id` produces `max_count` or more transactions within a `window_minutes` event-time window |
| `time_based` | `quiet_start` (HH:MM), `quiet_end` (HH:MM) | Triggers when `transaction.timestamp` falls within the quiet period (UTC) |

---

## Entity: Dead Letter Record

**Business Purpose**: Captures transactions that failed schema validation in the Flink stream processor. Dead letter records preserve the original data along with diagnostic information for debugging and data quality monitoring. They are routed to a separate Kafka topic to avoid blocking valid transaction processing.

**Storage**: Kafka topic `dead-letter` (JSON)  
**Volume**: <0.1% of transaction volume for generator-produced data (99.9% validation pass rate target, SC-009)

### Fields

| Field | Type | Nullable | Business Definition | Valid Values / Constraints | Example |
|-------|------|----------|---------------------|---------------------------|---------|
| `original_record` | string | No | Raw JSON string of the failed transaction, preserved exactly as received for debugging. | Valid JSON string | `{"transaction_id":"abc","amount":0}` |
| `error_field` | string | No | Name of the field that caused the validation failure. Enables targeted debugging. | Field name from Transaction schema | `amount` |
| `error_type` | string | No | Category of validation failure. Helps classify and aggregate quality issues. | One of: `missing`, `invalid_format`, `out_of_range` | `out_of_range` |
| `expected_format` | string | No | Description of what was expected for the failed field. Provides actionable guidance for fixing the source. | Human-readable format description | `non-zero decimal` |
| `actual_value` | string | Yes | The actual value that failed validation. May be `null` if the field was missing entirely. | String representation of the value | `0`, `null` |
| `error_timestamp` | string | No | When the validation error was detected by Flink. Used for monitoring error rate trends. | ISO 8601 | `2026-02-16T14:30:00.500Z` |
| `processor_id` | string | No | Identifier of the Flink subtask that detected the error. Useful for debugging processing-level issues. | Flink subtask identifier | `TransactionValidator-0` |

---

## Entity: Pipeline Session Metadata

**Business Purpose**: Technical metadata captured per pipeline run to support operational monitoring, performance analysis, and compliance with Constitution v2.0.0 Principle IV (Metadata & Lineage). Each row represents one execution session of the pipeline.

**Storage**: Iceberg table `financial.pipeline_session_metadata` (Parquet/ZSTD)  
**Partitioning**: None (low volume — one row per session)

### Fields

| Field | Type | Nullable | Business Definition | Valid Values / Constraints | Example |
|-------|------|----------|---------------------|---------------------------|---------|
| `session_id` | string | No | Unique identifier for this pipeline execution session. | UUID v4 format | `a1b2c3d4-e5f6-7890-abcd-ef1234567890` |
| `start_timestamp` | timestamp_tz | No | When the pipeline session began. | ISO 8601 | `2026-02-16T10:00:00.000Z` |
| `end_timestamp` | timestamp_tz | Yes | When the pipeline session ended. `null` if still running. | ISO 8601 or null | `2026-02-16T11:00:00.000Z` |
| `records_generated` | long | No | Total records produced by the data generator during this session. | Non-negative integer | `36000` |
| `records_published` | long | No | Total records successfully sent to Kafka during this session. | Non-negative integer | `36000` |
| `records_processed` | long | No | Total records processed by the Flink stream processor. | Non-negative integer | `35998` |
| `records_stored` | long | No | Total records written to the Iceberg transactions table. | Non-negative integer | `35996` |
| `alerts_generated` | long | No | Total alerts created during this session. | Non-negative integer | `1080` |
| `records_dead_lettered` | long | No | Total records routed to the dead letter queue. | Non-negative integer | `2` |
| `throughput_avg` | double | No | Average records per second over the session duration. | Positive decimal | `10.0` |
| `latency_p50_ms` | long | No | 50th percentile end-to-end latency (generation to Iceberg storage) in milliseconds. | Non-negative integer | `1200` |
| `latency_p95_ms` | long | No | 95th percentile end-to-end latency in milliseconds. | Non-negative integer | `3500` |
| `latency_p99_ms` | long | No | 99th percentile end-to-end latency in milliseconds. | Non-negative integer | `4800` |

---

## Merchant Category Taxonomy

The `merchant_category` field classifies merchants into one of seven categories. This taxonomy is used for spend pattern analysis and is embedded in the Transaction schema as a closed enum.

| Category | Description | Typical Merchants | Default Weight |
|----------|-------------|-------------------|---------------|
| `retail` | Physical retail stores and department stores | Walmart, Target, Best Buy | 25% |
| `dining` | Restaurants, cafes, and food service | McDonald's, Starbucks, local restaurants | 20% |
| `online` | E-commerce and digital services | Amazon, eBay, Netflix | 20% |
| `groceries` | Supermarkets and food stores | Whole Foods, Kroger, Trader Joe's | 15% |
| `entertainment` | Entertainment venues and media | Movie theaters, Spotify, gaming | 8% |
| `travel` | Airlines, hotels, car rentals, transit | Delta, Marriott, Uber | 7% |
| `utilities` | Utility bills and recurring services | Electric company, water, internet | 5% |

---

## Cross-Entity Relationships

```
Account (generator-internal)
    │
    │ 1:* (one account, many transactions)
    ▼
Transaction (Kafka + Iceberg)
    │
    │ 1:* (one transaction, zero or more alerts)
    ▼
Alert (Kafka + Iceberg)
    │
    │ *:1 (many alerts reference one rule)
    ▼
Alerting Rule (configuration)
```

### Key Relationships

| Relationship | Cardinality | Join Key | Business Meaning |
|-------------|-------------|----------|-----------------|
| Account → Transaction | 1:many | `account_id` | An account generates multiple transactions over time |
| Transaction → Alert | 1:many | `transaction_id` | A transaction can trigger zero, one, or multiple alerts |
| Alert → Alerting Rule | many:1 | `rule_name` | Multiple alerts can be generated by the same rule |
| Transaction → Dead Letter | 1:1 | embedded in `original_record` | A failed transaction produces exactly one dead letter record |

---

## Data Quality SLAs

| Metric | Target | Measurement |
|--------|--------|-------------|
| Generator validation pass rate | ≥99.9% | Valid records / total generated records (SC-009) |
| End-to-end latency (generation → storage) | <30s for 95% of records | `processing_timestamp - timestamp` (SC-004) |
| Alert latency (generation → alert) | <5s for 95% of alerting transactions | `alert_timestamp - transaction.timestamp` (SC-003) |
| Completeness | 100% of valid transactions stored | Records stored / records processed (SC-004) |
| Uniqueness | 0 duplicate `transaction_id` in Iceberg | COUNT vs COUNT DISTINCT on `transaction_id` |
