# Research: Streaming Financial Transaction Pipeline

**Date**: 2026-02-16  
**Branch**: `001-streaming-financial-pipeline`

## R1. Stream Processing Engine: Java Flink (not PyFlink)

**Decision**: Use Java Flink (DataStream API) for the stream processing component.

**Rationale**:
- PyFlink adds ~300-500MB per TaskManager slot for Python worker processes when using Python UDFs, which is prohibitive within the 6GB total memory budget
- The Iceberg Flink connector (`iceberg-flink-runtime`) is a Java-native JAR; PyFlink can only access it through the Table/SQL API, not the full DataStream API
- Alerting rules are pure business logic (threshold checks, windowed aggregations, time comparisons) — no need for Python data science libraries
- Java DataStream API's `KeyedProcessFunction`, `ValueState`, and event-time timers are idiomatic for implementing stateful alerting rules

**Alternatives considered**:
- PyFlink with SQL/Table API only — near-zero overhead if no Python UDFs, but limits expressiveness for complex windowed rules and state management
- PyFlink with Python UDFs — full Python flexibility but 300-500MB additional memory per slot

**Key version requirements**:
- Flink 1.20.x (latest stable, ARM64 Docker images available as `flink:1.20-java17`)
- Iceberg Flink connector: `iceberg-flink-runtime-1.20-1.7.0.jar`
- Iceberg format version 2 (required for row-level deletes and upsert support)

**Memory allocation**:
- Flink JobManager: 768MB (`jobmanager.memory.process.size: 768m`)
- Flink TaskManager: 1536MB (`taskmanager.memory.process.size: 1536m`)
- Total Flink: ~2.3GB
- State backend: HashMapStateBackend (no RocksDB needed at 10 tx/sec)

---

## R2. Message Broker: Apache Kafka in KRaft Mode

**Decision**: Use `apache/kafka:3.9.0` in KRaft combined mode (no ZooKeeper).

**Rationale**:
- Official Apache image with native ARM64 multi-arch support
- KRaft combined mode (controller + broker in one JVM) eliminates ZooKeeper, saving ~150-256MB RAM
- Kafka 3.9 is the last 3.x LTS with broadest community documentation and stable KRaft
- At 10 msg/sec across 3 topics, this is negligible load for any Kafka configuration

**Alternatives considered**:
- `bitnami/kafka` — good env-var config but uncertain maintenance under Broadcom
- `confluentinc/cp-kafka` — heavier (~900MB image), only needed for Confluent Schema Registry
- `apache/kafka-native` (GraalVM) — smallest footprint (~200MB) but experimental (KIP-974)
- `apache/kafka:4.1.0` — viable but 3.9 has broader troubleshooting docs

**Recommended memory allocation**:
- `KAFKA_HEAP_OPTS: "-Xmx256m -Xms256m"` with Docker `mem_limit: 512m`
- Total actual RSS: ~400-500MB

**Serialization**: Plain JSON, no Schema Registry.
- Schema Registry (`cp-schema-registry`) adds ~300-400MB RAM and pins to Confluent images
- At 10 msg/sec, wire-size difference between Avro and JSON is irrelevant
- Schema enforcement at the application layer (Pydantic models, JSON Schema validation in producer)
- `kafka-console-consumer` can read plain JSON for debugging

**Topic configuration**:
- `raw-transactions` — 1 partition, replication factor 1 (single node)
- `alerts` — 1 partition, replication factor 1
- `dead-letter` — 1 partition, replication factor 1

---

## R3. Storage: Apache Iceberg with REST Catalog

### Catalog

**Decision**: Use `tabulario/iceberg-rest` (Apache Iceberg REST catalog image) with SQLite backend.

**Rationale**:
- Implements the official Iceberg REST Catalog spec (industry standard)
- Single lightweight container (~256MB), ARM64 supported
- Multi-engine compatible: Flink, DuckDB/PyIceberg, Spark all connect natively
- Decouples catalog from compute — swap query engines without reconfiguring

**Alternatives considered**:
- Hadoop catalog — zero dependencies but no concurrent writer safety
- JDBC catalog (PostgreSQL) — viable but REST catalog is lighter and more portable
- Nessie — Git-like branching, over-engineered for local dev

### Query Engine

**Decision**: DuckDB + PyIceberg for analytics queries (no extra container).

**Rationale**:
- DuckDB is an in-process analytical engine: ~50-200MB memory, no JVM, no container
- PyIceberg reads the REST catalog, resolves snapshots/manifests, exposes tables as Arrow
- Time-travel queries supported: PyIceberg loads table at specific snapshot ID/timestamp
- ARM64 native wheels for both libraries

**Alternatives considered**:
- Trino — 1-2GB JVM overhead, unjustified at this data volume
- Spark — heaviest option (2-4GB), slow startup
- DuckDB iceberg extension — reads only, limited support

### Flink-Iceberg Connector

**Configuration**:
- Connector JAR: `iceberg-flink-runtime-1.20-1.7.0.jar`
- Format version: 2 (required for upserts, row-level deletes)
- Write format: Parquet with ZSTD compression
- Target file size: 128MB
- Checkpoint interval: 5-10 minutes (at 10 tx/sec, ~3000-6000 records per commit)

### Partitioning

**Decision**: `days(event_timestamp)` — daily partitioning for both tables.

**Rationale**: At ~864K records/day (~500MB-1GB), daily partitioning produces manageable file counts. Hourly would create 24 partitions/day with tiny files.

### Compaction Strategy

**Decision**: Daily compaction job + snapshot expiration.

**Approach**:
- Run `rewrite_data_files` daily (batch job or Python script)
- Expire snapshots older than 3 days, keep minimum 10 snapshots
- Run `remove_orphan_files` weekly
- At this volume (~1GB/day), weekly compaction would also be acceptable

---

## R4. Data Generator: Python with confluent-kafka

### Kafka Client

**Decision**: `confluent-kafka` Python library.

**Rationale**:
- C-backed (`librdkafka`) — 5-10x faster than `kafka-python`
- Active maintenance, pre-built ARM64 wheels since v2.x
- Native idempotent producer, delivery callbacks, internal batching
- `kafka-python` has stalled (last release 2022)

**Alternatives considered**:
- `kafka-python` — pure Python, no C deps, but maintenance stalled
- `aiokafka` — native asyncio, unnecessary complexity for single-threaded producer

### Rate Limiting

**Decision**: Token bucket algorithm with `time.monotonic()`.

**Approach**: Accumulate tokens at configured rate (e.g., 10/sec). Each message consumes one token. Sleep when bucket is empty. Handles bursts naturally, self-correcting, no drift.

### Dynamic Rate Adjustment

**Decision**: Config file watch with periodic reload (every 5 seconds).

**Approach**: Monitor a YAML config file mounted via Docker volume. When `rate` value changes, update the token bucket's refill rate. No restart required. Alternative: lightweight HTTP endpoint on a background thread.

### Data Generation

**Decision**: Faker for base data + custom domain-specific generators.

**Approach**:
- **Faker**: UUIDs, timestamps, country codes
- **Custom generators**: 
  - Transaction amounts: log-normal distribution tuned per merchant category
  - Merchant categories: weighted random selection (realistic frequency)
  - Transaction types: weighted (purchase 85%, refund 5%, withdrawal 8%, transfer 2%)
  - Account IDs: sample from pre-generated pool of ~1000 accounts
  - Fraud patterns: inject configurable percentage (1-5%) of anomalous transactions
- **Pre-generate static pools** at startup for accounts, merchants, locations

### Retry Strategy

**Decision**: Two-level retry with circuit breaker.

- **Level 1**: librdkafka internal retries (`message.send.max.retries=10`, `retry.backoff.ms=500`, `enable.idempotence=true`)
- **Level 2**: Application circuit breaker — after 10 consecutive delivery failures, pause generation, buffer in `collections.deque` (max 10,000 messages), exponential backoff 1s → 2s → 4s → ... capped at 60s, drain buffer on reconnection

### Architecture Note

Single-threaded producer design. `confluent-kafka` has its own internal I/O thread. Main thread: generate → serialize → `produce()` → `poll()`. Token bucket paces the main loop. Simple, debuggable, handles 1000 tx/sec with minimal resources.

---

## R5. Memory Budget Summary

| Service | Memory Allocation | Notes |
|---------|------------------|-------|
| Kafka (KRaft) | 500MB | 256MB heap + OS overhead |
| Flink JobManager | 768MB | |
| Flink TaskManager | 1536MB | HashMapStateBackend |
| Iceberg REST Catalog | 256MB | SQLite backend |
| Data Generator (Python) | 256MB | Single-threaded, in-memory pools |
| Health CLI Script | 50MB | On-demand, not always running |
| **Total** | **~3.4GB** | Well within 6GB budget |

Remaining headroom: ~2.6GB for Docker Desktop overhead, OS page cache (benefits Kafka and Iceberg reads), and DuckDB/PyIceberg analytics queries on demand.

---

## R6. Docker Image Summary

| Service | Image | Tag |
|---------|-------|-----|
| Kafka | `apache/kafka` | `3.9.0` |
| Flink JobManager | `flink` | `1.20-java17` |
| Flink TaskManager | `flink` | `1.20-java17` |
| Iceberg REST Catalog | `tabulario/iceberg-rest` | `latest` |
| Data Generator | `python` (custom Dockerfile) | `3.11-slim` |
