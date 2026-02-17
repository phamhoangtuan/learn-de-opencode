# Streaming Financial Transaction Pipeline

A real-time streaming pipeline that generates synthetic financial transactions, detects fraud/anomalies via configurable rules, and stores everything in an Iceberg lakehouse for historical analysis. Runs locally on Docker Compose (MacBook Air M1 compatible, ~3.4 GB RAM).

## Architecture

```
[Data Generator] ──> [Kafka] ──> [Flink Stream Processor] ──> [Iceberg Lakehouse]
    (Python)        (KRaft)         (Java, 3 rules)           (Parquet/ZSTD)
                                         │
                                         ├──> Kafka alerts topic ──> Iceberg alerts table
                                         └──> Kafka dead-letter topic (invalid records)
```

**Stack**: Apache Kafka 3.9 | Apache Flink 1.20 | Apache Iceberg | DuckDB + PyIceberg | Docker Compose

## Quick Start

```bash
# Start everything
docker compose up -d

# Check services are healthy (~3 min)
docker compose ps

# Watch transactions flowing
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic raw-transactions --max-messages 5

# Watch alerts
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic alerts --max-messages 5

# Query stored data (requires: pip install pyiceberg[duckdb] duckdb)
python scripts/query.py --table transactions --limit 10
python scripts/query.py --table alerts --limit 10

# Stop (preserves data)
docker compose down

# Stop and remove all data
docker compose down -v
```

## Alerting Rules

Configured in `config/alerting-rules.yaml` (hot-reloaded, no restart needed):

| Rule | Trigger | Severity |
|------|---------|----------|
| High-value transaction | Amount > $10,000 | High |
| Rapid activity | 5+ tx from same account in 1 min | Medium |
| Unusual hour | Transaction between 01:00-05:00 UTC | Low |

## Configuration

| File | Purpose | Hot Reload |
|------|---------|------------|
| `config/generator.yaml` | Generation rate, fraud %, account pool | Yes (5s poll) |
| `config/alerting-rules.yaml` | Alert rule thresholds and toggles | Yes |

## Services & Ports

| Service | Port | Memory |
|---------|------|--------|
| Kafka (KRaft) | 9092, 29092 | 512 MB |
| Flink JobManager | [localhost:8081](http://localhost:8081) | 768 MB |
| Flink TaskManager | — | 1536 MB |
| Iceberg REST Catalog | 8181 | 256 MB |
| Data Generator | — | 256 MB |

## Analytics Queries

```bash
# Date range
python scripts/query.py --table transactions --date-range 2026-02-16 2026-02-17

# Specific account
python scripts/query.py --table transactions --account-id ACC1234567

# Join alerts with transactions
python scripts/query.py --join-alerts

# Time-travel query
python scripts/query.py --table transactions --as-of-timestamp "2026-02-16T12:00:00"

# Table maintenance
python scripts/compact.py          # Compaction + snapshot expiry
python scripts/schema_evolution.py # Add columns without rewrite
```

## Tests

```bash
# Python (144 tests, 85.56% coverage)
pytest

# Java (106 tests)
./mvnw test -f flink-jobs/pom.xml
```

## Project Structure

```
docker-compose.yml              # All services
config/
  generator.yaml                # Generator settings
  alerting-rules.yaml           # Flink alerting rules
generator/src/                  # Python transaction generator
flink-jobs/src/                 # Java Flink stream processor
scripts/
  query.py                      # DuckDB/PyIceberg analytics CLI
  health-check.sh               # Service health monitor
  compact.py                    # Iceberg table maintenance
  schema_evolution.py           # Schema evolution demo
tests/                          # Unit, integration, contract tests
specs/001-streaming-financial-pipeline/
  spec.md                       # Feature specification
  data-model.md                 # Entity schemas
  data-dictionary.md            # Business definitions
  quickstart.md                 # Detailed setup guide
```
