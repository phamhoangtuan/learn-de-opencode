# Quickstart: Streaming Financial Transaction Pipeline

**Date**: 2026-02-16  
**Branch**: `001-streaming-financial-pipeline`

## Prerequisites

| Requirement | Minimum Version | Check Command |
|-------------|----------------|---------------|
| Docker Desktop | 4.x (with ARM64 support) | `docker --version` |
| Docker Compose | v2.x (bundled with Docker Desktop) | `docker compose version` |
| Free RAM | 6 GB available for containers | `sysctl hw.memsize` |
| Free Disk | 20 GB for images + data | `df -h .` |
| Git | 2.x | `git --version` |

Optional (for analytics queries):

| Requirement | Minimum Version | Install |
|-------------|----------------|---------|
| Python | 3.11+ | `brew install python@3.11` |
| DuckDB | 1.x | `pip install duckdb` |
| PyIceberg | 0.7+ | `pip install pyiceberg[duckdb]` |

## Quick Start

### 1. Clone and checkout

```bash
git clone <repo-url> learn_de
cd learn_de
git checkout 001-streaming-financial-pipeline
```

### 2. Start the pipeline

```bash
docker compose up -d
```

This starts all services with proper dependency ordering:

1. **Kafka** (KRaft mode) — message broker
2. **Iceberg REST Catalog** — table catalog with SQLite backend
3. **Flink JobManager** — orchestrates stream processing
4. **Flink TaskManager** — executes stream processing
5. **Data Generator** — produces synthetic transactions

All services should reach healthy state within **3 minutes**.

### 3. Verify services are running

```bash
# Check all containers are healthy
docker compose ps

# Check health via CLI script
./scripts/health-check.sh
```

### 4. Watch transactions flowing

```bash
# Read raw transactions from Kafka
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic raw-transactions \
  --from-beginning \
  --max-messages 5

# Read alerts
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic alerts \
  --from-beginning \
  --max-messages 5

# Check dead letter queue
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic dead-letter \
  --from-beginning \
  --max-messages 5
```

### 5. Query stored data (optional, requires Python)

```bash
# Install analytics dependencies
pip install pyiceberg[duckdb] duckdb

# Run sample query
python scripts/query.py --table transactions --limit 10
python scripts/query.py --table alerts --limit 10
```

### 6. Stop the pipeline

```bash
# Graceful shutdown (preserves data)
docker compose down

# Full cleanup (removes data volumes)
docker compose down -v
```

## Configuration

### Generator Rate

Adjust the transaction generation rate without restarting:

```bash
# Edit the generator config (mounted as a Docker volume)
# Change `rate` value (default: 10, range: 1-1000)
vim config/generator.yaml
```

The generator polls for config changes every 5 seconds.

### Alerting Rules

Edit alerting rules without pipeline restart:

```bash
vim config/alerting-rules.yaml
```

Default rules:

| Rule | Type | Trigger | Severity |
|------|------|---------|----------|
| `high-value-transaction` | threshold | amount > $10,000 | high |
| `rapid-activity` | velocity | 5+ tx from same account in 1 min | medium |
| `unusual-hour` | time_based | tx between 01:00-05:00 UTC | low |

## Service Ports

| Service | Port | Purpose |
|---------|------|---------|
| Kafka | 9092 | Broker (internal) |
| Kafka | 29092 | Broker (host access) |
| Flink Web UI | 8081 | Job monitoring dashboard |
| Iceberg REST | 8181 | Catalog REST API |

## Memory Budget

| Service | Allocation |
|---------|-----------|
| Kafka (KRaft) | 500 MB |
| Flink JobManager | 768 MB |
| Flink TaskManager | 1536 MB |
| Iceberg REST Catalog | 256 MB |
| Data Generator | 256 MB |
| **Total** | **~3.4 GB** |

Remaining ~2.6 GB for Docker Desktop overhead, OS cache, and on-demand DuckDB queries.

## Troubleshooting

### Services fail to start

```bash
# Check logs for a specific service
docker compose logs <service-name>

# Common fix: not enough memory
# Close other applications and retry
docker compose down && docker compose up -d
```

### Kafka broker not ready

Kafka in KRaft mode takes ~30-60 seconds to initialize. Other services have health check dependencies and will wait.

```bash
# Check Kafka status
docker compose logs kafka | tail -20
```

### Flink job not processing

```bash
# Check Flink Web UI at http://localhost:8081
# Look for failed/restarting jobs

# Check TaskManager logs
docker compose logs flink-taskmanager | tail -50
```

### Data not appearing in Iceberg

Flink commits to Iceberg at checkpoint intervals (every 5-10 minutes). Wait for at least one checkpoint cycle.

```bash
# Check Flink checkpoint status via Web UI
# or check Iceberg catalog
curl http://localhost:8181/v1/namespaces
```

### High memory usage

```bash
# Check per-container memory
docker stats --no-stream

# If over budget, reduce generator rate
# Edit config/generator.yaml: rate: 5
```

## Project Layout

```
learn_de/
├── docker-compose.yml           # Service definitions
├── config/
│   ├── generator.yaml           # Generator configuration (rate, patterns)
│   └── alerting-rules.yaml      # Flink alerting rule definitions
├── generator/
│   ├── Dockerfile               # Python 3.11-slim based
│   ├── requirements.txt         # confluent-kafka, faker, pyyaml
│   └── src/                     # Generator source code
├── flink-jobs/
│   ├── pom.xml                  # Maven build for Flink jobs
│   └── src/                     # Java Flink DataStream jobs
├── scripts/
│   ├── health-check.sh          # CLI health monitoring
│   └── query.py                 # DuckDB/PyIceberg analytics
├── tests/
│   ├── unit/                    # Unit tests
│   ├── integration/             # Docker-based integration tests
│   └── contract/                # Schema contract tests
└── specs/
    └── 001-streaming-financial-pipeline/
        ├── spec.md              # Feature specification
        ├── plan.md              # Implementation plan
        ├── research.md          # Research findings
        ├── data-model.md        # Data model definitions
        ├── quickstart.md        # This file
        └── contracts/           # JSON Schema contracts
```
