# Performance Baseline: Streaming Financial Transaction Pipeline

**Date**: 2026-02-16  
**Scope**: Expected performance targets and measurement methodology  
**Environment**: MacBook Air M1 (8-16GB RAM), Docker Compose, single-node

---

## Overview

This document defines expected performance baselines for the pipeline, measurement methods for each metric, and acceptance thresholds. These baselines correspond to Success Criteria SC-001 through SC-010 in `spec.md`.

---

## Throughput Baselines

### Transaction Generation Rate

| Metric | Target | Measurement Method |
|--------|--------|--------------------|
| Default throughput | 10 transactions/second | Generator structured logs: `records_per_second` field |
| Sustained throughput (1 hour) | 10 tx/sec ± 10% (36,000 ± 3,600 records) | `records_generated` from session metadata / elapsed seconds |
| Sustained throughput (24 hours) | ~864,000 transactions/day | Pipeline session metadata `records_generated` field |
| Max configurable rate | Up to 1,000 tx/sec | Modify `config/generator.yaml` rate field |

**How to measure**:
```bash
# Check generator throughput via logs
docker compose logs generator | grep "records_per_second"

# Check session metadata via query script
python scripts/query.py --table pipeline_session_metadata --limit 1
```

### Flink Processing Rate

| Metric | Target | Measurement Method |
|--------|--------|--------------------|
| Processing throughput | ≥10 tx/sec (match generator) | Flink Web UI: Records In/Out per second |
| Kafka consumer lag | <100 records sustained | `scripts/health-check.sh` or Kafka consumer group describe |
| Alert generation rate | ~3% of transaction rate (at default fraud rate) | Alerts topic consumer count |

**How to measure**:
```bash
# Flink Web UI
open http://localhost:8081

# Kafka consumer lag
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group flink-transaction-pipeline
```

---

## Latency Baselines

### End-to-End Latency (Generation → Iceberg Storage)

| Percentile | Target | Description |
|-----------|--------|-------------|
| p50 | <5 seconds | Median transaction latency |
| p95 | <30 seconds | 95th percentile (SC-004) |
| p99 | <60 seconds | 99th percentile |

**How to measure**:
- Pipeline session metadata: `latency_p50_ms`, `latency_p95_ms`, `latency_p99_ms`
- Calculated as: `processing_timestamp - timestamp` for each transaction
- Note: Iceberg commits happen at checkpoint intervals (every few minutes), so actual "visible in Iceberg" latency is higher than processing latency

### Alert Generation Latency (Generation → Alert)

| Percentile | Target | Description |
|-----------|--------|-------------|
| p50 | <2 seconds | Median alert latency |
| p95 | <5 seconds | 95th percentile (SC-003) |
| p99 | <10 seconds | 99th percentile |

**How to measure**:
- Calculated as: `alert_timestamp - transaction.timestamp` for each alert
- Query via DuckDB:
```sql
SELECT
  percentile_disc(0.50) WITHIN GROUP (ORDER BY
    epoch_ms(alert_timestamp) - epoch_ms(t.timestamp)
  ) as p50_ms,
  percentile_disc(0.95) WITHIN GROUP (ORDER BY
    epoch_ms(alert_timestamp) - epoch_ms(t.timestamp)
  ) as p95_ms
FROM alerts a
JOIN transactions t ON a.transaction_id = t.transaction_id
```

---

## Resource Baselines

### Memory Consumption

| Service | Allocation | Expected Usage | Measurement |
|---------|-----------|----------------|-------------|
| Kafka (KRaft) | 512 MB | 300-450 MB | `docker stats kafka` |
| Flink JobManager | 768 MB | 400-600 MB | `docker stats flink-jobmanager` |
| Flink TaskManager | 1,536 MB | 800-1,200 MB | `docker stats flink-taskmanager` |
| Iceberg REST Catalog | 256 MB | 100-200 MB | `docker stats iceberg-rest` |
| Data Generator | 256 MB | 80-150 MB | `docker stats generator` |
| **Total Pipeline** | **~3.4 GB** | **~2.0-2.6 GB** | `docker stats --no-stream` |
| **Total with Docker overhead** | **<6 GB** | **~3.5-4.5 GB** | System Activity Monitor |

**How to measure**:
```bash
# Per-container memory
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}"

# Total system memory (macOS)
sysctl hw.memsize
vm_stat
```

### Storage Consumption

| Data Type | Expected Size | Growth Rate | Measurement |
|-----------|--------------|-------------|-------------|
| Iceberg transactions | ~500 MB - 1 GB / day | ~0.5-1 KB per record (Parquet/ZSTD) | `du -sh` on warehouse volume |
| Iceberg alerts | ~15-30 MB / day | ~3% of transaction volume | `du -sh` on warehouse volume |
| Kafka logs | ~200-500 MB / day | Retained per topic config | `docker compose exec kafka du -sh /tmp/kafka-logs` |
| Flink checkpoints | ~50-100 MB | Grows slowly, bounded | `du -sh` on checkpoint volume |
| Docker images | ~2-3 GB (one-time) | Static | `docker system df` |

---

## Startup & Shutdown Baselines

### Startup Performance (SC-001)

| Phase | Target Time | Measurement |
|-------|------------|-------------|
| Docker Compose up | <30 seconds | `time docker compose up -d` |
| Kafka ready | <60 seconds | Healthcheck passes |
| Iceberg REST ready | <30 seconds | Healthcheck passes (curl /v1/namespaces) |
| Flink cluster ready | <60 seconds | Healthcheck passes (curl /overview) |
| All services healthy | <3 minutes | `docker compose ps` shows all healthy |
| First transaction processed | <4 minutes | Check raw-transactions topic |

**How to measure**:
```bash
# Time full startup
time docker compose up -d

# Monitor until all healthy
watch -n 5 'docker compose ps'

# Or use health-check script
./scripts/health-check.sh
```

### Shutdown Performance

| Phase | Target Time | Measurement |
|-------|------------|-------------|
| Graceful shutdown (all services) | <1 minute | `time docker compose down` |
| Generator flush (Kafka buffer drain) | <10 seconds | Generator logs: "shutdown complete" |
| Flink savepoint | <30 seconds | Flink logs: savepoint completed |

---

## Query Performance (SC-007)

### DuckDB/PyIceberg Analytical Queries

| Query Type | Target | Record Count | Measurement |
|-----------|--------|-------------|-------------|
| Simple scan (SELECT * LIMIT N) | <2 seconds | Up to 1M records | `scripts/query.py` execution time log |
| Date range filter | <5 seconds | Up to 1M records | `scripts/query.py --date-range` |
| Account filter | <3 seconds | Up to 1M records | `scripts/query.py --account-id` |
| Alert-transaction join | <10 seconds | Up to 1M records | `scripts/query.py --join-alerts` |
| Time-travel snapshot | <10 seconds | Up to 1M records | `scripts/query.py --as-of-timestamp` |

**How to measure**:
```bash
# Query with timing
time python scripts/query.py --table transactions --limit 100

# Check query execution logs (structured JSON)
python scripts/query.py --table transactions --date-range 2026-02-16 2026-02-17
# Logs include: query_type, execution_time_ms, row_count
```

---

## Data Quality Baselines (SC-005, SC-009)

| Metric | Target | Measurement |
|--------|--------|-------------|
| Generator validation pass rate | ≥99.9% | `records_dead_lettered / records_generated` from session metadata |
| Alert precision (true positives) | 100% | Deterministic rules = no false positives |
| Alert recall (detection rate) | 100% | All rule-matching transactions produce alerts (SC-005) |
| Dead letter queue depth | <0.1% of volume | `dead-letter` topic consumer count |

---

## Recovery Baselines (SC-008)

| Scenario | Target | Measurement |
|----------|--------|-------------|
| Service restart to healthy | <60 seconds | `time docker compose restart <service>` + healthcheck |
| Flink checkpoint recovery | <30 seconds | Flink Web UI: job restart time |
| Data loss after restart | 0 records lost | Compare pre/post restart record counts |
| Kafka offset recovery | Automatic | Consumer group offsets persisted |

**How to measure**:
```bash
# Force restart a service
docker compose restart flink-taskmanager

# Monitor recovery
watch -n 2 'docker compose ps'

# Verify no data loss
python scripts/query.py --table transactions --limit 1  # Check latest records
```

---

## 24-Hour Stability Baseline (SC-010)

| Metric | Target | Measurement |
|--------|--------|-------------|
| Uptime | 100% (no service crashes) | `docker compose ps` shows all healthy |
| Memory growth | <10% increase over 24 hours | Periodic `docker stats` samples |
| Transaction count | ~864,000 ± 5% | Session metadata `records_generated` |
| Alert count | ~26,000 ± 20% (at 3% fraud rate) | Session metadata `alerts_generated` |
| Error rate | <0.1% | `records_dead_lettered / records_generated` |
| Disk usage | <1.5 GB new data | `du -sh` on volumes |

**How to run 24-hour stability test**:
```bash
# Start pipeline
docker compose up -d

# Monitor periodically (cron or manual)
./scripts/health-check.sh
docker stats --no-stream

# After 24 hours, collect metrics
python scripts/query.py --table pipeline_session_metadata --limit 1
docker stats --no-stream
```

---

## Baseline Measurement Checklist

Use this checklist when running a baseline measurement session:

- [ ] All services healthy (`docker compose ps`)
- [ ] Generator producing at expected rate (check logs)
- [ ] Kafka consumer lag stable and low (<100)
- [ ] Flink job running without restarts (Web UI)
- [ ] Memory within budget (`docker stats`)
- [ ] No errors in service logs (`docker compose logs --tail 100`)
- [ ] Query response times within targets
- [ ] Alert latency within targets
- [ ] Dead letter queue depth acceptable
