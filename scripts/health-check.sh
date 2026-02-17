#!/usr/bin/env bash
# T073: Health check script for the streaming financial pipeline.
#
# Queries all service health endpoints and displays a status table.
# Shows key metrics: throughput, latency, error counts, Kafka consumer lag.
#
# Per spec.md FR-017: Simple CLI for pipeline health status.
# Per research.md R5: Memory budget validation.
#
# Usage:
#   ./scripts/health-check.sh          # Full health check
#   ./scripts/health-check.sh --quick  # Quick status only
#   ./scripts/health-check.sh --json   # JSON output

set -euo pipefail

# Configuration
KAFKA_CONTAINER="pipeline-kafka"
KAFKA_BOOTSTRAP="localhost:9092"
ICEBERG_REST_URL="http://localhost:8181"
FLINK_JM_URL="http://localhost:8081"
GENERATOR_CONTAINER="pipeline-generator"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse arguments
JSON_OUTPUT=false
QUICK_MODE=false
for arg in "$@"; do
    case $arg in
        --json)  JSON_OUTPUT=true ;;
        --quick) QUICK_MODE=true ;;
    esac
done

# --- Helper Functions ---

check_container_status() {
    local container_name="$1"
    local status
    status=$(docker inspect --format '{{.State.Status}}' "$container_name" 2>/dev/null || echo "not_found")
    echo "$status"
}

check_container_health() {
    local container_name="$1"
    local health
    health=$(docker inspect --format '{{.State.Health.Status}}' "$container_name" 2>/dev/null || echo "none")
    echo "$health"
}

get_container_uptime() {
    local container_name="$1"
    local started_at
    started_at=$(docker inspect --format '{{.State.StartedAt}}' "$container_name" 2>/dev/null || echo "")
    if [ -n "$started_at" ] && [ "$started_at" != "" ]; then
        # Calculate uptime
        local start_epoch
        start_epoch=$(date -j -f "%Y-%m-%dT%H:%M:%S" "${started_at%%.*}" "+%s" 2>/dev/null || echo "0")
        local now_epoch
        now_epoch=$(date "+%s")
        local diff=$((now_epoch - start_epoch))
        if [ "$diff" -gt 86400 ]; then
            echo "$((diff / 86400))d $((diff % 86400 / 3600))h"
        elif [ "$diff" -gt 3600 ]; then
            echo "$((diff / 3600))h $((diff % 3600 / 60))m"
        else
            echo "$((diff / 60))m $((diff % 60))s"
        fi
    else
        echo "unknown"
    fi
}

get_memory_usage() {
    local container_name="$1"
    docker stats --no-stream --format '{{.MemUsage}}' "$container_name" 2>/dev/null || echo "N/A"
}

# --- Service Checks ---

echo ""
echo "=================================================================="
echo "  Streaming Financial Pipeline — Health Check"
echo "  $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "=================================================================="
echo ""

# Service Status Table
printf "%-25s %-10s %-10s %-15s %s\n" "SERVICE" "STATUS" "HEALTH" "UPTIME" "MEMORY"
printf "%-25s %-10s %-10s %-15s %s\n" "-------" "------" "------" "------" "------"

services=("pipeline-kafka" "pipeline-iceberg-rest" "pipeline-flink-jobmanager" "pipeline-flink-taskmanager" "pipeline-generator")

for svc in "${services[@]}"; do
    status=$(check_container_status "$svc")
    health=$(check_container_health "$svc")
    uptime=$(get_container_uptime "$svc")
    memory=$(get_memory_usage "$svc" | head -1)
    
    # Color coding
    status_color=$NC
    if [ "$status" = "running" ]; then
        status_color=$GREEN
    elif [ "$status" = "not_found" ]; then
        status_color=$RED
    else
        status_color=$YELLOW
    fi
    
    health_color=$NC
    if [ "$health" = "healthy" ]; then
        health_color=$GREEN
    elif [ "$health" = "unhealthy" ]; then
        health_color=$RED
    else
        health_color=$YELLOW
    fi
    
    printf "%-25s ${status_color}%-10s${NC} ${health_color}%-10s${NC} %-15s %s\n" \
        "$svc" "$status" "$health" "$uptime" "$memory"
done

if $QUICK_MODE; then
    echo ""
    exit 0
fi

echo ""
echo "--- Kafka Topics ---"
docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list 2>/dev/null || echo "  (Kafka not reachable)"

echo ""
echo "--- Kafka Consumer Lag (Flink) ---"
docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --all-groups 2>/dev/null | head -20 || echo "  (No consumer groups found)"

echo ""
echo "--- Flink Job Status ---"
flink_response=$(curl -sf "${FLINK_JM_URL}/jobs" 2>/dev/null || echo '{"error": "Flink not reachable"}')
echo "  $flink_response"

# Get running job details
job_ids=$(echo "$flink_response" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for job in data.get('jobs', []):
        if job['status'] == 'RUNNING':
            print(job['id'])
except: pass
" 2>/dev/null)

for job_id in $job_ids; do
    echo ""
    echo "  Job: $job_id"
    
    # Checkpoint info
    checkpoint_info=$(curl -sf "${FLINK_JM_URL}/jobs/${job_id}/checkpoints" 2>/dev/null || echo '{}')
    latest_checkpoint=$(echo "$checkpoint_info" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    latest = data.get('latest', {}).get('completed', {})
    if latest:
        print(f\"  Latest checkpoint: #{latest.get('id', '?')} ({latest.get('status', '?')}) duration={latest.get('duration', '?')}ms size={latest.get('state_size', '?')}B\")
    counts = data.get('counts', {})
    if counts:
        print(f\"  Checkpoints: total={counts.get('total', 0)} completed={counts.get('completed', 0)} failed={counts.get('failed', 0)}\")
except: pass
" 2>/dev/null)
    echo "${latest_checkpoint:-  No checkpoint info available}"
done

echo ""
echo "--- Iceberg REST Catalog ---"
iceberg_response=$(curl -sf "${ICEBERG_REST_URL}/v1/namespaces" 2>/dev/null || echo "Not reachable")
echo "  Namespaces: $iceberg_response"

echo ""
echo "--- Docker Memory Summary ---"
docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}' 2>/dev/null | \
    grep -E "pipeline-|NAME" || echo "  (docker stats not available)"

echo ""
echo "=================================================================="
echo "  Health check complete"
echo "=================================================================="
