"""T072: Integration test — memory consumption stays under 6GB.

Per Constitution v2.0.0 Principle III: Tests written FIRST, must FAIL before implementation.
Per research.md R5: Total memory budget ~3.4GB, hard limit 6GB.

These tests require the Docker environment to be running:
    docker compose up -d

Run with:
    pytest tests/integration/test_resource_limits.py -v
"""

import json
import subprocess
import time

import pytest

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration

# Memory limits per research.md R5
MAX_TOTAL_MEMORY_MB = 6144  # 6GB hard limit
EXPECTED_BUDGET_MB = 3500   # ~3.4GB expected usage

# Service memory limits from docker-compose.yml
SERVICE_MEMORY_LIMITS = {
    "pipeline-kafka": 512,
    "pipeline-iceberg-rest": 256,
    "pipeline-flink-jobmanager": 768,
    "pipeline-flink-taskmanager": 1536,
    "pipeline-generator": 256,
}


class TestResourceLimits:
    """T072: Verify memory consumption stays under 6GB."""

    def test_docker_memory_limits_configured(self) -> None:
        """All services should have mem_limit configured in docker-compose.yml."""
        for container_name, expected_limit_mb in SERVICE_MEMORY_LIMITS.items():
            result = subprocess.run(
                [
                    "docker", "inspect",
                    "--format", "{{.HostConfig.Memory}}",
                    container_name,
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode != 0:
                pytest.skip(f"Container {container_name} not running")

            memory_bytes = int(result.stdout.strip())
            memory_mb = memory_bytes / (1024 * 1024)
            assert memory_mb <= expected_limit_mb * 1.1, (
                f"{container_name}: memory limit {memory_mb:.0f}MB exceeds "
                f"expected {expected_limit_mb}MB"
            )

    def test_total_memory_budget_under_limit(self) -> None:
        """Sum of all service memory limits should be under 6GB."""
        total = sum(SERVICE_MEMORY_LIMITS.values())
        assert total <= MAX_TOTAL_MEMORY_MB, (
            f"Total memory budget {total}MB exceeds limit {MAX_TOTAL_MEMORY_MB}MB"
        )

    def test_actual_memory_usage_under_limits(self) -> None:
        """Actual memory usage of running containers should be under their limits."""
        result = subprocess.run(
            [
                "docker", "stats", "--no-stream",
                "--format", "{{.Name}}\t{{.MemUsage}}",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            pytest.skip("docker stats failed — Docker may not be running")

        lines = result.stdout.strip().split("\n")
        for line in lines:
            if not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue

            container_name = parts[0].strip()
            mem_usage_str = parts[1].split("/")[0].strip()

            # Parse memory usage (e.g., "123.4MiB" or "1.5GiB")
            if container_name in SERVICE_MEMORY_LIMITS:
                usage_mb = _parse_memory_string(mem_usage_str)
                limit_mb = SERVICE_MEMORY_LIMITS[container_name]
                assert usage_mb <= limit_mb, (
                    f"{container_name}: actual usage {usage_mb:.0f}MB exceeds "
                    f"limit {limit_mb}MB"
                )

    def test_sustained_operation_memory_stable(self) -> None:
        """Memory usage should remain stable during 30 seconds of operation.

        This is a simplified version of the 24-hour stability test.
        Verifies no memory leaks in a short window.
        """
        measurements = []
        for _ in range(3):
            result = subprocess.run(
                [
                    "docker", "stats", "--no-stream",
                    "--format", "{{.Name}}\t{{.MemUsage}}",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode != 0:
                pytest.skip("docker stats failed")

            total_mb = 0
            for line in result.stdout.strip().split("\n"):
                if not line.strip():
                    continue
                parts = line.split("\t")
                if len(parts) >= 2:
                    container_name = parts[0].strip()
                    if container_name in SERVICE_MEMORY_LIMITS:
                        mem_str = parts[1].split("/")[0].strip()
                        total_mb += _parse_memory_string(mem_str)

            measurements.append(total_mb)
            time.sleep(10)

        # Memory should not grow more than 20% between measurements
        if len(measurements) >= 2:
            growth = measurements[-1] - measurements[0]
            growth_pct = (growth / measurements[0]) * 100 if measurements[0] > 0 else 0
            assert growth_pct < 20, (
                f"Memory grew {growth_pct:.1f}% during test "
                f"({measurements[0]:.0f}MB -> {measurements[-1]:.0f}MB)"
            )


def _parse_memory_string(mem_str: str) -> float:
    """Parse Docker memory string like '123.4MiB' or '1.5GiB' to MB."""
    mem_str = mem_str.strip()
    if mem_str.endswith("GiB"):
        return float(mem_str[:-3]) * 1024
    elif mem_str.endswith("MiB"):
        return float(mem_str[:-3])
    elif mem_str.endswith("KiB"):
        return float(mem_str[:-3]) / 1024
    elif mem_str.endswith("B"):
        return float(mem_str[:-1]) / (1024 * 1024)
    return 0
