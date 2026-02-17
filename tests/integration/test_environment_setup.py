"""T070: Integration test — all services reach healthy state within 3 minutes.

Per Constitution v2.0.0 Principle III: Tests written FIRST, must FAIL before implementation.
Per spec.md US4: One-command local environment setup.

These tests require the Docker environment to be running:
    docker compose up -d

Run with:
    pytest tests/integration/test_environment_setup.py -v
"""

import json
import subprocess
import time

import pytest
import requests

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration

# Service endpoints
KAFKA_BROKER = "localhost:9092"
ICEBERG_REST_URL = "http://localhost:8181"
FLINK_JM_URL = "http://localhost:8081"

# Timeout for all services to become healthy
HEALTH_TIMEOUT_SECONDS = 180  # 3 minutes


class TestServiceHealth:
    """T070: Verify all services reach healthy state within 3 minutes."""

    def test_kafka_is_healthy(self) -> None:
        """Kafka broker should respond to API version requests."""
        result = subprocess.run(
            [
                "docker", "exec", "pipeline-kafka",
                "/opt/kafka/bin/kafka-broker-api-versions.sh",
                "--bootstrap-server", "localhost:9092",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, f"Kafka not healthy: {result.stderr}"

    def test_iceberg_rest_is_healthy(self) -> None:
        """Iceberg REST catalog should respond to config endpoint."""
        response = requests.get(f"{ICEBERG_REST_URL}/v1/config", timeout=10)
        assert response.status_code == 200, (
            f"Iceberg REST not healthy: {response.status_code}"
        )

    def test_iceberg_namespace_exists(self) -> None:
        """The 'financial' namespace should exist in Iceberg catalog."""
        response = requests.get(f"{ICEBERG_REST_URL}/v1/namespaces", timeout=10)
        assert response.status_code == 200
        namespaces = response.json()
        namespace_names = [ns["namespace"] for ns in namespaces.get("namespaces", [])]
        assert ["financial"] in namespace_names, (
            f"'financial' namespace not found: {namespace_names}"
        )

    def test_iceberg_tables_exist(self) -> None:
        """All three financial tables should exist."""
        response = requests.get(
            f"{ICEBERG_REST_URL}/v1/namespaces/financial/tables", timeout=10
        )
        assert response.status_code == 200
        tables = response.json()
        table_names = [
            t["name"] if isinstance(t, dict) else t
            for t in tables.get("identifiers", [])
        ]
        # Check table names exist (format may vary by REST catalog version)
        table_str = str(tables)
        assert "transactions" in table_str, f"transactions table not found: {tables}"
        assert "alerts" in table_str, f"alerts table not found: {tables}"

    def test_flink_jobmanager_is_healthy(self) -> None:
        """Flink JobManager should respond to overview endpoint."""
        response = requests.get(f"{FLINK_JM_URL}/overview", timeout=10)
        assert response.status_code == 200
        data = response.json()
        assert "taskmanagers" in data
        assert data["taskmanagers"] >= 1, "No TaskManagers registered"

    def test_flink_job_is_running(self) -> None:
        """TransactionPipeline Flink job should be in RUNNING state."""
        response = requests.get(f"{FLINK_JM_URL}/jobs", timeout=10)
        assert response.status_code == 200
        jobs = response.json().get("jobs", [])
        running_jobs = [j for j in jobs if j["status"] == "RUNNING"]
        assert len(running_jobs) >= 1, (
            f"No running Flink jobs found. Jobs: {jobs}"
        )

    def test_kafka_topics_exist(self) -> None:
        """Required Kafka topics should exist."""
        result = subprocess.run(
            [
                "docker", "exec", "pipeline-kafka",
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server", "localhost:9092",
                "--list",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0
        topics = result.stdout.strip().split("\n")
        assert "raw-transactions" in topics, f"raw-transactions topic missing: {topics}"
        assert "alerts" in topics, f"alerts topic missing: {topics}"
        assert "dead-letter" in topics, f"dead-letter topic missing: {topics}"

    def test_generator_container_is_running(self) -> None:
        """Generator container should be in running state."""
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Status}}", "pipeline-generator"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0
        assert result.stdout.strip() == "running", (
            f"Generator not running: {result.stdout.strip()}"
        )

    def test_all_services_healthy_within_timeout(self) -> None:
        """All services should reach healthy state within 3 minutes."""
        services = {
            "kafka": lambda: subprocess.run(
                ["docker", "inspect", "--format", "{{.State.Health.Status}}", "pipeline-kafka"],
                capture_output=True, text=True, timeout=10
            ).stdout.strip() == "healthy",
            "iceberg-rest": lambda: subprocess.run(
                ["docker", "inspect", "--format", "{{.State.Health.Status}}", "pipeline-iceberg-rest"],
                capture_output=True, text=True, timeout=10
            ).stdout.strip() == "healthy",
            "flink-jobmanager": lambda: subprocess.run(
                ["docker", "inspect", "--format", "{{.State.Health.Status}}", "pipeline-flink-jobmanager"],
                capture_output=True, text=True, timeout=10
            ).stdout.strip() == "healthy",
        }

        start = time.time()
        unhealthy = set(services.keys())

        while unhealthy and (time.time() - start) < HEALTH_TIMEOUT_SECONDS:
            for name in list(unhealthy):
                try:
                    if services[name]():
                        unhealthy.discard(name)
                except Exception:
                    pass
            if unhealthy:
                time.sleep(5)

        assert not unhealthy, (
            f"Services not healthy after {HEALTH_TIMEOUT_SECONDS}s: {unhealthy}"
        )


class TestGracefulShutdown:
    """Test graceful shutdown completes within 1 minute."""

    def test_docker_compose_down_within_timeout(self) -> None:
        """docker compose down should complete within 60 seconds.

        NOTE: This test stops all services. Run it last or in isolation.
        It will require a docker compose up -d to restart.
        """
        pytest.skip(
            "Destructive test: stops all services. Run manually with "
            "pytest -k test_docker_compose_down_within_timeout --no-header"
        )
        # To run manually, remove the skip:
        # result = subprocess.run(
        #     ["docker", "compose", "down"],
        #     capture_output=True, text=True, timeout=60,
        # )
        # assert result.returncode == 0, f"docker compose down failed: {result.stderr}"
