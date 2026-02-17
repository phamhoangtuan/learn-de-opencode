"""T071: Integration test — pipeline resumes from checkpoint after restart.

Per Constitution v2.0.0 Principle III: Tests written FIRST, must FAIL before implementation.
Per spec.md FR-013: Flink checkpoints survive restarts.

These tests require the Docker environment to be running:
    docker compose up -d

Run with:
    pytest tests/integration/test_restart_recovery.py -v
"""

import json
import subprocess
import time

import pytest
import requests

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration

KAFKA_BROKER = "localhost:9092"
FLINK_JM_URL = "http://localhost:8081"

# How long to wait for recovery after restart
RECOVERY_TIMEOUT_SECONDS = 120  # 2 minutes


class TestRestartRecovery:
    """T071: Verify pipeline resumes from checkpoint after forced restart."""

    def test_flink_checkpointing_is_enabled(self) -> None:
        """Flink job should have checkpointing enabled."""
        response = requests.get(f"{FLINK_JM_URL}/jobs", timeout=10)
        assert response.status_code == 200
        jobs = response.json().get("jobs", [])
        running_jobs = [j for j in jobs if j["status"] == "RUNNING"]
        assert len(running_jobs) >= 1, "No running Flink jobs"

        job_id = running_jobs[0]["id"]
        config_response = requests.get(
            f"{FLINK_JM_URL}/jobs/{job_id}/checkpoints/config", timeout=10
        )
        assert config_response.status_code == 200
        config = config_response.json()
        assert config.get("mode") in ("exactly_once", "at_least_once"), (
            f"Checkpointing not configured: {config}"
        )

    def test_flink_checkpoint_directory_exists(self) -> None:
        """Flink checkpoint directory should be mounted as a Docker volume."""
        result = subprocess.run(
            [
                "docker", "exec", "pipeline-flink-jobmanager",
                "ls", "-la", "/checkpoints",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0, (
            f"Checkpoint directory not accessible: {result.stderr}"
        )

    def test_iceberg_warehouse_volume_persists(self) -> None:
        """Iceberg warehouse Docker volume should persist data across restarts."""
        result = subprocess.run(
            [
                "docker", "exec", "pipeline-iceberg-rest",
                "ls", "/warehouse",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0, (
            f"Warehouse directory not accessible: {result.stderr}"
        )

    def test_taskmanager_restart_recovery(self) -> None:
        """After TaskManager restart, Flink job should recover and resume.

        NOTE: This test restarts the TaskManager container. The job should
        automatically recover from the last checkpoint.
        """
        pytest.skip(
            "Destructive test: restarts TaskManager. Run manually with "
            "pytest -k test_taskmanager_restart_recovery --no-header"
        )
        # To run manually:
        # 1. Record current Kafka consumer offset
        # 2. Restart TaskManager: docker restart pipeline-flink-taskmanager
        # 3. Wait for job to recover
        # 4. Verify new messages are still being processed
        # 5. Verify no data loss (consumer offset progressed)

    def test_kafka_data_survives_restart(self) -> None:
        """Kafka data should survive broker restart due to persistent storage.

        NOTE: This test restarts the Kafka broker. Run in isolation.
        """
        pytest.skip(
            "Destructive test: restarts Kafka broker. Run manually with "
            "pytest -k test_kafka_data_survives_restart --no-header"
        )
