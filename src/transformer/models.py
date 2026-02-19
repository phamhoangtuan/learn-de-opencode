"""Domain models for the SQL transformation runner."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class TransformStatus(enum.Enum):
    """Status of a transform pipeline execution."""

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class TransformModel:
    """A single SQL transform model parsed from a .sql file.

    Attributes:
        name: Model name (derived from filename or -- model: header).
        file_path: Absolute path to the .sql file.
        sql: Raw SQL content of the file.
        depends_on: List of model names this model depends on.
    """

    name: str
    file_path: str
    sql: str
    depends_on: list[str] = field(default_factory=list)


@dataclass
class ModelResult:
    """Result of executing a single transform model.

    Attributes:
        name: Model name.
        success: Whether execution succeeded.
        error: Error message if execution failed.
        elapsed_seconds: Execution duration in seconds.
    """

    name: str
    success: bool
    error: str | None = None
    elapsed_seconds: float = 0.0


@dataclass
class TransformRunResult:
    """Aggregate result of a transform pipeline execution.

    Attributes:
        run_id: Unique identifier for this transform run.
        status: Final status of the run.
        models_executed: Number of models successfully executed.
        models_failed: Number of models that failed.
        elapsed_seconds: Total run duration in seconds.
        error_message: Error description if status is 'failed'.
        model_results: Per-model execution results.
    """

    run_id: str
    status: TransformStatus = TransformStatus.RUNNING
    models_executed: int = 0
    models_failed: int = 0
    elapsed_seconds: float = 0.0
    error_message: str | None = None
    model_results: list[ModelResult] = field(default_factory=list)

    def add_model_result(self, result: ModelResult) -> None:
        """Add a model result and update aggregate counters.

        Args:
            result: The model execution result to incorporate.
        """
        self.model_results.append(result)
        if result.success:
            self.models_executed += 1
        else:
            self.models_failed += 1

    def summary(self) -> str:
        """Generate a human-readable run summary.

        Returns:
            Formatted summary string.
        """
        return (
            f"Transform Run Summary ({self.run_id}):\n"
            f"  Status:           {self.status.value}\n"
            f"  Models executed:  {self.models_executed}\n"
            f"  Models failed:    {self.models_failed}\n"
            f"  Elapsed time:     {self.elapsed_seconds:.2f}s"
        )
