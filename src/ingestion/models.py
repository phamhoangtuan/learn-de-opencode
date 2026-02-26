"""Ingestion domain models for pipeline run tracking and validation results."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class RunStatus(enum.Enum):
    """Status of a pipeline execution run."""

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class FileResult:
    """Result of processing a single Parquet file.

    Attributes:
        file_name: Name of the source Parquet file.
        records_loaded: Number of records successfully loaded.
        records_quarantined: Number of records sent to quarantine.
        duplicates_skipped: Number of duplicate records skipped.
        error: Error message if the file could not be processed.
    """

    file_name: str
    records_loaded: int = 0
    records_quarantined: int = 0
    duplicates_skipped: int = 0
    error: str | None = None


@dataclass
class RunResult:
    """Aggregate result of a pipeline execution run.

    Attributes:
        run_id: Unique identifier for this pipeline run.
        status: Final status of the run.
        files_processed: Number of Parquet files processed.
        records_loaded: Total records successfully loaded.
        records_quarantined: Total records sent to quarantine.
        duplicates_skipped: Total duplicate records skipped.
        elapsed_seconds: Total run duration in seconds.
        file_results: Per-file processing results.
        files_checked: Total files discovered before manifest filter (Feature 008).
        files_skipped: Files skipped by manifest (already ingested, same hash).
        files_ingested: Files successfully ingested this run.
        files_failed: Files that failed ingestion this run.
        full_refresh: Whether this run used --full-refresh flag.
    """

    run_id: str
    status: RunStatus = RunStatus.RUNNING
    files_processed: int = 0
    records_loaded: int = 0
    records_quarantined: int = 0
    duplicates_skipped: int = 0
    elapsed_seconds: float = 0.0
    file_results: list[FileResult] = field(default_factory=list)
    files_checked: int = 0
    files_skipped: int = 0
    files_ingested: int = 0
    files_failed: int = 0
    full_refresh: bool = False

    def add_file_result(self, result: FileResult) -> None:
        """Add a file result and update aggregate counters.

        Args:
            result: The file processing result to incorporate.
        """
        self.file_results.append(result)
        self.files_processed += 1
        self.records_loaded += result.records_loaded
        self.records_quarantined += result.records_quarantined
        self.duplicates_skipped += result.duplicates_skipped

    def summary(self) -> str:
        """Generate a human-readable run summary.

        Returns:
            Formatted summary string.
        """
        mode = "Full refresh — manifest bypassed" if self.full_refresh else "Incremental"
        return (
            f"Pipeline Run Summary ({self.run_id}):\n"
            f"  Status:             {self.status.value}\n"
            f"  Mode:               {mode}\n"
            f"  Files checked:      {self.files_checked}\n"
            f"  Files skipped:      {self.files_skipped}\n"
            f"  Files ingested:     {self.files_ingested}\n"
            f"  Files failed:       {self.files_failed}\n"
            f"  Files processed:   {self.files_processed}\n"
            f"  Records loaded:    {self.records_loaded}\n"
            f"  Records quarantined: {self.records_quarantined}\n"
            f"  Duplicates skipped: {self.duplicates_skipped}\n"
            f"  Elapsed time:       {self.elapsed_seconds:.2f}s"
        )


@dataclass
class ValidationError:
    """A single validation failure for a record or file.

    Attributes:
        field: The field that failed validation (or 'schema' for file-level).
        message: Description of the validation failure.
    """

    field: str
    message: str


@dataclass
class ValidationResult:
    """Result of validating a DataFrame.

    Attributes:
        is_valid: Whether the entire input passed validation.
        errors: List of validation errors (file-level or record-level).
        valid_mask: Boolean mask indicating which rows passed validation
            (None for file-level failures where no rows are valid).
    """

    is_valid: bool
    errors: list[ValidationError] = field(default_factory=list)
    valid_mask: list[bool] | None = None
