"""Domain models for the data catalog builder."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum


class CatalogStatus(Enum):
    """Overall status of a catalog build run."""

    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class CatalogTableEntry:
    """One row destined for the catalog_tables table."""

    table_name: str
    table_schema: str = "main"
    table_type: str = "BASE TABLE"
    zone: str = "raw"
    description: str = ""
    owner: str = ""
    source_sql_file: str | None = None
    depends_on: str | None = None
    row_count: int | None = None
    column_count: int = 0
    last_built_at: str = ""


@dataclass
class CatalogColumnEntry:
    """One row destined for the catalog_columns table."""

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    table_name: str = ""
    column_name: str = ""
    ordinal_position: int = 0
    data_type: str = ""
    is_nullable: bool = True
    is_pk: bool = False
    description: str = ""
    sample_values: str | None = None


@dataclass
class CatalogBuildResult:
    """Aggregate result of one catalog build run."""

    tables_catalogued: int = 0
    columns_catalogued: int = 0
    elapsed_seconds: float = 0.0
    status: CatalogStatus = CatalogStatus.COMPLETED
    error_message: str | None = None

    def summary(self) -> str:
        """Generate a human-readable summary of the catalog build."""
        lines = [
            "Catalog Build Summary:",
            f"  Status:             {self.status.value}",
            f"  Tables catalogued:  {self.tables_catalogued}",
            f"  Columns catalogued: {self.columns_catalogued}",
            f"  Elapsed time:       {self.elapsed_seconds:.2f}s",
        ]
        if self.error_message:
            lines.append(f"  Error:              {self.error_message}")
        return "\n".join(lines)
