"""Parser for YAML metadata and SQL file headers for the data catalog."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger("run_catalog")

# Regex patterns for SQL header comments (reuses transformer convention)
_MODEL_PATTERN = re.compile(r"^--\s*model:\s*(.+)$", re.IGNORECASE)
_DEPENDS_PATTERN = re.compile(r"^--\s*depends_on:\s*(.+)$", re.IGNORECASE)
_DESCRIPTION_PATTERN = re.compile(r"^--\s*description:\s*(.+)$", re.IGNORECASE)

# Canonical zone values
VALID_ZONES = frozenset({"raw", "staging", "dimension", "fact", "mart", "metadata"})


@dataclass
class TableYamlMeta:
    """Parsed YAML metadata for a single table."""

    zone: str = ""
    description: str = ""
    owner: str = ""
    columns: dict[str, dict[str, object]] = field(default_factory=dict)


@dataclass
class SqlFileMeta:
    """Metadata extracted from a single SQL file header."""

    model_name: str = ""
    file_path: str = ""
    depends_on: str = ""
    description: str = ""


def load_yaml_metadata(yaml_path: Path | str) -> dict[str, TableYamlMeta]:
    """Load business metadata from a YAML file.

    Args:
        yaml_path: Path to the catalog.yaml file.

    Returns:
        Dict mapping table_name to TableYamlMeta. Empty on missing file.
    """
    yaml_path = Path(yaml_path)
    if not yaml_path.exists():
        logger.warning("Metadata YAML not found: %s", yaml_path)
        return {}

    try:
        import yaml
    except ImportError:
        logger.warning("PyYAML not installed; skipping YAML metadata")
        return {}

    try:
        content = yaml_path.read_text(encoding="utf-8")
        if not content.strip():
            return {}
        data = yaml.safe_load(content)
    except Exception:
        logger.warning("Failed to parse YAML: %s", yaml_path, exc_info=True)
        return {}

    if not isinstance(data, dict):
        return {}

    tables_raw = data.get("tables", data)
    if not isinstance(tables_raw, dict):
        return {}

    result: dict[str, TableYamlMeta] = {}
    for table_name, meta in tables_raw.items():
        if not isinstance(meta, dict):
            continue
        columns_raw = meta.get("columns", {})
        columns: dict[str, dict[str, object]] = {}
        if isinstance(columns_raw, dict):
            for col_name, col_meta in columns_raw.items():
                if isinstance(col_meta, dict):
                    columns[str(col_name)] = col_meta
        result[str(table_name)] = TableYamlMeta(
            zone=str(meta.get("zone", "")),
            description=str(meta.get("description", "")),
            owner=str(meta.get("owner", "")),
            columns=columns,
        )

    return result


def scan_sql_headers(sql_dirs: list[Path | str]) -> dict[str, SqlFileMeta]:
    """Scan SQL files for model/depends_on/description headers.

    Args:
        sql_dirs: Directories to scan for .sql files.

    Returns:
        Dict mapping model_name to SqlFileMeta.
    """
    result: dict[str, SqlFileMeta] = {}

    for sql_dir in sql_dirs:
        sql_dir = Path(sql_dir)
        if not sql_dir.is_dir():
            continue

        for sql_file in sorted(sql_dir.glob("*.sql")):
            try:
                content = sql_file.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                continue

            model_name: str | None = None
            depends_on_parts: list[str] = []
            description = ""

            for line in content.splitlines():
                stripped = line.strip()

                match = _MODEL_PATTERN.match(stripped)
                if match:
                    model_name = match.group(1).strip()
                    continue

                match = _DEPENDS_PATTERN.match(stripped)
                if match:
                    deps_str = match.group(1).strip()
                    deps = [d.strip() for d in deps_str.split(",") if d.strip()]
                    depends_on_parts.extend(deps)
                    continue

                match = _DESCRIPTION_PATTERN.match(stripped)
                if match:
                    description = match.group(1).strip()
                    continue

            # Derive model name from filename if no header
            if not model_name:
                stem = sql_file.stem
                if "__" in stem:
                    _, _, model_name = stem.partition("__")
                else:
                    model_name = stem

            result[model_name] = SqlFileMeta(
                model_name=model_name,
                file_path=str(sql_file),
                depends_on=", ".join(depends_on_parts),
                description=description,
            )

    return result


def infer_zone(table_name: str) -> str:
    """Infer a warehouse zone from a table name using naming conventions.

    Args:
        table_name: The table or view name.

    Returns:
        One of: raw, staging, dimension, fact, mart, metadata.
    """
    if table_name.startswith("stg_"):
        return "staging"
    if table_name.startswith("dim_"):
        return "dimension"
    if table_name.startswith("fct_"):
        return "fact"
    if table_name in {"transactions", "quarantine"}:
        return "raw"
    if table_name.endswith("_runs") or table_name.endswith("_results"):
        return "metadata"
    if table_name.startswith("catalog_"):
        return "metadata"
    return "mart"
