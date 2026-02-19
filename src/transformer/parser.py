"""SQL file parser for extracting model metadata and SQL content.

Parses `-- model:` and `-- depends_on:` comment headers from .sql files
per research decision #1.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

from src.transformer.models import TransformModel

logger = logging.getLogger("run_transforms")

# Regex patterns for header comments
_MODEL_PATTERN = re.compile(r"^--\s*model:\s*(.+)$", re.IGNORECASE)
_DEPENDS_PATTERN = re.compile(r"^--\s*depends_on:\s*(.+)$", re.IGNORECASE)


def derive_model_name(file_path: Path) -> str:
    """Derive a model name from a SQL filename.

    Strips the layer prefix (e.g., 'staging__' or 'mart__') and the .sql
    extension. If no double-underscore prefix exists, uses the full stem.

    Args:
        file_path: Path to the .sql file.

    Returns:
        The derived model name.

    Examples:
        >>> derive_model_name(Path("staging__stg_transactions.sql"))
        'stg_transactions'
        >>> derive_model_name(Path("mart__daily_spend.sql"))
        'daily_spend'
        >>> derive_model_name(Path("my_model.sql"))
        'my_model'
    """
    stem = file_path.stem
    if "__" in stem:
        return stem.split("__", 1)[1]
    return stem


def parse_sql_file(file_path: Path) -> TransformModel:
    """Parse a single SQL file into a TransformModel.

    Extracts the model name from a `-- model:` header (or derives it from
    the filename), extracts dependencies from `-- depends_on:` headers,
    and captures the full SQL content.

    Args:
        file_path: Path to the .sql file.

    Returns:
        A TransformModel with name, dependencies, and SQL content.

    Raises:
        ValueError: If the file cannot be read or is empty.
    """
    try:
        content = file_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as e:
        raise ValueError(f"Cannot read SQL file {file_path}: {e}") from e

    if not content.strip():
        raise ValueError(f"SQL file is empty: {file_path}")

    model_name: str | None = None
    depends_on: list[str] = []
    sql_lines: list[str] = []

    for line in content.splitlines():
        stripped = line.strip()

        # Check for model name header
        match = _MODEL_PATTERN.match(stripped)
        if match:
            model_name = match.group(1).strip()
            continue

        # Check for depends_on header
        match = _DEPENDS_PATTERN.match(stripped)
        if match:
            deps_str = match.group(1).strip()
            deps = [d.strip() for d in deps_str.split(",") if d.strip()]
            depends_on.extend(deps)
            continue

        sql_lines.append(line)

    # Fall back to filename-derived name if no -- model: header
    if not model_name:
        model_name = derive_model_name(file_path)

    sql = "\n".join(sql_lines).strip()
    if not sql:
        raise ValueError(f"SQL file has no SQL content after parsing headers: {file_path}")

    logger.debug("Parsed model '%s' from %s (deps: %s)", model_name, file_path, depends_on)

    return TransformModel(
        name=model_name,
        file_path=str(file_path),
        sql=sql,
        depends_on=depends_on,
    )


def discover_models(transforms_dir: Path) -> list[TransformModel]:
    """Discover and parse all SQL files in the transforms directory.

    Args:
        transforms_dir: Directory containing .sql files.

    Returns:
        List of parsed TransformModel objects.

    Raises:
        FileNotFoundError: If the transforms directory does not exist.
    """
    if not transforms_dir.is_dir():
        raise FileNotFoundError(f"Transforms directory not found: {transforms_dir}")

    sql_files = sorted(transforms_dir.glob("*.sql"))
    if not sql_files:
        logger.warning("No .sql files found in %s", transforms_dir)
        return []

    models: list[TransformModel] = []
    for sql_file in sql_files:
        try:
            model = parse_sql_file(sql_file)
            models.append(model)
            logger.info("Discovered model: %s (%s)", model.name, sql_file.name)
        except ValueError as e:
            logger.error("Skipping invalid SQL file: %s", e)

    return models
