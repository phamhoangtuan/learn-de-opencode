"""Parser for data quality check SQL files.

Reads SQL files from a checks directory, extracting metadata from
comment headers (``-- check:``, ``-- severity:``, ``-- description:``).
"""

from __future__ import annotations

import logging
from pathlib import Path

from src.checker.models import CheckModel, CheckSeverity

logger = logging.getLogger(__name__)

# Valid severity values
_VALID_SEVERITIES = {s.value for s in CheckSeverity}


def derive_check_name(file_path: Path) -> str:
    """Derive a check name from a SQL filename.

    Strips the ``check__`` prefix (if present) and the ``.sql`` suffix.

    Args:
        file_path: Path to the SQL file (only the name is used).

    Returns:
        The derived check name.
    """
    stem = file_path.stem
    if "__" in stem:
        # Strip everything before the first __
        _, _, name = stem.partition("__")
        return name
    return stem


def parse_check_file(file_path: Path) -> CheckModel:
    """Parse a single check SQL file into a CheckModel.

    Extracts ``-- check:``, ``-- severity:``, and ``-- description:``
    headers from the first lines of the file. Lines that are not
    recognised headers are treated as SQL content.

    Args:
        file_path: Path to the ``.sql`` file.

    Returns:
        A CheckModel with extracted metadata and SQL content.

    Raises:
        ValueError: If the file cannot be read, is empty, or has no
            SQL content after stripping headers.
    """
    if not file_path.exists():
        msg = f"Cannot read check file: {file_path}"
        raise ValueError(msg)

    content = file_path.read_text(encoding="utf-8").strip()
    if not content:
        msg = f"Check file is empty: {file_path}"
        raise ValueError(msg)

    check_name: str | None = None
    severity = CheckSeverity.WARN
    description = ""
    sql_lines: list[str] = []

    for line in content.splitlines():
        stripped = line.strip()

        if stripped.startswith("-- check:"):
            check_name = stripped.removeprefix("-- check:").strip()
        elif stripped.startswith("-- severity:"):
            raw = stripped.removeprefix("-- severity:").strip().lower()
            if raw in _VALID_SEVERITIES:
                severity = CheckSeverity(raw)
            else:
                logger.warning(
                    "Invalid severity '%s' in %s, defaulting to warn",
                    raw,
                    file_path,
                )
        elif stripped.startswith("-- description:"):
            description = stripped.removeprefix(
                "-- description:"
            ).strip()
        else:
            sql_lines.append(line)

    sql = "\n".join(sql_lines).strip()
    if not sql:
        msg = f"Check file has no SQL content: {file_path}"
        raise ValueError(msg)

    if check_name is None:
        check_name = derive_check_name(file_path)

    return CheckModel(
        name=check_name,
        file_path=file_path,
        sql=sql,
        severity=severity,
        description=description,
    )


def discover_checks(checks_dir: Path) -> list[CheckModel]:
    """Discover and parse all ``.sql`` check files in a directory.

    Files that fail to parse are skipped with a warning log.

    Args:
        checks_dir: Directory containing check SQL files.

    Returns:
        List of parsed CheckModel objects, sorted by filename.

    Raises:
        FileNotFoundError: If the directory does not exist.
    """
    if not checks_dir.is_dir():
        msg = f"Checks directory does not exist: {checks_dir}"
        raise FileNotFoundError(msg)

    sql_files = sorted(checks_dir.glob("*.sql"))
    checks: list[CheckModel] = []

    for sql_file in sql_files:
        try:
            check = parse_check_file(sql_file)
            checks.append(check)
        except ValueError:
            logger.warning("Skipping invalid check file: %s", sql_file)

    return checks
