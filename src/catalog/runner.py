"""Catalog runner: introspects DuckDB, merges YAML metadata, persists catalog.

Connects to the DuckDB warehouse, queries information_schema for all tables
and columns, merges business metadata from YAML and SQL file headers, and
persists to catalog_tables and catalog_columns for browsing.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from src.catalog.models import (
    CatalogBuildResult,
    CatalogColumnEntry,
    CatalogStatus,
    CatalogTableEntry,
)
from src.catalog.parser import (
    SqlFileMeta,
    TableYamlMeta,
    infer_zone,
    load_yaml_metadata,
    scan_sql_headers,
)

logger = logging.getLogger("run_catalog")

DEFAULT_DB_PATH: str = "data/warehouse/transactions.duckdb"
DEFAULT_YAML_PATH: str = "src/metadata/catalog.yaml"
DEFAULT_SQL_DIRS: list[str] = ["src/transforms", "src/fact_transforms", "src/checks"]

_CATALOG_TABLES_DDL: str = """
CREATE TABLE IF NOT EXISTS catalog_tables (
    table_name       VARCHAR NOT NULL PRIMARY KEY,
    table_schema     VARCHAR NOT NULL,
    table_type       VARCHAR NOT NULL,
    zone             VARCHAR NOT NULL,
    description      VARCHAR,
    owner            VARCHAR,
    source_sql_file  VARCHAR,
    depends_on       VARCHAR,
    row_count        BIGINT,
    column_count     INTEGER NOT NULL,
    last_built_at    TIMESTAMPTZ NOT NULL
);
"""

_CATALOG_COLUMNS_DDL: str = """
CREATE TABLE IF NOT EXISTS catalog_columns (
    id               VARCHAR NOT NULL PRIMARY KEY,
    table_name       VARCHAR NOT NULL,
    column_name      VARCHAR NOT NULL,
    ordinal_position INTEGER NOT NULL,
    data_type        VARCHAR NOT NULL,
    is_nullable      BOOLEAN NOT NULL,
    is_pk            BOOLEAN NOT NULL DEFAULT FALSE,
    description      VARCHAR,
    sample_values    VARCHAR
);
"""


def create_catalog_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the catalog_tables and catalog_columns tables if absent.

    Args:
        conn: An open DuckDB connection.
    """
    conn.execute(_CATALOG_TABLES_DDL)
    conn.execute(_CATALOG_COLUMNS_DDL)
    logger.debug("catalog_tables and catalog_columns tables ready")


def _get_sample_values(
    conn: duckdb.DuckDBPyConnection, table_name: str, column_name: str,
) -> str | None:
    """Get up to 3 distinct sample values as a JSON array string."""
    try:
        rows = conn.execute(
            f'SELECT DISTINCT "{column_name}" FROM "{table_name}" '
            f"WHERE \"{column_name}\" IS NOT NULL LIMIT 3"
        ).fetchall()
        if not rows:
            return None
        values = [str(row[0]) for row in rows]
        return json.dumps(values)
    except duckdb.Error:
        return None


def build_catalog(
    *,
    db_path: str | Path = DEFAULT_DB_PATH,
    yaml_path: str | Path = DEFAULT_YAML_PATH,
    sql_dirs: list[str] | None = None,
) -> CatalogBuildResult:
    """Build the data catalog by introspecting DuckDB and merging YAML metadata.

    Args:
        db_path: Path to the DuckDB database file.
        yaml_path: Path to the YAML business metadata file.
        sql_dirs: Directories to scan for SQL file headers.

    Returns:
        CatalogBuildResult with counts, timing, and status.
    """
    db_path = Path(db_path)
    yaml_path = Path(yaml_path)
    if sql_dirs is None:
        sql_dirs = DEFAULT_SQL_DIRS

    start_time = time.monotonic()
    result = CatalogBuildResult()

    # Validate database exists
    if not db_path.exists():
        result.status = CatalogStatus.FAILED
        result.error_message = f"Database not found: {db_path}"
        result.elapsed_seconds = time.monotonic() - start_time
        logger.error(result.error_message)
        return result

    conn = duckdb.connect(str(db_path))

    try:
        # Create catalog tables
        create_catalog_tables(conn)

        # Load metadata
        yaml_meta = load_yaml_metadata(yaml_path)
        sql_meta = scan_sql_headers([Path(d) for d in sql_dirs])

        now = datetime.now(UTC).isoformat()

        # Introspect all tables/views in schema 'main'
        tables_info = conn.execute(
            "SELECT table_name, table_type "
            "FROM information_schema.tables "
            "WHERE table_schema = 'main' "
            "ORDER BY table_name"
        ).fetchall()

        table_entries: list[CatalogTableEntry] = []
        column_entries: list[CatalogColumnEntry] = []

        for table_name, table_type in tables_info:
            # Get YAML metadata for this table
            ymeta: TableYamlMeta | None = yaml_meta.get(table_name)

            # Get SQL file metadata for this table
            smeta: SqlFileMeta | None = sql_meta.get(table_name)

            # Determine zone: YAML > inferred
            zone = (ymeta.zone if ymeta and ymeta.zone else "") or infer_zone(table_name)

            # Determine description: YAML > SQL header
            description = ""
            if ymeta and ymeta.description:
                description = ymeta.description
            elif smeta and smeta.description:
                description = smeta.description

            # Owner from YAML
            owner = ymeta.owner if ymeta else ""

            # Source SQL file and depends_on from SQL headers
            source_sql_file = smeta.file_path if smeta else None
            depends_on = smeta.depends_on if smeta and smeta.depends_on else None

            # Row count: COUNT(*) for TABLEs, NULL for VIEWs
            row_count: int | None = None
            if table_type == "BASE TABLE":
                try:
                    count_row = conn.execute(
                        f'SELECT COUNT(*) FROM "{table_name}"'
                    ).fetchone()
                    row_count = count_row[0] if count_row else 0
                except duckdb.Error:
                    row_count = None

            # Get columns from information_schema
            columns_info = conn.execute(
                "SELECT column_name, ordinal_position, data_type, is_nullable "
                "FROM information_schema.columns "
                "WHERE table_schema = 'main' AND table_name = ? "
                "ORDER BY ordinal_position",
                [table_name],
            ).fetchall()

            column_count = len(columns_info)

            table_entries.append(CatalogTableEntry(
                table_name=table_name,
                table_schema="main",
                table_type=table_type,
                zone=zone,
                description=description,
                owner=owner,
                source_sql_file=source_sql_file,
                depends_on=depends_on,
                row_count=row_count,
                column_count=column_count,
                last_built_at=now,
            ))

            # Build column entries
            yaml_columns = ymeta.columns if ymeta else {}
            for col_name, ordinal, data_type, is_nullable_str in columns_info:
                is_nullable = is_nullable_str == "YES"
                col_yaml = yaml_columns.get(col_name, {})
                is_pk = bool(col_yaml.get("is_pk", False))
                col_desc = str(col_yaml.get("description", ""))

                # Sample values only for BASE TABLEs
                sample_values: str | None = None
                if table_type == "BASE TABLE":
                    sample_values = _get_sample_values(conn, table_name, col_name)

                column_entries.append(CatalogColumnEntry(
                    id=str(uuid.uuid4()),
                    table_name=table_name,
                    column_name=col_name,
                    ordinal_position=ordinal,
                    data_type=data_type,
                    is_nullable=is_nullable,
                    is_pk=is_pk,
                    description=col_desc,
                    sample_values=sample_values,
                ))

        # Idempotent rebuild: DELETE + INSERT
        conn.execute("DELETE FROM catalog_columns")
        conn.execute("DELETE FROM catalog_tables")

        # Insert table entries
        for entry in table_entries:
            conn.execute(
                "INSERT INTO catalog_tables "
                "(table_name, table_schema, table_type, zone, description, "
                "owner, source_sql_file, depends_on, row_count, column_count, last_built_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::TIMESTAMPTZ)",
                [
                    entry.table_name,
                    entry.table_schema,
                    entry.table_type,
                    entry.zone,
                    entry.description,
                    entry.owner,
                    entry.source_sql_file,
                    entry.depends_on,
                    entry.row_count,
                    entry.column_count,
                    entry.last_built_at,
                ],
            )

        # Insert column entries
        for entry in column_entries:
            conn.execute(
                "INSERT INTO catalog_columns "
                "(id, table_name, column_name, ordinal_position, data_type, "
                "is_nullable, is_pk, description, sample_values) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    entry.id,
                    entry.table_name,
                    entry.column_name,
                    entry.ordinal_position,
                    entry.data_type,
                    entry.is_nullable,
                    entry.is_pk,
                    entry.description,
                    entry.sample_values,
                ],
            )

        result.tables_catalogued = len(table_entries)
        result.columns_catalogued = len(column_entries)
        result.status = CatalogStatus.COMPLETED
        result.elapsed_seconds = time.monotonic() - start_time

        logger.info(result.summary())
        return result

    except Exception as exc:
        result.status = CatalogStatus.FAILED
        result.error_message = str(exc)
        result.elapsed_seconds = time.monotonic() - start_time
        logger.error("Catalog build failed: %s", exc)
        return result

    finally:
        conn.close()
