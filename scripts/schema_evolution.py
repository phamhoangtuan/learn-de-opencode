"""T066: Schema evolution demonstration script.

Demonstrates adding a new column to the financial.transactions Iceberg table
without rewriting existing data, using PyIceberg's update_schema() API.

Per spec.md FR-008: Schema evolution without data rewrite.
Per Constitution v2.0.0 Principle IV: Metadata & Lineage.

Usage:
    python scripts/schema_evolution.py [--catalog-uri URI]
    python scripts/schema_evolution.py --rollback
"""

import argparse
import logging
import sys
from datetime import datetime, timezone

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.types import StringType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_CATALOG_URI = "http://localhost:8181"
DEFAULT_S3_ENDPOINT = "http://localhost:9000"
NAMESPACE = "financial"
TABLE_NAME = "transactions"
DEMO_COLUMN_NAME = "risk_score_category"


def get_catalog(uri: str) -> RestCatalog:
    """Create and return a PyIceberg REST catalog client.

    Args:
        uri: Iceberg REST catalog URI.

    Returns:
        Configured RestCatalog instance.
    """
    return RestCatalog(
        name="rest",
        **{"uri": uri, "s3.endpoint": DEFAULT_S3_ENDPOINT},
    )


def demonstrate_schema_evolution(catalog_uri: str, rollback: bool = False) -> None:
    """Demonstrate Iceberg schema evolution.

    Adds a new optional column 'risk_score_category' (StringType) to the
    transactions table. The column is nullable and has no default value,
    so existing data files are NOT rewritten — they simply return NULL for
    the new column on read.

    Args:
        catalog_uri: Iceberg REST catalog URI.
        rollback: If True, removes the demo column instead.
    """
    catalog = get_catalog(catalog_uri)
    table = catalog.load_table(f"{NAMESPACE}.{TABLE_NAME}")

    current_schema = table.schema()
    logger.info("Current schema has %d fields:", len(current_schema.fields))
    for field in current_schema.fields:
        logger.info(
            "  %d: %s (%s, required=%s)",
            field.field_id,
            field.name,
            field.field_type,
            field.required,
        )

    if rollback:
        _rollback_evolution(table)
    else:
        _apply_evolution(table)

    # Show updated schema
    table = catalog.load_table(f"{NAMESPACE}.{TABLE_NAME}")
    updated_schema = table.schema()
    logger.info("Updated schema has %d fields:", len(updated_schema.fields))
    for field in updated_schema.fields:
        logger.info(
            "  %d: %s (%s, required=%s)",
            field.field_id,
            field.name,
            field.field_type,
            field.required,
        )

    # Show that no data was rewritten
    snapshots = list(table.snapshots())
    logger.info(
        "Total snapshots: %d (schema evolution does NOT create new data snapshots)",
        len(snapshots),
    )

    logger.info("Schema evolution demonstration complete.")
    logger.info(
        "KEY LEARNING: Adding a new column to an Iceberg table does NOT "
        "rewrite existing Parquet files. Existing data returns NULL for the "
        "new column. This is a metadata-only operation."
    )


def _apply_evolution(table) -> None:
    """Add the demo column to the table."""
    schema = table.schema()
    existing_names = {f.name for f in schema.fields}

    if DEMO_COLUMN_NAME in existing_names:
        logger.info(
            "Column '%s' already exists, skipping addition.", DEMO_COLUMN_NAME
        )
        return

    logger.info("Adding new column '%s' (StringType, optional)...", DEMO_COLUMN_NAME)
    with table.update_schema() as update:
        update.add_column(DEMO_COLUMN_NAME, StringType())
    logger.info("Column '%s' added successfully.", DEMO_COLUMN_NAME)


def _rollback_evolution(table) -> None:
    """Remove the demo column from the table."""
    schema = table.schema()
    existing_names = {f.name for f in schema.fields}

    if DEMO_COLUMN_NAME not in existing_names:
        logger.info(
            "Column '%s' does not exist, nothing to rollback.", DEMO_COLUMN_NAME
        )
        return

    logger.info("Removing column '%s'...", DEMO_COLUMN_NAME)
    with table.update_schema() as update:
        update.delete_column(DEMO_COLUMN_NAME)
    logger.info("Column '%s' removed successfully.", DEMO_COLUMN_NAME)


def main() -> None:
    """Entry point for schema evolution demo."""
    parser = argparse.ArgumentParser(
        description="Demonstrate Iceberg schema evolution on financial.transactions"
    )
    parser.add_argument(
        "--catalog-uri",
        default=DEFAULT_CATALOG_URI,
        help=f"Iceberg REST catalog URI (default: {DEFAULT_CATALOG_URI})",
    )
    parser.add_argument(
        "--rollback",
        action="store_true",
        help="Remove the demo column instead of adding it",
    )

    args = parser.parse_args()
    demonstrate_schema_evolution(args.catalog_uri, rollback=args.rollback)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error("Schema evolution demonstration failed: %s", e)
        sys.exit(1)
