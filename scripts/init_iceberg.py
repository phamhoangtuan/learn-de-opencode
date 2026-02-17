"""Initialize Iceberg tables via PyIceberg REST catalog client.

Creates the 'financial' namespace and three tables:
- financial.transactions
- financial.alerts  
- financial.pipeline_session_metadata

Idempotent: skips creation if namespace/table already exists.
Per Constitution v2.0.0 Principle IV (Metadata & Lineage).
"""

import os
import sys
import logging

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

ICEBERG_REST_URI = os.environ.get("ICEBERG_REST_URI", "http://localhost:8181")
NAMESPACE = "financial"


def get_catalog() -> RestCatalog:
    """Create and return a PyIceberg REST catalog client.

    Returns:
        Configured RestCatalog instance connected to the REST endpoint.
    """
    return RestCatalog(
        name="rest",
        **{"uri": ICEBERG_REST_URI, "s3.endpoint": "http://localhost:9000"},
    )


def create_namespace(catalog: RestCatalog) -> None:
    """Create the 'financial' namespace if it doesn't exist.

    Args:
        catalog: PyIceberg REST catalog client.
    """
    try:
        catalog.create_namespace(NAMESPACE)
        logger.info("Created namespace '%s'", NAMESPACE)
    except NamespaceAlreadyExistsError:
        logger.info("Namespace '%s' already exists, skipping", NAMESPACE)


def create_transactions_table(catalog: RestCatalog) -> None:
    """Create the transactions Iceberg table.

    Schema from data-model.md: 10 base + 3 enrichment fields.
    Partitioned by days(timestamp), sorted by transaction_id.

    Args:
        catalog: PyIceberg REST catalog client.
    """
    schema = Schema(
        NestedField(field_id=1, name="transaction_id", field_type=StringType(), required=True),
        NestedField(field_id=2, name="timestamp", field_type=TimestamptzType(), required=True),
        NestedField(field_id=3, name="account_id", field_type=StringType(), required=True),
        NestedField(field_id=4, name="amount", field_type=DecimalType(precision=10, scale=2), required=True),
        NestedField(field_id=5, name="currency", field_type=StringType(), required=True),
        NestedField(field_id=6, name="merchant_name", field_type=StringType(), required=True),
        NestedField(field_id=7, name="merchant_category", field_type=StringType(), required=True),
        NestedField(field_id=8, name="transaction_type", field_type=StringType(), required=True),
        NestedField(field_id=9, name="location_country", field_type=StringType(), required=True),
        NestedField(field_id=10, name="status", field_type=StringType(), required=True),
        NestedField(field_id=11, name="processing_timestamp", field_type=TimestamptzType(), required=False),
        NestedField(field_id=12, name="partition_date", field_type=DateType(), required=False),
        NestedField(field_id=13, name="is_flagged", field_type=BooleanType(), required=False),
    )

    partition_spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="timestamp_day")
    )

    sort_order = SortOrder(SortField(source_id=1))

    try:
        catalog.create_table(
            identifier=f"{NAMESPACE}.transactions",
            schema=schema,
            partition_spec=partition_spec,
            sort_order=sort_order,
        )
        logger.info("Created table '%s.transactions'", NAMESPACE)
    except TableAlreadyExistsError:
        logger.info("Table '%s.transactions' already exists, skipping", NAMESPACE)


def create_alerts_table(catalog: RestCatalog) -> None:
    """Create the alerts Iceberg table.

    Schema from data-model.md: 6 fields.
    Partitioned by days(alert_timestamp).

    Args:
        catalog: PyIceberg REST catalog client.
    """
    schema = Schema(
        NestedField(field_id=1, name="alert_id", field_type=StringType(), required=True),
        NestedField(field_id=2, name="transaction_id", field_type=StringType(), required=True),
        NestedField(field_id=3, name="rule_name", field_type=StringType(), required=True),
        NestedField(field_id=4, name="severity", field_type=StringType(), required=True),
        NestedField(field_id=5, name="alert_timestamp", field_type=TimestamptzType(), required=True),
        NestedField(field_id=6, name="description", field_type=StringType(), required=True),
    )

    partition_spec = PartitionSpec(
        PartitionField(source_id=5, field_id=1000, transform=DayTransform(), name="alert_timestamp_day")
    )

    try:
        catalog.create_table(
            identifier=f"{NAMESPACE}.alerts",
            schema=schema,
            partition_spec=partition_spec,
        )
        logger.info("Created table '%s.alerts'", NAMESPACE)
    except TableAlreadyExistsError:
        logger.info("Table '%s.alerts' already exists, skipping", NAMESPACE)


def create_pipeline_session_metadata_table(catalog: RestCatalog) -> None:
    """Create the pipeline_session_metadata Iceberg table.

    Schema from data-model.md: 13 fields. No partitioning (low volume).

    Args:
        catalog: PyIceberg REST catalog client.
    """
    schema = Schema(
        NestedField(field_id=1, name="session_id", field_type=StringType(), required=True),
        NestedField(field_id=2, name="start_timestamp", field_type=TimestamptzType(), required=True),
        NestedField(field_id=3, name="end_timestamp", field_type=TimestamptzType(), required=False),
        NestedField(field_id=4, name="records_generated", field_type=LongType(), required=False),
        NestedField(field_id=5, name="records_published", field_type=LongType(), required=False),
        NestedField(field_id=6, name="records_processed", field_type=LongType(), required=False),
        NestedField(field_id=7, name="records_stored", field_type=LongType(), required=False),
        NestedField(field_id=8, name="alerts_generated", field_type=LongType(), required=False),
        NestedField(field_id=9, name="records_dead_lettered", field_type=LongType(), required=False),
        NestedField(field_id=10, name="throughput_avg", field_type=DoubleType(), required=False),
        NestedField(field_id=11, name="latency_p50_ms", field_type=LongType(), required=False),
        NestedField(field_id=12, name="latency_p95_ms", field_type=LongType(), required=False),
        NestedField(field_id=13, name="latency_p99_ms", field_type=LongType(), required=False),
    )

    try:
        catalog.create_table(
            identifier=f"{NAMESPACE}.pipeline_session_metadata",
            schema=schema,
        )
        logger.info("Created table '%s.pipeline_session_metadata'", NAMESPACE)
    except TableAlreadyExistsError:
        logger.info("Table '%s.pipeline_session_metadata' already exists, skipping", NAMESPACE)


def main() -> None:
    """Initialize all Iceberg tables."""
    logger.info("Connecting to Iceberg REST catalog at %s", ICEBERG_REST_URI)
    catalog = get_catalog()

    create_namespace(catalog)
    create_transactions_table(catalog)
    create_alerts_table(catalog)
    create_pipeline_session_metadata_table(catalog)

    logger.info("Iceberg initialization complete")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error("Iceberg initialization failed: %s", e)
        sys.exit(1)
