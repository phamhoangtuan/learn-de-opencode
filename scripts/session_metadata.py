"""T067: Pipeline session metadata capture to Iceberg.

Writes pipeline_session_metadata records to the Iceberg table, capturing
session-level metrics (record counts, latency percentiles, throughput).

Per Constitution v2.0.0 Principle IV: Metadata & Lineage.
Per data-model.md: pipeline_session_metadata table schema.

Usage:
    # Start a session (writes initial record with start_timestamp):
    python scripts/session_metadata.py start

    # End a session (updates end_timestamp and final metrics):
    python scripts/session_metadata.py end --session-id <UUID> \
        --records-generated 1000 --records-published 1000 \
        --records-processed 995 --records-stored 990 \
        --alerts-generated 15 --records-dead-lettered 5 \
        --throughput-avg 10.5 \
        --latency-p50 120 --latency-p95 450 --latency-p99 890

    # List all sessions:
    python scripts/session_metadata.py list
"""

import argparse
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from typing import Optional

import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_CATALOG_URI = "http://localhost:8181"
DEFAULT_S3_ENDPOINT = "http://localhost:9000"
NAMESPACE = "financial"
TABLE_NAME = "pipeline_session_metadata"


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


def start_session(catalog_uri: str) -> str:
    """Start a new pipeline session by writing an initial metadata record.

    Args:
        catalog_uri: Iceberg REST catalog URI.

    Returns:
        The generated session_id UUID string.
    """
    catalog = get_catalog(catalog_uri)
    table = catalog.load_table(f"{NAMESPACE}.{TABLE_NAME}")

    session_id = str(uuid.uuid4())
    now = datetime.now(tz=timezone.utc)

    record = pa.table(
        {
            "session_id": [session_id],
            "start_timestamp": pa.array([now], type=pa.timestamp("us", tz="UTC")),
            "end_timestamp": pa.array([None], type=pa.timestamp("us", tz="UTC")),
            "records_generated": pa.array([None], type=pa.int64()),
            "records_published": pa.array([None], type=pa.int64()),
            "records_processed": pa.array([None], type=pa.int64()),
            "records_stored": pa.array([None], type=pa.int64()),
            "alerts_generated": pa.array([None], type=pa.int64()),
            "records_dead_lettered": pa.array([None], type=pa.int64()),
            "throughput_avg": pa.array([None], type=pa.float64()),
            "latency_p50_ms": pa.array([None], type=pa.int64()),
            "latency_p95_ms": pa.array([None], type=pa.int64()),
            "latency_p99_ms": pa.array([None], type=pa.int64()),
        }
    )

    table.append(record)

    logger.info(
        "Started pipeline session",
        extra={
            "session_id": session_id,
            "start_timestamp": now.isoformat(),
        },
    )
    print(f"Session started: {session_id}")
    return session_id


def end_session(
    catalog_uri: str,
    session_id: str,
    records_generated: Optional[int] = None,
    records_published: Optional[int] = None,
    records_processed: Optional[int] = None,
    records_stored: Optional[int] = None,
    alerts_generated: Optional[int] = None,
    records_dead_lettered: Optional[int] = None,
    throughput_avg: Optional[float] = None,
    latency_p50_ms: Optional[int] = None,
    latency_p95_ms: Optional[int] = None,
    latency_p99_ms: Optional[int] = None,
) -> None:
    """End a pipeline session by appending a completion record with final metrics.

    Note: Iceberg append-only — we write a new record with end_timestamp set.
    The latest record for a given session_id (by start_timestamp) is the current state.

    Args:
        catalog_uri: Iceberg REST catalog URI.
        session_id: The session UUID to end.
        records_generated: Total records produced by generator.
        records_published: Total records sent to Kafka.
        records_processed: Total records processed by Flink.
        records_stored: Total records written to Iceberg.
        alerts_generated: Total alerts created.
        records_dead_lettered: Total records routed to DLQ.
        throughput_avg: Average records/sec.
        latency_p50_ms: 50th percentile end-to-end latency in ms.
        latency_p95_ms: 95th percentile end-to-end latency in ms.
        latency_p99_ms: 99th percentile end-to-end latency in ms.
    """
    catalog = get_catalog(catalog_uri)
    table = catalog.load_table(f"{NAMESPACE}.{TABLE_NAME}")

    now = datetime.now(tz=timezone.utc)

    # Read existing session to get start_timestamp
    arrow_data = table.scan().to_arrow()
    import duckdb

    result = duckdb.sql(
        f"SELECT start_timestamp FROM arrow_data WHERE session_id = '{session_id}' "
        f"ORDER BY start_timestamp DESC LIMIT 1"
    ).fetchone()

    if result is None:
        logger.error("Session '%s' not found", session_id)
        print(f"Error: Session '{session_id}' not found")
        sys.exit(1)

    start_timestamp = result[0]

    record = pa.table(
        {
            "session_id": [session_id],
            "start_timestamp": pa.array([start_timestamp], type=pa.timestamp("us", tz="UTC")),
            "end_timestamp": pa.array([now], type=pa.timestamp("us", tz="UTC")),
            "records_generated": pa.array([records_generated], type=pa.int64()),
            "records_published": pa.array([records_published], type=pa.int64()),
            "records_processed": pa.array([records_processed], type=pa.int64()),
            "records_stored": pa.array([records_stored], type=pa.int64()),
            "alerts_generated": pa.array([alerts_generated], type=pa.int64()),
            "records_dead_lettered": pa.array([records_dead_lettered], type=pa.int64()),
            "throughput_avg": pa.array([throughput_avg], type=pa.float64()),
            "latency_p50_ms": pa.array([latency_p50_ms], type=pa.int64()),
            "latency_p95_ms": pa.array([latency_p95_ms], type=pa.int64()),
            "latency_p99_ms": pa.array([latency_p99_ms], type=pa.int64()),
        }
    )

    table.append(record)

    logger.info(
        "Ended pipeline session",
        extra={
            "session_id": session_id,
            "end_timestamp": now.isoformat(),
            "records_generated": records_generated,
            "records_processed": records_processed,
            "alerts_generated": alerts_generated,
        },
    )
    print(f"Session ended: {session_id}")


def list_sessions(catalog_uri: str) -> None:
    """List all pipeline sessions.

    Args:
        catalog_uri: Iceberg REST catalog URI.
    """
    catalog = get_catalog(catalog_uri)
    table = catalog.load_table(f"{NAMESPACE}.{TABLE_NAME}")

    arrow_data = table.scan().to_arrow()
    if len(arrow_data) == 0:
        print("No sessions found.")
        return

    import duckdb

    result = duckdb.sql("""
        SELECT
            session_id,
            start_timestamp,
            end_timestamp,
            records_generated,
            records_processed,
            alerts_generated,
            records_dead_lettered,
            throughput_avg
        FROM arrow_data
        ORDER BY start_timestamp DESC
    """).fetch_arrow_table().to_pandas()

    print(result.to_string())


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    """Parse CLI arguments for session metadata management.

    Args:
        argv: Command-line arguments (defaults to sys.argv).

    Returns:
        Parsed argument namespace with subcommand and session parameters.
    """
    parser = argparse.ArgumentParser(
        description="Manage pipeline session metadata in Iceberg"
    )
    parser.add_argument(
        "--catalog-uri",
        default=DEFAULT_CATALOG_URI,
        help=f"Iceberg REST catalog URI (default: {DEFAULT_CATALOG_URI})",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # start command
    subparsers.add_parser("start", help="Start a new pipeline session")

    # end command
    end_parser = subparsers.add_parser("end", help="End a pipeline session")
    end_parser.add_argument("--session-id", required=True, help="Session UUID to end")
    end_parser.add_argument("--records-generated", type=int, default=None)
    end_parser.add_argument("--records-published", type=int, default=None)
    end_parser.add_argument("--records-processed", type=int, default=None)
    end_parser.add_argument("--records-stored", type=int, default=None)
    end_parser.add_argument("--alerts-generated", type=int, default=None)
    end_parser.add_argument("--records-dead-lettered", type=int, default=None)
    end_parser.add_argument("--throughput-avg", type=float, default=None)
    end_parser.add_argument("--latency-p50", type=int, default=None)
    end_parser.add_argument("--latency-p95", type=int, default=None)
    end_parser.add_argument("--latency-p99", type=int, default=None)

    # list command
    subparsers.add_parser("list", help="List all pipeline sessions")

    return parser.parse_args(argv)


def main() -> None:
    """Entry point for session metadata CLI."""
    args = parse_args()

    if args.command == "start":
        start_session(args.catalog_uri)
    elif args.command == "end":
        end_session(
            catalog_uri=args.catalog_uri,
            session_id=args.session_id,
            records_generated=args.records_generated,
            records_published=args.records_published,
            records_processed=args.records_processed,
            records_stored=args.records_stored,
            alerts_generated=args.alerts_generated,
            records_dead_lettered=args.records_dead_lettered,
            throughput_avg=args.throughput_avg,
            latency_p50_ms=args.latency_p50,
            latency_p95_ms=args.latency_p95,
            latency_p99_ms=args.latency_p99,
        )
    elif args.command == "list":
        list_sessions(args.catalog_uri)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error("Session metadata operation failed: %s", e)
        sys.exit(1)
