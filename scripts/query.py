"""DuckDB + PyIceberg analytics CLI for querying financial pipeline data.

T064: Core CLI with DuckDB+PyIceberg analytics subcommands.
T065: Time-travel query support (--snapshot-id, --as-of-timestamp).
T069: Structured logging for analytics queries.

Per spec.md FR-007: Time-travel queries on stored Iceberg data.
Per spec.md FR-008: Schema evolution support.
Per SC-007: Analytical queries within 10 seconds for 1M records.
Per Constitution v2.0.0 Principle IV: Metadata & Lineage.
"""

import argparse
import csv
import io
import json
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import duckdb
from pyiceberg.catalog.rest import RestCatalog

# Structured JSON logging per Constitution v2.0.0 Principle IV
logger = logging.getLogger("query")

DEFAULT_CATALOG_URI = "http://localhost:8181"
DEFAULT_S3_ENDPOINT = "http://localhost:9000"
NAMESPACE = "financial"


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    """Parse CLI arguments for the analytics query tool.

    Args:
        argv: Command-line arguments. Uses sys.argv if None.

    Returns:
        Parsed argument namespace.
    """
    parser = argparse.ArgumentParser(
        description="Query financial pipeline Iceberg tables via DuckDB + PyIceberg",
        prog="query",
    )

    # Table selection
    parser.add_argument(
        "--table",
        choices=["transactions", "alerts"],
        default="transactions",
        help="Which Iceberg table to query (default: transactions)",
    )

    # Result limiting
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=100,
        help="Maximum number of rows to return (default: 100)",
    )

    # Filtering
    parser.add_argument(
        "--date-range",
        nargs=2,
        metavar=("START", "END"),
        help="Filter by date range (ISO 8601 dates, e.g. 2026-01-01 2026-01-31)",
    )
    parser.add_argument(
        "--account-id",
        default=None,
        help="Filter transactions by account ID",
    )

    # Alert-related
    parser.add_argument(
        "--show-alerts",
        action="store_true",
        default=False,
        help="Show alerts related to matching transactions",
    )
    parser.add_argument(
        "--join-alerts",
        action="store_true",
        default=False,
        help="Join transactions with alerts for full context",
    )

    # Time-travel (mutually exclusive)
    time_travel_group = parser.add_mutually_exclusive_group()
    time_travel_group.add_argument(
        "--snapshot-id",
        default=None,
        help="Query table at a specific Iceberg snapshot ID",
    )
    time_travel_group.add_argument(
        "--as-of-timestamp",
        default=None,
        help="Query table as of a specific ISO 8601 timestamp",
    )

    # Connection
    parser.add_argument(
        "--catalog-uri",
        default=DEFAULT_CATALOG_URI,
        help=f"Iceberg REST catalog URI (default: {DEFAULT_CATALOG_URI})",
    )

    # Output format
    parser.add_argument(
        "--format",
        dest="output_format",
        choices=["table", "json", "csv"],
        default="table",
        help="Output format (default: table)",
    )

    return parser.parse_args(argv)


def _positive_int(value: str) -> int:
    """Validate that a CLI argument is a positive integer."""
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError(f"{value} is not a positive integer")
    return ivalue


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


def validate_time_travel_args(args: argparse.Namespace) -> None:
    """Validate time-travel arguments.

    Args:
        args: Parsed CLI arguments.

    Raises:
        ValueError: If as-of-timestamp is not a valid ISO 8601 string.
    """
    if args.as_of_timestamp is not None:
        try:
            datetime.fromisoformat(args.as_of_timestamp.replace("Z", "+00:00"))
        except (ValueError, AttributeError) as e:
            raise ValueError(
                f"Invalid timestamp format: '{args.as_of_timestamp}'. "
                "Expected ISO 8601 format (e.g. 2026-02-16T14:30:00Z)"
            ) from e


def build_query(args: argparse.Namespace) -> str:
    """Build a DuckDB SQL query from parsed CLI arguments.

    The query operates on Arrow tables named 'tx_arrow' (transactions)
    and 'alert_arrow' (alerts) which are loaded from PyIceberg scans.

    Args:
        args: Parsed CLI arguments.

    Returns:
        SQL query string.
    """
    if args.join_alerts:
        return _build_join_query(args)

    if args.show_alerts:
        return _build_show_alerts_query(args)

    if args.table == "transactions":
        return _build_transactions_query(args)
    else:
        return _build_alerts_query(args)


def _build_transactions_query(args: argparse.Namespace) -> str:
    """Build a query for the transactions table."""
    conditions = []

    if args.date_range:
        conditions.append(
            f"timestamp >= '{args.date_range[0]}' AND timestamp <= '{args.date_range[1]}'"
        )
    if args.account_id:
        conditions.append(f"account_id = '{args.account_id}'")

    where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    return f"SELECT * FROM tx_arrow{where_clause} ORDER BY timestamp DESC LIMIT {args.limit}"


def _build_alerts_query(args: argparse.Namespace) -> str:
    """Build a query for the alerts table."""
    conditions = []

    if args.date_range:
        conditions.append(
            f"alert_timestamp >= '{args.date_range[0]}' AND alert_timestamp <= '{args.date_range[1]}'"
        )

    where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    return f"SELECT * FROM alert_arrow{where_clause} ORDER BY alert_timestamp DESC LIMIT {args.limit}"


def _build_join_query(args: argparse.Namespace) -> str:
    """Build a JOIN query between transactions and alerts."""
    conditions = []

    if args.date_range:
        conditions.append(
            f"t.timestamp >= '{args.date_range[0]}' AND t.timestamp <= '{args.date_range[1]}'"
        )
    if args.account_id:
        conditions.append(f"t.account_id = '{args.account_id}'")

    where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    return (
        f"SELECT t.transaction_id, t.timestamp, t.account_id, t.amount, t.currency, "
        f"t.merchant_name, t.status, a.alert_id, a.rule_name, a.severity, a.description "
        f"FROM tx_arrow t "
        f"JOIN alert_arrow a ON t.transaction_id = a.transaction_id"
        f"{where_clause} "
        f"ORDER BY t.timestamp DESC LIMIT {args.limit}"
    )


def _build_show_alerts_query(args: argparse.Namespace) -> str:
    """Build a query to show alerts for matching transactions."""
    if args.account_id:
        # Show alerts for transactions belonging to the given account
        return (
            f"SELECT a.* FROM alert_arrow a "
            f"JOIN tx_arrow t ON a.transaction_id = t.transaction_id "
            f"WHERE t.account_id = '{args.account_id}' "
            f"ORDER BY a.alert_timestamp DESC LIMIT {args.limit}"
        )
    else:
        return f"SELECT * FROM alert_arrow ORDER BY alert_timestamp DESC LIMIT {args.limit}"


def _resolve_snapshot_id(table, args: argparse.Namespace) -> Optional[int]:
    """Resolve the snapshot ID for time-travel queries.

    Args:
        table: PyIceberg Table instance.
        args: Parsed CLI arguments.

    Returns:
        Snapshot ID if time-travel requested, None otherwise.
    """
    if args.snapshot_id is not None:
        return int(args.snapshot_id)

    if args.as_of_timestamp is not None:
        target_ts = datetime.fromisoformat(
            args.as_of_timestamp.replace("Z", "+00:00")
        )
        target_ms = int(target_ts.timestamp() * 1000)

        # Find the latest snapshot at or before the target timestamp
        best_snapshot = None
        for snapshot in table.snapshots():
            if snapshot.timestamp_ms <= target_ms:
                if best_snapshot is None or snapshot.timestamp_ms > best_snapshot.timestamp_ms:
                    best_snapshot = snapshot

        if best_snapshot is None:
            raise ValueError(
                f"No snapshot found at or before {args.as_of_timestamp}"
            )
        return best_snapshot.snapshot_id

    return None


def _format_output(result, output_format: str) -> str:
    """Format DuckDB query result for output.

    Args:
        result: DuckDB query result relation.
        output_format: One of 'table', 'json', 'csv'.

    Returns:
        Formatted string.
    """
    if output_format == "json":
        rows = result.fetchall()
        columns = [desc[0] for desc in result.description]
        records = []
        for row in rows:
            record = {}
            for col, val in zip(columns, row):
                # Convert non-serializable types
                if hasattr(val, "isoformat"):
                    val = val.isoformat()
                elif isinstance(val, bytes):
                    val = val.decode("utf-8", errors="replace")
                record[col] = val
            records.append(record)
        return json.dumps(records, indent=2, default=str)

    elif output_format == "csv":
        rows = result.fetchall()
        columns = [desc[0] for desc in result.description]
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        writer.writerows(rows)
        return output.getvalue()

    else:  # table format
        return result.fetch_arrow_table().to_pandas().to_string()


def execute_query(args: argparse.Namespace) -> None:
    """Execute the analytics query and display results.

    Connects to the Iceberg REST catalog, loads the requested table(s),
    scans to Arrow, queries via DuckDB, and outputs results.

    Args:
        args: Parsed CLI arguments.
    """
    start_time = time.time()
    query_metadata = {
        "table": args.table,
        "limit": args.limit,
        "has_date_range": args.date_range is not None,
        "has_account_id": args.account_id is not None,
        "join_alerts": args.join_alerts,
        "show_alerts": args.show_alerts,
        "time_travel": args.snapshot_id is not None or args.as_of_timestamp is not None,
        "output_format": args.output_format,
    }

    logger.info("Starting query", extra={"query_metadata": query_metadata})

    catalog = get_catalog(args.catalog_uri)

    # Load transaction table if needed
    tx_arrow = None
    if args.table == "transactions" or args.join_alerts or args.show_alerts:
        tx_table = catalog.load_table(f"{NAMESPACE}.transactions")
        snapshot_id = _resolve_snapshot_id(tx_table, args)
        scan_kwargs = {"snapshot_id": snapshot_id} if snapshot_id else {}
        tx_arrow = tx_table.scan(**scan_kwargs).to_arrow()  # noqa: F841 — used by DuckDB

    # Load alerts table if needed
    alert_arrow = None
    if args.table == "alerts" or args.join_alerts or args.show_alerts:
        alert_table = catalog.load_table(f"{NAMESPACE}.alerts")
        if args.table == "alerts":
            snapshot_id = _resolve_snapshot_id(alert_table, args)
        else:
            snapshot_id = None
        scan_kwargs = {"snapshot_id": snapshot_id} if snapshot_id else {}
        alert_arrow = alert_table.scan(**scan_kwargs).to_arrow()  # noqa: F841 — used by DuckDB

    # Build and execute query
    query = build_query(args)
    logger.debug("Executing SQL", extra={"sql": query})

    result = duckdb.sql(query)
    row_count = result.fetchone()  # peek to check
    # Re-execute since fetchone consumed a row
    result = duckdb.sql(query)

    output = _format_output(result, args.output_format)
    elapsed = time.time() - start_time

    # Re-execute for count
    count_result = duckdb.sql(query).fetchall()
    row_count = len(count_result)

    # T069: Structured logging for analytics queries
    logger.info(
        "Query completed",
        extra={
            "query_type": _describe_query_type(args),
            "execution_time_seconds": round(elapsed, 3),
            "row_count": row_count,
            "table": args.table,
            "time_travel": args.snapshot_id or args.as_of_timestamp or None,
        },
    )

    print(output)
    print(f"\n--- {row_count} row(s) returned in {elapsed:.3f}s ---")


def _describe_query_type(args: argparse.Namespace) -> str:
    """Generate a human-readable description of the query type."""
    parts = [args.table]
    if args.join_alerts:
        parts.append("join-alerts")
    if args.show_alerts:
        parts.append("show-alerts")
    if args.date_range:
        parts.append("date-range")
    if args.account_id:
        parts.append("account-filter")
    if args.snapshot_id:
        parts.append("time-travel-snapshot")
    if args.as_of_timestamp:
        parts.append("time-travel-timestamp")
    return "+".join(parts)


def _configure_logging() -> None:
    """Configure structured JSON logging for the query tool."""
    handler = logging.StreamHandler(sys.stderr)

    class JsonFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            log_entry = {
                "timestamp": datetime.now(tz=timezone.utc).isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            }
            # Include extra fields
            for key in ("query_metadata", "query_type", "execution_time_seconds",
                        "row_count", "table", "time_travel", "sql"):
                if hasattr(record, key):
                    log_entry[key] = getattr(record, key)
            return json.dumps(log_entry)

    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def main() -> None:
    """Entry point for the analytics query CLI."""
    _configure_logging()
    args = parse_args()
    validate_time_travel_args(args)
    execute_query(args)


if __name__ == "__main__":
    main()
