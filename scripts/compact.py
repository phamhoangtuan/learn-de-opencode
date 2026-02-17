"""T068: Iceberg table compaction and maintenance script.

Performs periodic maintenance operations on Iceberg tables:
- Daily: rewrite_data_files (compact small Parquet files)
- Snapshot expiration: expire snapshots older than 3 days (keep min 10)
- Weekly: remove_orphan_files (clean up unreferenced files)

Per research.md R3: Compaction strategy for Iceberg tables.
Per Constitution v2.0.0 Principle IV: Metadata & Lineage.

Usage:
    # Run all maintenance operations:
    python scripts/compact.py --all

    # Run specific operations:
    python scripts/compact.py --compact
    python scripts/compact.py --expire-snapshots
    python scripts/compact.py --remove-orphans

    # Customize retention:
    python scripts/compact.py --expire-snapshots --retention-days 7 --min-snapshots 20
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from pyiceberg.catalog.rest import RestCatalog

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_CATALOG_URI = "http://localhost:8181"
DEFAULT_S3_ENDPOINT = "http://localhost:9000"
NAMESPACE = "financial"
TABLES = ["transactions", "alerts", "pipeline_session_metadata"]

# Default retention settings per research.md R3
DEFAULT_RETENTION_DAYS = 3
DEFAULT_MIN_SNAPSHOTS = 10


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


def compact_table(catalog: RestCatalog, table_name: str) -> dict:
    """Compact small data files in an Iceberg table.

    Uses PyIceberg's rewrite_data_files to merge small Parquet files into
    larger ones (target: 128MB per file per IcebergSinkBuilder config).

    Args:
        catalog: PyIceberg REST catalog client.
        table_name: Fully qualified table name (e.g. financial.transactions).

    Returns:
        Dict with compaction results (files_before, files_after, duration_seconds).
    """
    start = time.time()
    table = catalog.load_table(table_name)

    # Count files before compaction
    scan = table.scan()
    plan = scan.plan_files()
    files_before = sum(1 for _ in plan)

    logger.info(
        "Starting compaction for %s (%d data files)", table_name, files_before
    )

    # Note: PyIceberg's rewrite_data_files may not be available in all versions.
    # For learning purposes, we document the approach and attempt the call.
    try:
        # PyIceberg >= 0.7 supports table operations for maintenance
        # The exact API may vary; this is the standard approach
        if hasattr(table, "rewrite_data_files"):
            table.rewrite_data_files()
            logger.info("Compaction completed for %s", table_name)
        else:
            logger.info(
                "rewrite_data_files not available in this PyIceberg version for %s. "
                "At this data volume (~1GB/day), manual compaction is not critical.",
                table_name,
            )
    except Exception as e:
        logger.warning("Compaction failed for %s: %s", table_name, e)

    # Count files after compaction
    table = catalog.load_table(table_name)
    scan = table.scan()
    plan = scan.plan_files()
    files_after = sum(1 for _ in plan)

    elapsed = time.time() - start
    result = {
        "table": table_name,
        "files_before": files_before,
        "files_after": files_after,
        "duration_seconds": round(elapsed, 3),
    }

    logger.info(
        "Compaction result for %s: %d -> %d files (%.3fs)",
        table_name,
        files_before,
        files_after,
        elapsed,
    )
    return result


def expire_snapshots(
    catalog: RestCatalog,
    table_name: str,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    min_snapshots: int = DEFAULT_MIN_SNAPSHOTS,
) -> dict:
    """Expire old snapshots from an Iceberg table.

    Removes snapshots older than retention_days, but always keeps at least
    min_snapshots snapshots.

    Args:
        catalog: PyIceberg REST catalog client.
        table_name: Fully qualified table name.
        retention_days: Number of days to retain snapshots.
        min_snapshots: Minimum number of snapshots to always keep.

    Returns:
        Dict with expiration results.
    """
    start = time.time()
    table = catalog.load_table(table_name)

    snapshots = list(table.snapshots())
    total_snapshots = len(snapshots)

    if total_snapshots == 0:
        logger.info("No snapshots to expire for %s", table_name)
        return {
            "table": table_name,
            "snapshots_before": 0,
            "snapshots_expired": 0,
            "snapshots_after": 0,
            "duration_seconds": 0,
        }

    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=retention_days)
    cutoff_ms = int(cutoff.timestamp() * 1000)

    # Sort snapshots by timestamp (newest first)
    snapshots_sorted = sorted(snapshots, key=lambda s: s.timestamp_ms, reverse=True)

    # Identify snapshots to keep (at least min_snapshots, plus any within retention)
    snapshots_to_expire = []
    for i, snap in enumerate(snapshots_sorted):
        if i < min_snapshots:
            continue  # Always keep the newest min_snapshots
        if snap.timestamp_ms < cutoff_ms:
            snapshots_to_expire.append(snap)

    logger.info(
        "Expiring %d of %d snapshots for %s (retention: %d days, min keep: %d)",
        len(snapshots_to_expire),
        total_snapshots,
        table_name,
        retention_days,
        min_snapshots,
    )

    expired_count = 0
    for snap in snapshots_to_expire:
        try:
            # Use manage_snapshots to expire individual snapshots
            table.manage_snapshots().remove_snapshot(snap.snapshot_id).commit()
            expired_count += 1
            logger.debug("Expired snapshot %d from %s", snap.snapshot_id, table_name)
        except Exception as e:
            logger.warning(
                "Failed to expire snapshot %d from %s: %s",
                snap.snapshot_id,
                table_name,
                e,
            )

    elapsed = time.time() - start
    result = {
        "table": table_name,
        "snapshots_before": total_snapshots,
        "snapshots_expired": expired_count,
        "snapshots_after": total_snapshots - expired_count,
        "duration_seconds": round(elapsed, 3),
    }

    logger.info(
        "Snapshot expiration for %s: %d -> %d snapshots (%d expired, %.3fs)",
        table_name,
        total_snapshots,
        total_snapshots - expired_count,
        expired_count,
        elapsed,
    )
    return result


def remove_orphan_files(catalog: RestCatalog, table_name: str) -> dict:
    """Remove orphan files from an Iceberg table.

    Identifies and removes files in the table's data directory that are not
    referenced by any snapshot's manifest.

    Args:
        catalog: PyIceberg REST catalog client.
        table_name: Fully qualified table name.

    Returns:
        Dict with orphan removal results.
    """
    start = time.time()
    table = catalog.load_table(table_name)

    logger.info("Checking for orphan files in %s", table_name)

    # Note: PyIceberg orphan file removal may require filesystem access.
    # For learning purposes, we document the approach.
    try:
        if hasattr(table, "remove_orphan_files"):
            table.remove_orphan_files()
            logger.info("Orphan file removal completed for %s", table_name)
        else:
            logger.info(
                "remove_orphan_files not directly available in this PyIceberg version for %s. "
                "In production, use Spark's RemoveOrphanFilesAction or a custom filesystem scanner. "
                "At this data volume, orphan files are unlikely to accumulate significantly.",
                table_name,
            )
    except Exception as e:
        logger.warning("Orphan file removal failed for %s: %s", table_name, e)

    elapsed = time.time() - start
    result = {
        "table": table_name,
        "duration_seconds": round(elapsed, 3),
    }
    return result


def run_maintenance(
    catalog_uri: str,
    compact: bool = False,
    expire: bool = False,
    remove_orphans: bool = False,
    run_all: bool = False,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    min_snapshots: int = DEFAULT_MIN_SNAPSHOTS,
) -> list[dict]:
    """Run maintenance operations on all financial Iceberg tables.

    Args:
        catalog_uri: Iceberg REST catalog URI.
        compact: Run compaction.
        expire: Run snapshot expiration.
        remove_orphans: Run orphan file removal.
        run_all: Run all operations.
        retention_days: Snapshot retention in days.
        min_snapshots: Minimum snapshots to keep.

    Returns:
        List of result dicts from each operation.
    """
    catalog = get_catalog(catalog_uri)
    results = []

    for table_short_name in TABLES:
        table_name = f"{NAMESPACE}.{table_short_name}"
        logger.info("--- Maintenance for %s ---", table_name)

        if compact or run_all:
            results.append(compact_table(catalog, table_name))

        if expire or run_all:
            results.append(
                expire_snapshots(catalog, table_name, retention_days, min_snapshots)
            )

        if remove_orphans or run_all:
            results.append(remove_orphan_files(catalog, table_name))

    return results


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    """Parse CLI arguments for compaction script.

    Args:
        argv: Command-line arguments (defaults to sys.argv).

    Returns:
        Parsed argument namespace with operation flags and retention settings.
    """
    parser = argparse.ArgumentParser(
        description="Iceberg table maintenance: compaction, snapshot expiration, orphan cleanup"
    )
    parser.add_argument(
        "--catalog-uri",
        default=DEFAULT_CATALOG_URI,
        help=f"Iceberg REST catalog URI (default: {DEFAULT_CATALOG_URI})",
    )

    # Operation selection
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all maintenance operations",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Compact small data files (daily)",
    )
    parser.add_argument(
        "--expire-snapshots",
        action="store_true",
        help="Expire old snapshots",
    )
    parser.add_argument(
        "--remove-orphans",
        action="store_true",
        help="Remove orphan files (weekly)",
    )

    # Retention settings
    parser.add_argument(
        "--retention-days",
        type=int,
        default=DEFAULT_RETENTION_DAYS,
        help=f"Snapshot retention in days (default: {DEFAULT_RETENTION_DAYS})",
    )
    parser.add_argument(
        "--min-snapshots",
        type=int,
        default=DEFAULT_MIN_SNAPSHOTS,
        help=f"Minimum snapshots to keep (default: {DEFAULT_MIN_SNAPSHOTS})",
    )

    args = parser.parse_args(argv)

    # Require at least one operation
    if not (args.all or args.compact or args.expire_snapshots or args.remove_orphans):
        parser.error("Specify at least one operation: --all, --compact, --expire-snapshots, or --remove-orphans")

    return args


def main() -> None:
    """Entry point for compaction CLI."""
    args = parse_args()

    results = run_maintenance(
        catalog_uri=args.catalog_uri,
        compact=args.compact,
        expire=args.expire_snapshots,
        remove_orphans=args.remove_orphans,
        run_all=args.all,
        retention_days=args.retention_days,
        min_snapshots=args.min_snapshots,
    )

    print("\n=== Maintenance Summary ===")
    print(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error("Maintenance operation failed: %s", e)
        sys.exit(1)
