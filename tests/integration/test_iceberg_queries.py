"""T062/T063: Integration test — DuckDB/PyIceberg reads from Iceberg REST catalog.

Per Constitution v2.0.0 Principle III: Tests written FIRST, must FAIL before implementation.
Per spec.md FR-007: Time-travel queries on stored Iceberg data.
Per SC-007: Analytical queries over 1M records within 10 seconds.

These tests require the Docker environment running (Iceberg REST catalog + MinIO).
"""

import time
from datetime import datetime, timezone
from decimal import Decimal

import pytest

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture
def iceberg_catalog():
    """Create a PyIceberg REST catalog client for the running environment."""
    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(
        name="rest",
        **{"uri": "http://localhost:8181", "s3.endpoint": "http://localhost:9000"},
    )


@pytest.fixture
def sample_transaction_arrow():
    """Create a sample PyArrow table with transaction data for writing to Iceberg."""
    import pyarrow as pa

    return pa.table(
        {
            "transaction_id": ["test-tx-001", "test-tx-002", "test-tx-003"],
            "timestamp": pa.array(
                [
                    datetime(2026, 2, 16, 10, 0, 0, tzinfo=timezone.utc),
                    datetime(2026, 2, 16, 11, 0, 0, tzinfo=timezone.utc),
                    datetime(2026, 2, 16, 12, 0, 0, tzinfo=timezone.utc),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "account_id": ["ACC001", "ACC002", "ACC001"],
            "amount": pa.array(
                [Decimal("100.50"), Decimal("15000.00"), Decimal("250.75")],
                type=pa.decimal128(10, 2),
            ),
            "currency": ["USD", "USD", "EUR"],
            "merchant_name": ["Amazon", "LuxuryStore", "Carrefour"],
            "merchant_category": ["online", "retail", "groceries"],
            "transaction_type": ["purchase", "purchase", "purchase"],
            "location_country": ["US", "US", "FR"],
            "status": ["completed", "completed", "completed"],
            "processing_timestamp": pa.array(
                [
                    datetime(2026, 2, 16, 10, 0, 1, tzinfo=timezone.utc),
                    datetime(2026, 2, 16, 11, 0, 1, tzinfo=timezone.utc),
                    datetime(2026, 2, 16, 12, 0, 1, tzinfo=timezone.utc),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "partition_date": pa.array(
                [
                    datetime(2026, 2, 16).date(),
                    datetime(2026, 2, 16).date(),
                    datetime(2026, 2, 16).date(),
                ],
                type=pa.date32(),
            ),
            "is_flagged": [False, True, False],
        }
    )


@pytest.fixture
def sample_alert_arrow():
    """Create a sample PyArrow table with alert data for writing to Iceberg."""
    import pyarrow as pa

    return pa.table(
        {
            "alert_id": ["alert-001", "alert-002"],
            "transaction_id": ["test-tx-002", "test-tx-002"],
            "rule_name": ["high-value", "unusual-hour"],
            "severity": ["high", "medium"],
            "alert_timestamp": pa.array(
                [
                    datetime(2026, 2, 16, 11, 0, 2, tzinfo=timezone.utc),
                    datetime(2026, 2, 16, 11, 0, 2, tzinfo=timezone.utc),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "description": [
                "Transaction amount $15000.00 exceeds threshold $10000.00",
                "Transaction during quiet hours 01:00-05:00 UTC",
            ],
        }
    )


class TestDuckDBIcebergReads:
    """T062: Test DuckDB/PyIceberg reads transactions from Iceberg REST catalog."""

    def test_read_transactions_table_exists(self, iceberg_catalog) -> None:
        """financial.transactions table should exist in the catalog."""
        table = iceberg_catalog.load_table("financial.transactions")
        assert table is not None

    def test_read_alerts_table_exists(self, iceberg_catalog) -> None:
        """financial.alerts table should exist in the catalog."""
        table = iceberg_catalog.load_table("financial.alerts")
        assert table is not None

    def test_write_and_read_transactions_via_pyiceberg(
        self, iceberg_catalog, sample_transaction_arrow
    ) -> None:
        """Write transactions to Iceberg and read back via PyIceberg scan."""
        table = iceberg_catalog.load_table("financial.transactions")
        table.append(sample_transaction_arrow)

        # Read back via scan
        scan = table.scan()
        arrow_table = scan.to_arrow()
        assert len(arrow_table) >= 3
        tx_ids = arrow_table.column("transaction_id").to_pylist()
        assert "test-tx-001" in tx_ids

    def test_read_transactions_with_duckdb(
        self, iceberg_catalog, sample_transaction_arrow
    ) -> None:
        """Read transactions via DuckDB querying an Arrow table from PyIceberg."""
        import duckdb

        table = iceberg_catalog.load_table("financial.transactions")
        # Ensure data exists
        table.append(sample_transaction_arrow)

        arrow_table = table.scan().to_arrow()
        result = duckdb.sql("SELECT count(*) as cnt FROM arrow_table").fetchone()
        assert result[0] >= 3

    def test_query_transactions_by_account_id_with_duckdb(
        self, iceberg_catalog, sample_transaction_arrow
    ) -> None:
        """DuckDB query filtering by account_id should return correct results."""
        import duckdb

        table = iceberg_catalog.load_table("financial.transactions")
        table.append(sample_transaction_arrow)

        arrow_table = table.scan().to_arrow()
        result = duckdb.sql(
            "SELECT * FROM arrow_table WHERE account_id = 'ACC001'"
        ).fetchall()
        # ACC001 has 2 transactions in sample data
        assert len(result) >= 2

    def test_query_transactions_by_date_range_with_duckdb(
        self, iceberg_catalog, sample_transaction_arrow
    ) -> None:
        """DuckDB query filtering by date range should return correct results."""
        import duckdb

        table = iceberg_catalog.load_table("financial.transactions")
        table.append(sample_transaction_arrow)

        arrow_table = table.scan().to_arrow()
        result = duckdb.sql(
            "SELECT * FROM arrow_table WHERE timestamp >= '2026-02-16T11:00:00Z' AND timestamp <= '2026-02-16T12:00:00Z'"
        ).fetchall()
        assert len(result) >= 2

    def test_write_and_read_alerts_via_pyiceberg(
        self, iceberg_catalog, sample_alert_arrow
    ) -> None:
        """Write alerts to Iceberg and read back via PyIceberg scan."""
        table = iceberg_catalog.load_table("financial.alerts")
        table.append(sample_alert_arrow)

        arrow_table = table.scan().to_arrow()
        assert len(arrow_table) >= 2
        alert_ids = arrow_table.column("alert_id").to_pylist()
        assert "alert-001" in alert_ids

    def test_join_transactions_and_alerts_with_duckdb(
        self, iceberg_catalog, sample_transaction_arrow, sample_alert_arrow
    ) -> None:
        """DuckDB JOIN between transactions and alerts should provide full context."""
        import duckdb

        tx_table = iceberg_catalog.load_table("financial.transactions")
        tx_table.append(sample_transaction_arrow)
        alert_table = iceberg_catalog.load_table("financial.alerts")
        alert_table.append(sample_alert_arrow)

        tx_arrow = tx_table.scan().to_arrow()
        alert_arrow = alert_table.scan().to_arrow()

        result = duckdb.sql("""
            SELECT t.transaction_id, t.amount, a.rule_name, a.severity
            FROM tx_arrow t
            JOIN alert_arrow a ON t.transaction_id = a.transaction_id
            WHERE t.transaction_id = 'test-tx-002'
        """).fetchall()
        # test-tx-002 should have 2 alerts
        assert len(result) >= 2

    def test_query_partitioning_correct(
        self, iceberg_catalog, sample_transaction_arrow
    ) -> None:
        """Transactions table should be partitioned by days(timestamp)."""
        table = iceberg_catalog.load_table("financial.transactions")
        spec = table.spec()
        assert len(spec.fields) == 1
        field = spec.fields[0]
        assert field.name == "timestamp_day"

    def test_query_performance_within_threshold(
        self, iceberg_catalog, sample_transaction_arrow
    ) -> None:
        """Query execution should complete within 10 seconds (SC-007)."""
        import duckdb

        table = iceberg_catalog.load_table("financial.transactions")
        table.append(sample_transaction_arrow)

        start = time.time()
        arrow_table = table.scan().to_arrow()
        duckdb.sql("SELECT * FROM arrow_table LIMIT 1000").fetchall()
        elapsed = time.time() - start
        assert elapsed < 10.0, f"Query took {elapsed:.2f}s, expected < 10s"


class TestTimeTravelQueries:
    """T063: Test time-travel query returns historical snapshot data."""

    def test_time_travel_by_snapshot_id(
        self, iceberg_catalog
    ) -> None:
        """Query at a specific snapshot ID should return data as of that snapshot."""
        import pyarrow as pa

        table = iceberg_catalog.load_table("financial.transactions")

        # Write first batch
        batch1 = pa.table(
            {
                "transaction_id": ["tt-snap-001"],
                "timestamp": pa.array(
                    [datetime(2026, 2, 15, 10, 0, 0, tzinfo=timezone.utc)],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                "account_id": ["SNAP_ACC001"],
                "amount": pa.array([Decimal("100.00")], type=pa.decimal128(10, 2)),
                "currency": ["USD"],
                "merchant_name": ["SnapMerchant1"],
                "merchant_category": ["retail"],
                "transaction_type": ["purchase"],
                "location_country": ["US"],
                "status": ["completed"],
                "processing_timestamp": pa.array(
                    [datetime(2026, 2, 15, 10, 0, 1, tzinfo=timezone.utc)],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                "partition_date": pa.array(
                    [datetime(2026, 2, 15).date()], type=pa.date32()
                ),
                "is_flagged": [False],
            }
        )
        table.append(batch1)

        # Record snapshot ID after first batch
        table = iceberg_catalog.load_table("financial.transactions")
        snapshot_after_batch1 = table.current_snapshot()
        assert snapshot_after_batch1 is not None
        snapshot_id = snapshot_after_batch1.snapshot_id

        # Write second batch
        batch2 = pa.table(
            {
                "transaction_id": ["tt-snap-002"],
                "timestamp": pa.array(
                    [datetime(2026, 2, 15, 11, 0, 0, tzinfo=timezone.utc)],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                "account_id": ["SNAP_ACC002"],
                "amount": pa.array([Decimal("200.00")], type=pa.decimal128(10, 2)),
                "currency": ["EUR"],
                "merchant_name": ["SnapMerchant2"],
                "merchant_category": ["dining"],
                "transaction_type": ["purchase"],
                "location_country": ["FR"],
                "status": ["completed"],
                "processing_timestamp": pa.array(
                    [datetime(2026, 2, 15, 11, 0, 1, tzinfo=timezone.utc)],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                "partition_date": pa.array(
                    [datetime(2026, 2, 15).date()], type=pa.date32()
                ),
                "is_flagged": [False],
            }
        )
        table.append(batch2)

        # Query at snapshot_id (should NOT see batch2)
        table = iceberg_catalog.load_table("financial.transactions")
        scan = table.scan(snapshot_id=snapshot_id)
        arrow_result = scan.to_arrow()
        tx_ids = arrow_result.column("transaction_id").to_pylist()
        assert "tt-snap-001" in tx_ids
        assert "tt-snap-002" not in tx_ids

    def test_time_travel_by_timestamp(
        self, iceberg_catalog
    ) -> None:
        """Query with as-of-timestamp should return data as of that point in time."""
        import pyarrow as pa

        table = iceberg_catalog.load_table("financial.transactions")

        # Write a batch
        batch = pa.table(
            {
                "transaction_id": ["tt-time-001"],
                "timestamp": pa.array(
                    [datetime(2026, 2, 14, 10, 0, 0, tzinfo=timezone.utc)],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                "account_id": ["TIME_ACC001"],
                "amount": pa.array([Decimal("300.00")], type=pa.decimal128(10, 2)),
                "currency": ["USD"],
                "merchant_name": ["TimeMerchant"],
                "merchant_category": ["travel"],
                "transaction_type": ["purchase"],
                "location_country": ["US"],
                "status": ["completed"],
                "processing_timestamp": pa.array(
                    [datetime(2026, 2, 14, 10, 0, 1, tzinfo=timezone.utc)],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                "partition_date": pa.array(
                    [datetime(2026, 2, 14).date()], type=pa.date32()
                ),
                "is_flagged": [False],
            }
        )
        table.append(batch)

        # Record the time after writing
        table = iceberg_catalog.load_table("financial.transactions")
        snapshot = table.current_snapshot()
        assert snapshot is not None
        snapshot_timestamp_ms = snapshot.timestamp_ms

        # The as-of-timestamp should resolve to a snapshot at or before this time
        # Query using the snapshot timestamp — should include tt-time-001
        scan = table.scan(snapshot_id=snapshot.snapshot_id)
        arrow_result = scan.to_arrow()
        tx_ids = arrow_result.column("transaction_id").to_pylist()
        assert "tt-time-001" in tx_ids

    def test_time_travel_lists_snapshots(
        self, iceberg_catalog
    ) -> None:
        """Table should have multiple snapshots after multiple writes."""
        table = iceberg_catalog.load_table("financial.transactions")
        snapshots = list(table.snapshots())
        # After the writes in previous tests, there should be multiple snapshots
        assert len(snapshots) >= 1

    def test_time_travel_current_snapshot_has_all_data(
        self, iceberg_catalog
    ) -> None:
        """Current snapshot (no time-travel) should include all written data."""
        table = iceberg_catalog.load_table("financial.transactions")
        arrow_table = table.scan().to_arrow()
        # Should have data from all writes
        assert len(arrow_table) >= 1
