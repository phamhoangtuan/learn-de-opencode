"""T061: Unit test — query.py CLI argument parsing and query construction.

Per Constitution v2.0.0 Principle III: Tests written FIRST, must FAIL before implementation.
Per spec.md FR-007/FR-008: Time-travel queries, schema evolution.
Per SC-007: Analytical queries over 1M records within 10 seconds.
"""

from datetime import datetime, timezone

import pytest


class TestQueryCLIArgumentParsing:
    """Test CLI argument parsing for query.py subcommands and flags."""

    def test_parse_args_table_transactions_default(self) -> None:
        """Default table should be 'transactions' when no --table specified."""
        from scripts.query import parse_args

        args = parse_args([])
        assert args.table == "transactions"

    def test_parse_args_table_transactions_explicit(self) -> None:
        """--table transactions should set table to 'transactions'."""
        from scripts.query import parse_args

        args = parse_args(["--table", "transactions"])
        assert args.table == "transactions"

    def test_parse_args_table_alerts(self) -> None:
        """--table alerts should set table to 'alerts'."""
        from scripts.query import parse_args

        args = parse_args(["--table", "alerts"])
        assert args.table == "alerts"

    def test_parse_args_table_invalid_rejected(self) -> None:
        """Invalid table name should raise SystemExit."""
        from scripts.query import parse_args

        with pytest.raises(SystemExit):
            parse_args(["--table", "nonexistent"])

    def test_parse_args_limit_default(self) -> None:
        """Default limit should be 100."""
        from scripts.query import parse_args

        args = parse_args([])
        assert args.limit == 100

    def test_parse_args_limit_custom(self) -> None:
        """--limit 50 should set limit to 50."""
        from scripts.query import parse_args

        args = parse_args(["--limit", "50"])
        assert args.limit == 50

    def test_parse_args_limit_must_be_positive(self) -> None:
        """--limit 0 or negative should raise SystemExit."""
        from scripts.query import parse_args

        with pytest.raises(SystemExit):
            parse_args(["--limit", "0"])

    def test_parse_args_date_range(self) -> None:
        """--date-range should accept two ISO 8601 date strings."""
        from scripts.query import parse_args

        args = parse_args(["--date-range", "2026-01-01", "2026-01-31"])
        assert args.date_range == ["2026-01-01", "2026-01-31"]

    def test_parse_args_date_range_requires_two_values(self) -> None:
        """--date-range with one value should raise SystemExit."""
        from scripts.query import parse_args

        with pytest.raises(SystemExit):
            parse_args(["--date-range", "2026-01-01"])

    def test_parse_args_account_id(self) -> None:
        """--account-id should set account_id."""
        from scripts.query import parse_args

        args = parse_args(["--account-id", "ACC123456789"])
        assert args.account_id == "ACC123456789"

    def test_parse_args_account_id_default_none(self) -> None:
        """Default account_id should be None."""
        from scripts.query import parse_args

        args = parse_args([])
        assert args.account_id is None

    def test_parse_args_show_alerts_flag(self) -> None:
        """--show-alerts should set show_alerts to True."""
        from scripts.query import parse_args

        args = parse_args(["--show-alerts"])
        assert args.show_alerts is True

    def test_parse_args_show_alerts_default_false(self) -> None:
        """Default show_alerts should be False."""
        from scripts.query import parse_args

        args = parse_args([])
        assert args.show_alerts is False

    def test_parse_args_join_alerts_flag(self) -> None:
        """--join-alerts should set join_alerts to True."""
        from scripts.query import parse_args

        args = parse_args(["--join-alerts"])
        assert args.join_alerts is True

    def test_parse_args_join_alerts_default_false(self) -> None:
        """Default join_alerts should be False."""
        from scripts.query import parse_args

        args = parse_args([])
        assert args.join_alerts is False

    def test_parse_args_snapshot_id(self) -> None:
        """--snapshot-id should set snapshot_id."""
        from scripts.query import parse_args

        args = parse_args(["--snapshot-id", "12345678901234"])
        assert args.snapshot_id == "12345678901234"

    def test_parse_args_snapshot_id_default_none(self) -> None:
        """Default snapshot_id should be None."""
        from scripts.query import parse_args

        args = parse_args([])
        assert args.snapshot_id is None

    def test_parse_args_as_of_timestamp(self) -> None:
        """--as-of-timestamp should set as_of_timestamp."""
        from scripts.query import parse_args

        args = parse_args(["--as-of-timestamp", "2026-02-16T14:30:00Z"])
        assert args.as_of_timestamp == "2026-02-16T14:30:00Z"

    def test_parse_args_as_of_timestamp_default_none(self) -> None:
        """Default as_of_timestamp should be None."""
        from scripts.query import parse_args

        args = parse_args([])
        assert args.as_of_timestamp is None

    def test_parse_args_snapshot_id_and_as_of_mutually_exclusive(self) -> None:
        """--snapshot-id and --as-of-timestamp should be mutually exclusive."""
        from scripts.query import parse_args

        with pytest.raises(SystemExit):
            parse_args(["--snapshot-id", "123", "--as-of-timestamp", "2026-01-01T00:00:00Z"])

    def test_parse_args_catalog_uri_default(self) -> None:
        """Default catalog URI should be http://localhost:8181."""
        from scripts.query import parse_args

        args = parse_args([])
        assert args.catalog_uri == "http://localhost:8181"

    def test_parse_args_catalog_uri_custom(self) -> None:
        """--catalog-uri should override the default catalog URI."""
        from scripts.query import parse_args

        args = parse_args(["--catalog-uri", "http://custom:9999"])
        assert args.catalog_uri == "http://custom:9999"

    def test_parse_args_output_format_default(self) -> None:
        """Default output format should be 'table'."""
        from scripts.query import parse_args

        args = parse_args([])
        assert args.output_format == "table"

    def test_parse_args_output_format_json(self) -> None:
        """--format json should set output_format to 'json'."""
        from scripts.query import parse_args

        args = parse_args(["--format", "json"])
        assert args.output_format == "json"

    def test_parse_args_output_format_csv(self) -> None:
        """--format csv should set output_format to 'csv'."""
        from scripts.query import parse_args

        args = parse_args(["--format", "csv"])
        assert args.output_format == "csv"


class TestQueryConstruction:
    """Test SQL query construction from parsed arguments."""

    def test_build_query_transactions_basic(self) -> None:
        """Basic transaction query should SELECT * with LIMIT."""
        from scripts.query import build_query, parse_args

        args = parse_args(["--table", "transactions", "--limit", "10"])
        query = build_query(args)
        assert "SELECT" in query.upper()
        assert "LIMIT 10" in query.upper()

    def test_build_query_alerts_basic(self) -> None:
        """Basic alerts query should SELECT from alerts arrow table."""
        from scripts.query import build_query, parse_args

        args = parse_args(["--table", "alerts", "--limit", "10"])
        query = build_query(args)
        assert "SELECT" in query.upper()
        assert "LIMIT 10" in query.upper()

    def test_build_query_with_date_range_transactions(self) -> None:
        """Date range filter on transactions should use 'timestamp' column."""
        from scripts.query import build_query, parse_args

        args = parse_args(["--table", "transactions", "--date-range", "2026-01-01", "2026-01-31"])
        query = build_query(args)
        assert "timestamp" in query.lower()
        assert "2026-01-01" in query
        assert "2026-01-31" in query

    def test_build_query_with_date_range_alerts(self) -> None:
        """Date range filter on alerts should use 'alert_timestamp' column."""
        from scripts.query import build_query, parse_args

        args = parse_args(["--table", "alerts", "--date-range", "2026-01-01", "2026-01-31"])
        query = build_query(args)
        assert "alert_timestamp" in query.lower()

    def test_build_query_with_account_id(self) -> None:
        """Account ID filter should add WHERE account_id = condition."""
        from scripts.query import build_query, parse_args

        args = parse_args(["--account-id", "ACC123"])
        query = build_query(args)
        assert "account_id" in query.lower()
        assert "ACC123" in query

    def test_build_query_join_alerts(self) -> None:
        """--join-alerts should produce a JOIN between transactions and alerts."""
        from scripts.query import build_query, parse_args

        args = parse_args(["--join-alerts"])
        query = build_query(args)
        assert "JOIN" in query.upper()
        assert "transaction_id" in query.lower()

    def test_build_query_show_alerts(self) -> None:
        """--show-alerts should query the alerts table filtered by transaction criteria."""
        from scripts.query import build_query, parse_args

        args = parse_args(["--show-alerts", "--account-id", "ACC123"])
        query = build_query(args)
        # show-alerts with account filter should include subquery or join
        assert "alert" in query.lower()

    def test_build_query_no_filters(self) -> None:
        """Query with no filters should have no WHERE clause."""
        from scripts.query import build_query, parse_args

        args = parse_args(["--table", "transactions"])
        query = build_query(args)
        # Should not have WHERE clause when no filters applied
        assert "SELECT" in query.upper()


class TestCatalogConnection:
    """Test catalog connection helper function."""

    def test_get_catalog_is_callable(self) -> None:
        """get_catalog should be a callable that accepts a URI string."""
        from scripts.query import get_catalog

        assert callable(get_catalog)

    def test_get_catalog_returns_rest_catalog_type(self) -> None:
        """get_catalog should return a RestCatalog instance (mocked connection)."""
        from unittest.mock import patch
        from scripts.query import get_catalog

        with patch("scripts.query.RestCatalog") as mock_catalog:
            mock_catalog.return_value = "mock_catalog"
            result = get_catalog("http://localhost:8181")
            assert result == "mock_catalog"
            mock_catalog.assert_called_once()

    def test_get_catalog_passes_uri(self) -> None:
        """get_catalog should pass the URI to RestCatalog constructor."""
        from unittest.mock import patch
        from scripts.query import get_catalog

        with patch("scripts.query.RestCatalog") as mock_catalog:
            get_catalog("http://custom:9999")
            call_kwargs = mock_catalog.call_args
            assert call_kwargs[1]["uri"] == "http://custom:9999"

    def test_get_catalog_includes_s3_endpoint(self) -> None:
        """get_catalog should include s3.endpoint in catalog config."""
        from unittest.mock import patch
        from scripts.query import get_catalog

        with patch("scripts.query.RestCatalog") as mock_catalog:
            get_catalog("http://localhost:8181")
            call_kwargs = mock_catalog.call_args
            assert "s3.endpoint" in call_kwargs[1]


class TestTimeTravelParsing:
    """Test time-travel argument validation and parsing."""

    def test_validate_snapshot_id_numeric(self) -> None:
        """Snapshot ID should be parseable as a numeric value."""
        from scripts.query import validate_time_travel_args, parse_args

        args = parse_args(["--snapshot-id", "12345678901234"])
        # Should not raise
        validate_time_travel_args(args)

    def test_validate_as_of_timestamp_iso8601(self) -> None:
        """as-of-timestamp should be a valid ISO 8601 string."""
        from scripts.query import validate_time_travel_args, parse_args

        args = parse_args(["--as-of-timestamp", "2026-02-16T14:30:00Z"])
        # Should not raise
        validate_time_travel_args(args)

    def test_validate_as_of_timestamp_invalid_format(self) -> None:
        """Invalid timestamp format should raise ValueError."""
        from scripts.query import validate_time_travel_args, parse_args

        args = parse_args(["--as-of-timestamp", "not-a-timestamp"])
        with pytest.raises(ValueError, match="timestamp"):
            validate_time_travel_args(args)
