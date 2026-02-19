"""Unit tests for DAG resolution (topological sort and cycle detection)."""

from __future__ import annotations

import pytest

from src.transformer.graph import (
    CircularDependencyError,
    resolve_execution_order,
)
from src.transformer.models import TransformModel


def _model(name: str, depends_on: list[str] | None = None) -> TransformModel:
    """Helper to create a TransformModel for testing."""
    return TransformModel(
        name=name,
        file_path=f"/fake/{name}.sql",
        sql=f"CREATE OR REPLACE VIEW {name} AS SELECT 1;",
        depends_on=depends_on or [],
    )


class TestResolveExecutionOrder:
    """Tests for resolve_execution_order function."""

    def test_empty_list(self) -> None:
        """Empty input returns empty output."""
        assert resolve_execution_order([]) == []

    def test_single_model_no_deps(self) -> None:
        """Single model with no dependencies returns itself."""
        models = [_model("a")]
        result = resolve_execution_order(models)
        assert [m.name for m in result] == ["a"]

    def test_linear_chain(self) -> None:
        """Linear dependency chain: a -> b -> c produces correct order."""
        models = [
            _model("c", ["b"]),
            _model("b", ["a"]),
            _model("a"),
        ]
        result = resolve_execution_order(models)
        names = [m.name for m in result]
        assert names.index("a") < names.index("b")
        assert names.index("b") < names.index("c")

    def test_diamond_dependency(self) -> None:
        """Diamond pattern: a -> b, a -> c, b -> d, c -> d."""
        models = [
            _model("d", ["b", "c"]),
            _model("b", ["a"]),
            _model("c", ["a"]),
            _model("a"),
        ]
        result = resolve_execution_order(models)
        names = [m.name for m in result]
        assert names.index("a") < names.index("b")
        assert names.index("a") < names.index("c")
        assert names.index("b") < names.index("d")
        assert names.index("c") < names.index("d")

    def test_independent_models(self) -> None:
        """Models with no dependencies can be in any order."""
        models = [_model("a"), _model("b"), _model("c")]
        result = resolve_execution_order(models)
        assert len(result) == 3
        assert {m.name for m in result} == {"a", "b", "c"}

    def test_external_dependency_ignored(self) -> None:
        """Dependencies on external tables (not in graph) are allowed."""
        models = [
            _model("stg", ["transactions"]),  # 'transactions' is external
            _model("mart", ["stg"]),
        ]
        result = resolve_execution_order(models)
        names = [m.name for m in result]
        assert names.index("stg") < names.index("mart")

    def test_circular_dependency_raises(self) -> None:
        """Circular dependency is detected and raises error."""
        models = [
            _model("a", ["b"]),
            _model("b", ["a"]),
        ]
        with pytest.raises(CircularDependencyError) as exc_info:
            resolve_execution_order(models)
        assert set(exc_info.value.cycle_models) == {"a", "b"}

    def test_circular_three_node_cycle(self) -> None:
        """Three-node cycle is detected."""
        models = [
            _model("a", ["c"]),
            _model("b", ["a"]),
            _model("c", ["b"]),
        ]
        with pytest.raises(CircularDependencyError):
            resolve_execution_order(models)

    def test_real_world_graph(self) -> None:
        """Simulates the actual 003 transform graph."""
        models = [
            _model("stg_transactions", ["transactions"]),
            _model("daily_spend_by_category", ["stg_transactions"]),
            _model("monthly_account_summary", ["stg_transactions"]),
        ]
        result = resolve_execution_order(models)
        names = [m.name for m in result]
        assert names[0] == "stg_transactions"
        # Both marts come after staging, order between them is flexible
        assert set(names[1:]) == {"daily_spend_by_category", "monthly_account_summary"}
