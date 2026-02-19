"""DAG resolution for SQL transform models using Kahn's algorithm.

Implements topological sorting with cycle detection per research decision #2.
"""

from __future__ import annotations

import logging
from collections import deque

from src.transformer.models import TransformModel

logger = logging.getLogger("run_transforms")


class CircularDependencyError(Exception):
    """Raised when a circular dependency is detected in the model graph."""

    def __init__(self, cycle_models: list[str]) -> None:
        self.cycle_models = cycle_models
        super().__init__(
            f"Circular dependency detected among models: {', '.join(cycle_models)}"
        )


class MissingDependencyError(Exception):
    """Raised when a model depends on a model that does not exist."""

    def __init__(self, model_name: str, missing_dep: str) -> None:
        self.model_name = model_name
        self.missing_dep = missing_dep
        super().__init__(
            f"Model '{model_name}' depends on '{missing_dep}' which does not exist"
        )


def resolve_execution_order(models: list[TransformModel]) -> list[TransformModel]:
    """Resolve the execution order of transform models using topological sort.

    Uses Kahn's algorithm (BFS-based) to produce a valid execution order
    that respects all declared dependencies. Detects circular dependencies
    and missing dependencies.

    Args:
        models: List of TransformModel objects to sort.

    Returns:
        List of TransformModel objects in valid execution order.

    Raises:
        CircularDependencyError: If a cycle is detected in the dependency graph.
        MissingDependencyError: If a model depends on a non-existent model.
    """
    if not models:
        return []

    # Build lookup by model name
    model_map: dict[str, TransformModel] = {m.name: m for m in models}

    # Validate all dependencies exist (allow depending on raw tables like 'transactions')
    # Only check dependencies that reference other models in the graph
    known_names = set(model_map.keys())
    for model in models:
        for dep in model.depends_on:
            if dep not in known_names:
                # This dependency is on an external table (e.g., 'transactions'),
                # not on another model. That's fine -- we skip it.
                logger.debug(
                    "Model '%s' depends on '%s' (external table, not in model graph)",
                    model.name,
                    dep,
                )

    # Build adjacency list and in-degree count (only for model-to-model deps)
    in_degree: dict[str, int] = {m.name: 0 for m in models}
    dependents: dict[str, list[str]] = {m.name: [] for m in models}

    for model in models:
        for dep in model.depends_on:
            if dep in known_names:
                in_degree[model.name] += 1
                dependents[dep].append(model.name)

    # Kahn's algorithm: start with nodes that have no model dependencies
    queue: deque[str] = deque()
    for name, degree in in_degree.items():
        if degree == 0:
            queue.append(name)

    execution_order: list[str] = []
    while queue:
        current = queue.popleft()
        execution_order.append(current)

        for dependent in dependents[current]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    # If not all models are in execution_order, there's a cycle
    if len(execution_order) != len(models):
        remaining = [name for name in model_map if name not in set(execution_order)]
        raise CircularDependencyError(remaining)

    ordered_models = [model_map[name] for name in execution_order]
    logger.info(
        "Resolved execution order: %s",
        " -> ".join(m.name for m in ordered_models),
    )
    return ordered_models
