"""Pipeline orchestration module for the learn_de project.

Wires all 5 pipeline steps (generate, ingest, transforms, checks, dashboard)
into a single runnable DAG using the transformer.graph module for topological
ordering and DuckDB for pipeline run metadata.
"""
