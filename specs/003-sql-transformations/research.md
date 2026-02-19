# Research: SQL Transformations Layer

**Feature**: 003-sql-transformations  
**Date**: 2026-02-19

## Research Questions

### 1. How should SQL transform files declare dependencies?

**Options Evaluated**:
- **A) Comment-based `-- depends_on:` header** -- Simple, language-agnostic, parseable with regex
- **B) YAML frontmatter** -- More structured but requires YAML parser, non-standard for SQL
- **C) Filename-based implicit ordering** -- No explicit declaration, fragile
- **D) Separate manifest file** -- Centralized but creates sync issues with SQL files

**Decision**: **Option A** -- Comment-based `-- depends_on:` header. This is the simplest approach that keeps dependency metadata co-located with the SQL it describes. Multiple dependencies use comma separation: `-- depends_on: stg_transactions, other_model`. Models with no dependencies omit the header entirely. This aligns with dbt's convention and is trivially parseable.

**Format**:
```sql
-- model: stg_transactions
-- depends_on: transactions
CREATE OR REPLACE VIEW stg_transactions AS
SELECT ...
```

The `-- model:` header declares the output name. The `-- depends_on:` header lists upstream dependencies. Both are optional: model name defaults to the filename (without layer prefix and `.sql` extension), and missing depends_on means no dependencies (root model).

### 2. How should the dependency graph be resolved?

**Options Evaluated**:
- **A) Kahn's algorithm (BFS topological sort)** -- Standard, detects cycles, O(V+E)
- **B) DFS-based topological sort** -- Also standard, slightly more complex cycle detection
- **C) Simple level-based ordering** -- Assign levels and sort, less flexible

**Decision**: **Option A** -- Kahn's algorithm. It naturally produces a topological ordering while detecting cycles (if any nodes remain after processing, there's a cycle). It's well-understood, has linear time complexity, and is straightforward to implement in Python using collections.deque and a dictionary of in-degrees.

### 3. Should the runner use DuckDB transactions for atomicity?

**Options Evaluated**:
- **A) Each model in its own implicit transaction** -- DuckDB auto-commits each statement
- **B) All models in a single transaction** -- Atomic all-or-nothing execution
- **C) Per-model explicit transactions with savepoints** -- Granular rollback on failure

**Decision**: **Option A** -- Each model executes as its own statement with auto-commit. This aligns with the full-refresh idempotency model: if a model fails, the previous successful models' outputs persist, and re-running fixes the failed model. Wrapping everything in a single transaction would roll back successful models when one fails, which contradicts FR-015 (continue executing independent models after failure).

### 4. What naming convention for SQL files?

**Options Evaluated**:
- **A) `<layer>__<model_name>.sql`** -- e.g., `staging__stg_transactions.sql`
- **B) `<number>_<model_name>.sql`** -- e.g., `01_stg_transactions.sql`
- **C) `<model_name>.sql`** -- e.g., `stg_transactions.sql`

**Decision**: **Option A** -- Layer-prefixed with double underscore separator. The layer prefix (`staging__`, `mart__`) provides visual organization in a flat directory and makes it easy to identify model types at a glance. The double underscore avoids ambiguity with model names that contain single underscores. The model name portion (after `__`) determines the output table/view name.

### 5. How to determine output type (VIEW vs TABLE)?

**Options Evaluated**:
- **A) Parse from SQL content** -- Detect `CREATE OR REPLACE VIEW` vs `CREATE OR REPLACE TABLE`
- **B) Convention-based** -- staging = VIEW, mart = TABLE
- **C) Explicit header comment** -- `-- materialized: view|table`

**Decision**: **Option A** -- Parse from SQL content. The SQL file already must contain the full CREATE statement per FR-006, so the output type is inherent in the SQL. No additional metadata needed. The runner does not need to know the type; it just executes the SQL.

## Technology Decisions Summary

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Dependency declaration | `-- depends_on:` comment | Simple, co-located, dbt-aligned |
| Graph resolution | Kahn's algorithm | Standard, cycle detection built-in |
| Transaction model | Per-model auto-commit | Supports partial success (FR-015) |
| File naming | `<layer>__<model_name>.sql` | Visual organization, unambiguous |
| Output type detection | Parse from SQL content | No redundant metadata needed |
| CLI framework | argparse (PEP 723 inline deps) | Consistent with Feature 001/002 pattern |
