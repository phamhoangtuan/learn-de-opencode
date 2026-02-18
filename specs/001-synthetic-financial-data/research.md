# Research: Synthetic Financial Transaction Data Generator

**Feature**: 001-synthetic-financial-data
**Date**: 2026-02-18

## Research Areas

### 1. uv Inline Script Dependencies (PEP 723)

**Decision**: Use PEP 723 inline metadata in the generator script header.

**Rationale**: `uv run` supports `# /// script` metadata blocks that declare
dependencies directly in the Python file. This eliminates the need for a
separate `requirements.txt` or `pyproject.toml` for running the generator.
Users only need `uv` installed.

**Alternatives considered**:
- `pyproject.toml` with `uv run -m`: Requires project setup, heavier.
- `requirements.txt` with `pip install`: Requires virtual env management.
- Single-file with `pip install` inline: Non-standard, fragile.

**Example**:
```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "polars",
#     "numpy",
#     "faker",
# ]
# ///
```

### 2. Data Processing Library Choice

**Decision**: Use Polars for data generation and output.

**Rationale**: Polars provides fast columnar operations, native Parquet/CSV
write support, and handles large datasets efficiently with lazy evaluation.
For 10M+ records, Polars' streaming write capabilities prevent memory
exhaustion. Polars also produces deterministic output with consistent column
ordering, supporting the reproducibility requirement (SC-003).

**Alternatives considered**:
- Pandas: Slower for large datasets, higher memory usage. Would work for
  < 1M rows but struggles at scale.
- DuckDB: Excellent for SQL-based transforms but overkill for pure generation.
  Better suited for downstream analytics.
- Raw Python + PyArrow: More control but significantly more code for the
  same result.

### 3. Realistic Amount Distribution

**Decision**: Use NumPy's log-normal distribution for transaction amounts.

**Rationale**: Real financial transaction data follows a right-skewed
distribution where most transactions are small (grocery, coffee) and few
are large (rent, electronics). Log-normal distribution with parameters
mu=3.0, sigma=1.5 produces amounts centered around $20-50 with a long
tail up to $5,000+. Currency-specific scaling adjusts for exchange rates
(e.g., JPY amounts are ~100x USD).

**Alternatives considered**:
- Normal distribution: Symmetric, unrealistic for financial data.
- Uniform random: Even less realistic, no clustering.
- Pareto distribution: Too extreme a tail for typical consumer transactions.
- Mixture model: More realistic but over-engineered for synthetic data.

### 4. Merchant & Category Reference Data

**Decision**: Embed 50-100 merchants across 18 spending categories as a
JSON reference file in `src/data/merchants.json`.

**Rationale**: A static reference file is simple, version-controlled, and
deterministic. Categories are weighted by real-world spending patterns
(Groceries ~18%, Dining ~12%, Transport ~10%, etc.). Each merchant maps to
exactly one category, ensuring consistent merchant-category pairings.

**Alternatives considered**:
- Faker providers: Random names lack category mapping and realism.
- External API: Defeats the purpose of removing API dependencies.
- YAML config: JSON is simpler to load and validate programmatically.

### 5. Timestamp Generation Strategy

**Decision**: Generate timestamps using uniform random distribution within
the configured date range, with optional time-of-day weighting.

**Rationale**: Uniform distribution across the date range is simple and
sufficient for pipeline testing. Transactions are generated with random
times throughout the day. For a learning project, this provides adequate
realism without the complexity of modeling daily/weekly spending patterns.

**Alternatives considered**:
- Weighted by day-of-week: More realistic but adds complexity.
- Poisson process: Accurate for event modeling but over-engineered here.
- Sequential timestamps: Unrealistic, all transactions at regular intervals.

### 6. Seed-Based Reproducibility

**Decision**: Use a single seed value that initializes both NumPy's
`default_rng()` and Python's `random` module (for Faker). Polars output
ordering is deterministic by construction (no shuffling).

**Rationale**: A single seed controls all random state, ensuring byte-identical
output across runs (SC-003). The seed is logged in the generation metadata
for reproduction. When no seed is provided, a random seed is generated and
reported.

**Alternatives considered**:
- Per-component seeds: More isolation but harder to reproduce full runs.
- Hash-based determinism: Complex, unnecessary for this use case.
