# learn_de Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-02-18

## Active Technologies
- Python 3.11+ + DuckDB (SQL transforms, DAG resolution, metadata tracking) (003-sql-transformations)
- DuckDB SQL transforms: staging views + mart aggregate tables in `data/warehouse/transactions.duckdb` (003-sql-transformations)

- Python 3.11+ + Polars (Parquet read, validation, dedup), DuckDB (warehouse storage, SQL queries), NumPy (inherited from Feature 001) (002-duckdb-ingestion)
- DuckDB embedded database at `data/warehouse/transactions.duckdb` (002-duckdb-ingestion)

- Python 3.11+ + Polars (data generation/output), Faker (names/merchants), NumPy (distributions) (001-synthetic-financial-data)

## Project Structure

```text
src/
tests/
```

## Commands

cd src [ONLY COMMANDS FOR ACTIVE TECHNOLOGIES][ONLY COMMANDS FOR ACTIVE TECHNOLOGIES] pytest [ONLY COMMANDS FOR ACTIVE TECHNOLOGIES][ONLY COMMANDS FOR ACTIVE TECHNOLOGIES] ruff check .

## Code Style

Python 3.11+: Follow standard conventions

## Recent Changes
- 003-sql-transformations: Added Python 3.11+ + DuckDB (SQL transforms, DAG resolution, metadata tracking). Run transforms: `uv run src/run_transforms.py`

- 002-duckdb-ingestion: Added Python 3.11+ + Polars (Parquet read, validation, dedup), DuckDB (warehouse storage, SQL queries), NumPy (inherited from Feature 001)

- 001-synthetic-financial-data: Added Python 3.11+ + Polars (data generation/output), Faker (names/merchants), NumPy (distributions)

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
