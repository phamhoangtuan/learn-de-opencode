# Quickstart: Synthetic Financial Transaction Data Generator

**Feature**: 001-synthetic-financial-data
**Date**: 2026-02-18

## Prerequisites

- [uv](https://docs.astral.sh/uv/) installed (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- Python 3.11+ (uv will manage this automatically)

## Generate Transactions

### Basic Usage (defaults: 10,000 records, Parquet, last 90 days)

```bash
uv run src/generate_transactions.py
```

Output: `data/raw/transactions_YYYYMMDD_HHMMSS.parquet`

### Custom Record Count

```bash
uv run src/generate_transactions.py --count 50000
```

### Custom Date Range

```bash
uv run src/generate_transactions.py --start-date 2025-01-01 --end-date 2025-12-31
```

### CSV Output

```bash
uv run src/generate_transactions.py --format csv
```

### Reproducible Generation (with seed)

```bash
uv run src/generate_transactions.py --seed 42
```

### Custom Account Count

```bash
uv run src/generate_transactions.py --accounts 50
```

### Full Example (all options)

```bash
uv run src/generate_transactions.py \
  --count 100000 \
  --start-date 2025-06-01 \
  --end-date 2025-12-31 \
  --accounts 200 \
  --format parquet \
  --seed 42
```

## Expected Output

After running, the script prints generation metadata to stdout:

```json
{
  "records_generated": 100000,
  "seed": 42,
  "start_date": "2025-06-01",
  "end_date": "2025-12-31",
  "accounts": 200,
  "format": "parquet",
  "output_path": "data/raw/transactions_20260218_143022.parquet",
  "duration_seconds": 3.45
}
```

## Verify Output

### Read Parquet with Polars

```bash
uv run --with polars python -c "
import polars as pl
df = pl.read_parquet('data/raw/transactions_20260218_143022.parquet')
print(df.shape)
print(df.head())
print(df.describe())
"
```

### Check Schema

```bash
uv run --with polars python -c "
import polars as pl
df = pl.read_parquet('data/raw/transactions_20260218_143022.parquet')
print(df.schema)
"
```

## Run Tests

```bash
uv run --with pytest --with pytest-cov pytest tests/ -v --cov=src --cov-report=term-missing
```
