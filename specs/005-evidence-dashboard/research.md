# Research: Evidence.dev Dashboard

**Feature**: 005-evidence-dashboard
**Date**: 2026-02-23

## Research Questions

### 1. How does Evidence.dev connect to DuckDB?

**Options Evaluated**:
- **A) DuckDB source plugin with source queries** -- Evidence uses a plugin system (`evidence.plugins.yaml`) to register data sources. For DuckDB, the `@evidence-dev/duckdb` plugin connects to a `.duckdb` file. Source queries are `.sql` files placed in `sources/<source_name>/` that extract data into Parquet at build/dev time. All page-level queries then run against these extracted Parquet files using DuckDB SQL dialect.
- **B) Direct file attachment in queries** -- DuckDB allows `read_parquet()` or attaching databases inline, but Evidence doesn't support this; it uses the source plugin abstraction.

**Decision**: **Option A** -- Use the `@evidence-dev/duckdb` plugin. Configure it in `evidence.plugins.yaml`. Place source queries in `sources/warehouse/` that SELECT from the existing DuckDB tables. The `connection.yaml` file points to `../../data/warehouse/transactions.duckdb` with `read_only: true`. Each source query creates a table accessible as `warehouse.<query_name>` in page markdown.

### 2. How should the DuckDB connection handle read-only access?

**Options Evaluated**:
- **A) DuckDB `access_mode: read_only`** -- DuckDB supports opening databases in read-only mode, which prevents write locks and conflicts with concurrent pipeline processes.
- **B) Copy the database file** -- Copy `.duckdb` to `sources/` and read from the copy. Avoids locking but data becomes stale.
- **C) Rely on DuckDB's multiple-reader support** -- DuckDB allows multiple readers but a writer takes an exclusive lock. If pipelines run while dashboard is open, locking errors can occur.

**Decision**: **Option A** -- Configure `access_mode: read_only` in `connection.yaml`. This ensures the dashboard never acquires a write lock and can coexist with pipeline processes that write to the same database.

### 3. Should we use source queries or inline queries in markdown?

**Options Evaluated**:
- **A) Source queries only** -- All SQL in `sources/warehouse/*.sql`, extracted at `npm run sources` time. Page queries reference the extracted data. Requires re-running sources when data changes.
- **B) Inline queries only** -- All SQL in markdown code fences. Evidence runs them in dev mode using its built-in DuckDB. But these queries run against extracted source data, not live database.
- **C) Source queries for extraction + inline queries for page-level aggregation** -- Source queries extract raw/semi-aggregated data. Inline queries in markdown do final shaping. This is the Evidence-recommended pattern.

**Decision**: **Option C** -- Use source queries in `sources/warehouse/` to extract table data from the DuckDB warehouse. Use inline SQL code fences in markdown pages for page-specific aggregations and formatting. This separates data extraction (source queries) from presentation logic (inline queries).

### 4. What Evidence.dev components are best suited for each dashboard section?

**Analysis of available components against spec requirements**:

| Dashboard Section | Data Shape | Component Choice | Rationale |
|-------------------|-----------|------------------|-----------|
| Overview: latest run status | Single row per run type | `BigValue` | Shows key metric prominently |
| Overview: pipeline timeline | Time series, multiple run types | `DataTable` | Combined chronological view of all runs |
| Financial: daily spend by category | Time series, grouped by category | `BarChart` (stacked) | Shows volume and composition over time |
| Financial: monthly account summary | Tabular, multi-column | `DataTable` + `BarChart` | Table for detail, chart for trends |
| Financial: top merchants | Ranked list | `BarChart` (horizontal) | Natural ranking visualization |
| Financial: currency breakdown | Categorical distribution | `BarChart` | Shows volume by currency |
| Ingestion: run summary | Tabular time series | `DataTable` | Run-level detail with all metrics |
| Ingestion: trends over time | Time series, multi-metric | `LineChart` | Trend comparison of loaded/quarantined/duplicates |
| Ingestion: quarantine breakdown | Categorical | `BarChart` | Reason distribution |
| Data Quality: latest run summary | Single row, multi-metric | `BigValue` | Pass/fail/error counts at a glance |
| Data Quality: trends over time | Time series, per-check status | `BarChart` (stacked) | Status distribution per run |
| Data Quality: violation details | Tabular | `DataTable` | Detailed check results |

### 5. How to initialize the Evidence.dev project?

**Options Evaluated**:
- **A) `npx degit evidence-dev/template`** -- Official template scaffolding, recommended by Evidence docs. Creates a clean project with standard structure.
- **B) `npm create evidence`** -- Interactive CLI that sets up a project. More guided but may ask interactive questions.
- **C) Manual setup** -- Create files from scratch. Full control but error-prone.

**Decision**: **Option A** -- Use `npx degit evidence-dev/template` to scaffold the project in `dashboard/`. Then configure the DuckDB source plugin and add custom pages. This is the officially recommended approach from the spec's clarification Q1.

### 6. Project structure within `dashboard/`

Evidence.dev projects follow a specific directory structure:

```
dashboard/
  pages/               # Markdown pages (file-based routing)
    index.md            # Home/overview page at /
    financial-analytics.md    # /financial-analytics
    ingestion-health.md       # /ingestion-health
    data-quality.md           # /data-quality
  sources/              # Data source configurations
    warehouse/          # DuckDB source named "warehouse"
      connection.yaml   # DuckDB connection config
      *.sql             # Source extraction queries
  evidence.plugins.yaml # Plugin configuration
  package.json          # Node.js dependencies
  .gitignore            # Ignore node_modules, .evidence
```

Pages are ordered in the sidebar alphabetically by default. Evidence uses the file name for sidebar ordering, so we can prefix with numbers or rely on the default alphabetical order per the spec (Overview is index.md = home).

## Technology Decisions Summary

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Data source plugin | `@evidence-dev/duckdb` | Official plugin, direct DuckDB file access |
| Connection mode | `read_only: true` | Prevents write locks (FR-009) |
| Query strategy | Source queries + inline queries | Separation of extraction and presentation |
| Project scaffolding | `npx degit evidence-dev/template` | Official recommended approach |
| Chart components | BigValue, BarChart, LineChart, DataTable | Best fit per data shape (see table above) |
| Sidebar navigation | Default Evidence sidebar | Per spec clarification Q5 |
| Page routing | File-based (Evidence default) | `pages/index.md` = home, one `.md` per section |
