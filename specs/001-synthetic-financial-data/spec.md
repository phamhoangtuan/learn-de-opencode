# Feature Specification: Synthetic Financial Transaction Data Generator

**Feature Branch**: `001-synthetic-financial-data`
**Created**: 2026-02-18
**Status**: Draft
**Input**: User description: "Python script with uv to generate financial transactions data as synthetic data instead of calling data API"

## Clarifications

### Session 2026-02-18

- Q: Which currencies should be generated and what is their distribution? → A: Multi-currency: USD, EUR, GBP, JPY with weighted distribution (~70/15/10/5%). Supports future currency conversion pipeline work.
- Q: How many merchant categories and merchants should be generated? → A: Realistic set of 15-20 spending categories with weighted distribution and 50-100 pre-defined merchant names mapped to categories.
- Q: Where should output files be written and how should they be named? → A: Default to `data/raw/` directory with timestamped filename (e.g., `transactions_20260218_143022.parquet`).

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Generate Realistic Financial Transactions (Priority: P1)

As a data engineer, I want to run a single command to generate a configurable
number of synthetic financial transactions so that I can develop and test data
pipelines without depending on an external data API.

**Why this priority**: This is the core value proposition. Without transaction
generation, no downstream pipeline work can proceed. Removing the API dependency
eliminates external rate limits, costs, and availability issues.

**Independent Test**: Can be fully tested by running the generator script and
verifying the output file contains valid transaction records with expected fields
and realistic value distributions.

**Acceptance Scenarios**:

1. **Given** the generator script is installed, **When** the user runs the
   generate command with a count of 1000, **Then** the system produces an output
   file containing exactly 1000 transaction records.
2. **Given** the generator script is installed, **When** the user runs the
   generate command with default settings, **Then** each transaction record
   contains all required fields (transaction ID, timestamp, amount, currency,
   merchant, category, account ID, transaction type, status).
3. **Given** the generator is run, **When** output is produced, **Then**
   transaction amounts follow realistic distributions (not uniform random) and
   timestamps span a configurable date range.

---

### User Story 2 - Configure Generation Parameters (Priority: P2)

As a data engineer, I want to control generation parameters (number of records,
date range, number of unique accounts, output format) so that I can simulate
different data volumes and scenarios for pipeline testing.

**Why this priority**: Configurability enables testing edge cases, performance
benchmarks, and varied data scenarios. It builds directly on top of the core
generation capability.

**Independent Test**: Can be tested by running the generator with different
parameter combinations and verifying each parameter is respected in the output.

**Acceptance Scenarios**:

1. **Given** the generator accepts a date range parameter, **When** the user
   specifies start and end dates, **Then** all generated timestamps fall within
   that range.
2. **Given** the generator accepts an account count parameter, **When** the user
   specifies 50 unique accounts, **Then** the output contains transactions
   distributed across exactly 50 distinct account IDs.
3. **Given** the generator supports multiple output formats, **When** the user
   specifies Parquet as the output format, **Then** the output file is a valid
   Parquet file.

---

### User Story 3 - Reproducible Data Generation (Priority: P3)

As a data engineer, I want to generate the same dataset by providing a seed
value so that I can reproduce test scenarios and share deterministic datasets
with teammates.

**Why this priority**: Reproducibility is essential for debugging pipeline
issues and for consistent test fixtures. It ensures that test results are
deterministic and comparable across runs.

**Independent Test**: Can be tested by running the generator twice with the
same seed and verifying both outputs are byte-identical.

**Acceptance Scenarios**:

1. **Given** a seed value of 42, **When** the generator is run twice with
   identical parameters, **Then** both runs produce identical output files.
2. **Given** no seed is provided, **When** the generator is run, **Then** a
   random seed is used and logged in the output metadata for later reproduction.

---

### Edge Cases

- What happens when the user requests zero records? The system MUST return an
  empty output file with valid headers/schema.
- What happens when the end date is before the start date? The system MUST
  raise a clear validation error before generating any data.
- What happens when the user requests more unique accounts than transactions?
  The system MUST cap unique accounts to the transaction count and log a warning.
- How does the system handle very large generation requests (e.g., 10M records)?
  The system MUST use streaming/chunked writes when generating more than
  1,000,000 records to avoid memory exhaustion.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST generate synthetic financial transaction records with
  fields: transaction_id, timestamp, amount, currency, merchant_name, category,
  account_id, transaction_type (debit/credit), and status (completed/pending/
  failed). Currency MUST be one of USD, EUR, GBP, or JPY with weighted
  distribution (~70/15/10/5% respectively).
- **FR-002**: System MUST support configurable record count via a command-line
  argument (default: 10,000 records).
- **FR-003**: System MUST support configurable date range for transaction
  timestamps (default: last 90 days from today). Timestamps MUST be
  distributed uniformly within the configured date range.
- **FR-004**: System MUST support configurable number of unique accounts
  (default: 100 accounts).
- **FR-005**: System MUST generate realistic transaction amounts following a
  log-normal distribution with parameters mu=3.0, sigma=1.5 (USD base). Amounts
  MUST be currency-scaled using factors: USD=1.0, EUR=0.9, GBP=0.8, JPY=110.0.
  The resulting distribution must be right-skewed (most transactions small, few
  large).
- **FR-006**: System MUST support output in Parquet format (primary) and CSV
  format (secondary). Output files MUST be written to the `data/raw/` directory
  with timestamped filenames (e.g., `transactions_20260218_143022.parquet`).
- **FR-007**: System MUST accept an optional random seed for reproducible
  generation.
- **FR-008**: System MUST be runnable as a standalone script using `uv run`
  with inline dependency declarations (no separate requirements file needed).
  The script MUST support a `--help` flag displaying usage examples, parameter
  descriptions, and default values.
- **FR-009**: System MUST log generation metadata (record count, seed used,
  date range, duration, output path) to stdout in JSON format upon
  completion.
- **FR-010**: System MUST validate all input parameters and provide clear error
  messages for invalid configurations.
- **FR-011**: System MUST include a realistic set of 15-20 spending categories
  (e.g., Groceries, Dining, Transport, Utilities, Entertainment, Healthcare)
  with weighted distributions reflecting real-world spending patterns.
- **FR-012**: System MUST include 50-100 pre-defined merchant names, each mapped
  to a specific spending category, to produce realistic merchant-category
  pairings in generated transactions.

### Key Entities

- **Transaction**: A single financial transaction record representing a purchase,
  transfer, or payment. Key attributes: unique ID, timestamp, monetary amount,
  currency code, merchant information, spending category, originating account,
  type (debit/credit), and processing status.
- **Account**: A synthetic bank/financial account that originates transactions.
  Key attributes: unique account ID, account holder name, account type
  (checking/savings/credit).
- **Merchant**: A business entity that receives transaction payments. Key
  attributes: merchant name, merchant category code (MCC), industry sector.
  50-100 pre-defined merchants mapped to 15-20 spending categories with
  weighted selection reflecting real-world spending patterns.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A user can generate 10,000 synthetic transactions with a single
  command in under 10 seconds.
- **SC-002**: Generated data passes schema validation with zero field-level
  errors (no nulls in required fields, correct data types, valid ranges).
- **SC-003**: Two runs with the same seed produce byte-identical output files
  100% of the time.
- **SC-004**: Generated transaction amounts exhibit a realistic distribution
  (skewness > 1.0, median < mean) matching real-world financial patterns.
- **SC-005**: The generator runs successfully with `uv run` without requiring
  any prior environment setup or dependency installation by the user.
