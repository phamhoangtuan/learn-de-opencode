# Data Model: Synthetic Financial Transaction Data Generator

**Feature**: 001-synthetic-financial-data
**Date**: 2026-02-18

## Entities

### Transaction

The primary output entity. Each record represents a single financial
transaction.

| Field | Type | Constraints | Description |
|-------|------|-------------|-------------|
| transaction_id | String (UUID) | Unique, not null | Unique identifier for the transaction |
| timestamp | Datetime (UTC) | Not null, within configured range | When the transaction occurred |
| amount | Float64 | Not null, > 0 | Transaction amount in the transaction's currency |
| currency | String (3-char ISO 4217) | Not null, one of: USD, EUR, GBP, JPY | Currency code |
| merchant_name | String | Not null | Name of the merchant |
| category | String | Not null, from predefined list | Spending category (e.g., Groceries, Dining) |
| account_id | String | Not null, format: ACC-XXXXX | Originating account identifier |
| transaction_type | String (enum) | Not null, one of: debit, credit | Direction of money flow |
| status | String (enum) | Not null, one of: completed, pending, failed | Processing status |

**Distribution rules**:
- `amount`: Log-normal distribution (mu=3.0, sigma=1.5), currency-scaled
- `currency`: Weighted random — USD 70%, EUR 15%, GBP 10%, JPY 5%
- `transaction_type`: Weighted — debit 85%, credit 15%
- `status`: Weighted — completed 92%, pending 5%, failed 3%

### Account

Generated reference entity. Accounts are pre-generated and transactions
are distributed across them.

| Field | Type | Constraints | Description |
|-------|------|-------------|-------------|
| account_id | String | Unique, not null, format: ACC-XXXXX | Unique account identifier |
| account_holder | String | Not null | Generated full name |
| account_type | String (enum) | Not null, one of: checking, savings, credit | Type of financial account |

**Distribution rules**:
- `account_type`: Weighted — checking 50%, savings 20%, credit 30%
- Count: Configurable (default 100), capped at transaction count

### Merchant (Reference Data)

Static reference data embedded in `src/data/merchants.json`. Not generated
dynamically.

| Field | Type | Constraints | Description |
|-------|------|-------------|-------------|
| merchant_name | String | Unique, not null | Business name |
| category | String | Not null, from predefined list | Spending category |
| mcc | String (4-digit) | Not null | Merchant Category Code |

**Categories (18 total, with approximate weights)**:

| Category | Weight | Example Merchants |
|----------|--------|-------------------|
| Groceries | 18% | Whole Foods, Trader Joe's, Kroger |
| Dining | 12% | Chipotle, Starbucks, McDonald's |
| Transport | 10% | Uber, Shell, BP Gas |
| Utilities | 8% | Electric Co, Water Utility, Internet Provider |
| Entertainment | 7% | Netflix, Spotify, AMC Theaters |
| Healthcare | 6% | CVS Pharmacy, Walgreens, Dr. Smith |
| Shopping | 6% | Amazon, Target, Walmart |
| Travel | 5% | Delta Airlines, Marriott, Airbnb |
| Insurance | 4% | State Farm, Allstate, GEICO |
| Education | 4% | Coursera, University Bookstore |
| Subscriptions | 3% | Adobe, Microsoft 365, Dropbox |
| Personal Care | 3% | Great Clips, Sephora |
| Home & Garden | 3% | Home Depot, Lowe's, IKEA |
| Fitness | 3% | Planet Fitness, Peloton |
| Electronics | 2% | Best Buy, Apple Store |
| Clothing | 2% | Nike, H&M, Zara |
| Gifts & Donations | 2% | GoFundMe, Red Cross |
| Miscellaneous | 2% | Post Office, Dry Cleaners |

## Relationships

```
Account (1) ──── generates ────> (N) Transaction
Merchant (1) ──── receives ────> (N) Transaction
Category (1) ──── groups ──────> (N) Merchant
```

- Each Transaction belongs to exactly one Account.
- Each Transaction references exactly one Merchant.
- Each Merchant belongs to exactly one Category.
- Accounts are generated first, then transactions are distributed across them.
- Merchant selection is weighted by category weight, then uniform within category.

## State Transitions

Transactions do not transition between states. The `status` field is assigned
at generation time based on weighted random selection and remains immutable
in the output.

## Schema Versioning

The output schema is versioned implicitly by the generator script version.
If fields are added or removed in future iterations, the generation metadata
(FR-009) will include a schema version identifier.
