"""Shared constants and enums for the data generator.

Defines canonical values for transaction types, merchant categories,
currencies, statuses, and alert severities. These MUST match the
JSON Schema contracts in specs/001-streaming-financial-pipeline/contracts/.

Per Constitution v2.0.0 Principle II (Data Quality & Governance):
All enum values are centrally defined to ensure consistency.
"""

from enum import StrEnum


class TransactionType(StrEnum):
    """Valid transaction types per data-model.md and raw-transactions.schema.json."""

    PURCHASE = "purchase"
    WITHDRAWAL = "withdrawal"
    TRANSFER = "transfer"
    REFUND = "refund"


class MerchantCategory(StrEnum):
    """Valid merchant categories per data-model.md and raw-transactions.schema.json."""

    RETAIL = "retail"
    DINING = "dining"
    TRAVEL = "travel"
    ONLINE = "online"
    GROCERIES = "groceries"
    ENTERTAINMENT = "entertainment"
    UTILITIES = "utilities"


class Currency(StrEnum):
    """Supported ISO 4217 currency codes per data-model.md."""

    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    JPY = "JPY"


class TransactionStatus(StrEnum):
    """Valid transaction statuses per data-model.md.

    State transitions follow a linear model:
    - pending -> completed -> reversed
    - pending -> failed
    No other transitions are valid.
    """

    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    REVERSED = "reversed"


class AlertSeverity(StrEnum):
    """Alert severity levels per data-model.md and alerts.schema.json."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AccountType(StrEnum):
    """Account types per data-model.md."""

    CHECKING = "checking"
    SAVINGS = "savings"
    CREDIT = "credit"


class RuleType(StrEnum):
    """Alerting rule types per data-model.md."""

    THRESHOLD = "threshold"
    VELOCITY = "velocity"
    TIME_BASED = "time_based"


# Valid state transitions for Transaction status
VALID_STATE_TRANSITIONS: dict[TransactionStatus, list[TransactionStatus]] = {
    TransactionStatus.PENDING: [TransactionStatus.COMPLETED, TransactionStatus.FAILED],
    TransactionStatus.COMPLETED: [TransactionStatus.REVERSED],
    TransactionStatus.FAILED: [],
    TransactionStatus.REVERSED: [],
}
