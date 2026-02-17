"""Fraud injector for anomalous transaction pattern injection.

Injects fraudulent/anomalous patterns into a percentage of generated
transactions to create test data for the alerting rules.

Per spec.md FR-004: High-value threshold, rapid activity, unusual hours.
Per config/generator.yaml: fraud_percentage controls injection rate.
"""

import random
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone

from generator.src.models.transaction import Transaction


@dataclass
class FraudResult:
    """Result of fraud injection attempt.

    Attributes:
        transaction: The (possibly modified) transaction.
        is_fraudulent: Whether fraud was injected.
        injection_type: Type of fraud injection applied, or None.
    """

    transaction: Transaction
    is_fraudulent: bool
    injection_type: str | None


def inject_high_value(tx: Transaction) -> Transaction:
    """Inject a high-value amount to trigger high-value-transaction rule.

    Sets amount to a random value between $10,001 and $49,999.

    Args:
        tx: The transaction to modify.

    Returns:
        Modified transaction with high-value amount.
    """
    high_amount = round(random.uniform(10001.0, 49999.0), 2)
    return Transaction(
        transaction_id=tx.transaction_id,
        timestamp=tx.timestamp,
        account_id=tx.account_id,
        amount=high_amount,
        currency=tx.currency,
        merchant_name=tx.merchant_name,
        merchant_category=tx.merchant_category,
        transaction_type=tx.transaction_type,
        location_country=tx.location_country,
        status=tx.status,
    )


def inject_unusual_hour(tx: Transaction) -> Transaction:
    """Inject unusual hour timestamp to trigger unusual-hour rule.

    Sets timestamp to a random time between 01:00 and 04:59 UTC.

    Args:
        tx: The transaction to modify.

    Returns:
        Modified transaction with unusual-hour timestamp.
    """
    now = datetime.now(timezone.utc)
    # Set to today at a random hour between 1 and 4 (01:00-04:59 range)
    unusual_hour = random.randint(1, 4)
    unusual_minute = random.randint(0, 59)
    unusual_ts = now.replace(
        hour=unusual_hour,
        minute=unusual_minute,
        second=random.randint(0, 59),
        microsecond=0,
    )
    return Transaction(
        transaction_id=tx.transaction_id,
        timestamp=unusual_ts.isoformat(),
        account_id=tx.account_id,
        amount=tx.amount,
        currency=tx.currency,
        merchant_name=tx.merchant_name,
        merchant_category=tx.merchant_category,
        transaction_type=tx.transaction_type,
        location_country=tx.location_country,
        status=tx.status,
    )


def maybe_inject_fraud(
    tx: Transaction,
    fraud_percentage: int = 3,
) -> FraudResult:
    """Conditionally inject fraud patterns based on configured percentage.

    Randomly decides whether to inject fraud, then randomly selects
    which type of fraud to inject.

    Args:
        tx: The transaction to potentially modify.
        fraud_percentage: Percentage chance (0-100) of injection.

    Returns:
        FraudResult with the (possibly modified) transaction.
    """
    if fraud_percentage <= 0 or random.randint(1, 100) > fraud_percentage:
        return FraudResult(
            transaction=tx,
            is_fraudulent=False,
            injection_type=None,
        )

    # Randomly select injection type
    injection_type = random.choice(["high_value", "unusual_hour", "rapid_activity"])

    if injection_type == "high_value":
        modified = inject_high_value(tx)
    elif injection_type == "unusual_hour":
        modified = inject_unusual_hour(tx)
    elif injection_type == "rapid_activity":
        # For rapid activity, we don't modify the single transaction.
        # The rapid-activity pattern is about volume from the same account,
        # which is handled at the generator level. We mark it for tracking.
        modified = tx
    else:
        modified = tx

    return FraudResult(
        transaction=modified,
        is_fraudulent=True,
        injection_type=injection_type,
    )
