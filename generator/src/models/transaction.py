"""Transaction dataclass with JSON serialization and validation.

Per data-model.md: Transaction is the primary entity flowing through the pipeline.
Published as JSON to Kafka, persisted as Parquet to Iceberg.
"""

import json
import math
import random
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from generator.src.models.enums import (
    Currency,
    MerchantCategory,
    TransactionStatus,
    TransactionType,
)

# Default distribution weights (can be overridden by config)
_DEFAULT_CURRENCY_WEIGHTS = {
    Currency.USD: 0.60,
    Currency.EUR: 0.20,
    Currency.GBP: 0.12,
    Currency.JPY: 0.08,
}

_DEFAULT_CATEGORY_WEIGHTS = {
    MerchantCategory.RETAIL: 0.25,
    MerchantCategory.DINING: 0.20,
    MerchantCategory.ONLINE: 0.20,
    MerchantCategory.GROCERIES: 0.15,
    MerchantCategory.ENTERTAINMENT: 0.08,
    MerchantCategory.TRAVEL: 0.07,
    MerchantCategory.UTILITIES: 0.05,
}

_DEFAULT_TYPE_WEIGHTS = {
    TransactionType.PURCHASE: 0.85,
    TransactionType.WITHDRAWAL: 0.08,
    TransactionType.REFUND: 0.05,
    TransactionType.TRANSFER: 0.02,
}

# Merchant names by category for realistic data
_MERCHANT_NAMES: dict[str, list[str]] = {
    "retail": ["Walmart", "Target", "Best Buy", "Costco", "Home Depot"],
    "dining": ["Starbucks", "McDonald's", "Chipotle", "Olive Garden", "Panera"],
    "travel": ["Delta Airlines", "Marriott", "Uber", "Hertz", "Airbnb"],
    "online": ["Amazon", "eBay", "Shopify Store", "Etsy", "Netflix"],
    "groceries": ["Whole Foods", "Trader Joe's", "Kroger", "Safeway", "Aldi"],
    "entertainment": ["AMC Theaters", "Spotify", "Steam", "Xbox Store", "Ticketmaster"],
    "utilities": ["Electric Co", "Water Utility", "Gas Company", "Internet ISP", "Phone Carrier"],
}


@dataclass
class Transaction:
    """A financial transaction record.

    Attributes:
        transaction_id: UUID v4 string, globally unique.
        timestamp: ISO 8601 datetime with timezone.
        account_id: Alphanumeric account identifier, 10-12 chars.
        amount: Monetary value; positive for charges, negative for refunds.
        currency: ISO 4217 currency code.
        merchant_name: Merchant or counterparty name.
        merchant_category: Merchant classification category.
        transaction_type: Type of financial operation.
        location_country: ISO 3166-1 alpha-2 country code.
        status: Current transaction state.
    """

    transaction_id: str
    timestamp: str
    account_id: str
    amount: float
    currency: str
    merchant_name: str
    merchant_category: str
    transaction_type: str
    location_country: str
    status: str

    @classmethod
    def generate(
        cls,
        account: "Account",  # noqa: F821 — forward reference
        currency_weights: dict | None = None,
        category_weights: dict | None = None,
        type_weights: dict | None = None,
    ) -> "Transaction":
        """Generate a realistic random transaction for the given account.

        Args:
            account: The Account initiating the transaction.
            currency_weights: Optional currency distribution weights.
            category_weights: Optional merchant category distribution weights.
            type_weights: Optional transaction type distribution weights.

        Returns:
            A new Transaction with realistic random values.
        """
        cw = currency_weights or _DEFAULT_CURRENCY_WEIGHTS
        catw = category_weights or _DEFAULT_CATEGORY_WEIGHTS
        tw = type_weights or _DEFAULT_TYPE_WEIGHTS

        # Transaction type (weighted)
        types = list(tw.keys())
        t_weights = list(tw.values())
        tx_type = random.choices(types, weights=t_weights, k=1)[0]

        # Currency (weighted)
        currencies = list(cw.keys())
        c_weights = list(cw.values())
        currency = random.choices(currencies, weights=c_weights, k=1)[0]

        # Merchant category (weighted)
        categories = list(catw.keys())
        cat_weights = list(catw.values())
        category = random.choices(categories, weights=cat_weights, k=1)[0]

        # Merchant name from category
        cat_value = category.value if hasattr(category, "value") else str(category)
        merchant_names = _MERCHANT_NAMES.get(cat_value, ["Unknown Merchant"])
        merchant_name = random.choice(merchant_names)

        # Amount: log-normal distribution — mostly small, some large
        # mu=3.5, sigma=1.5 gives median ~$33, with occasional >$10K
        raw_amount = random.lognormvariate(mu=3.5, sigma=1.5)
        amount = round(min(raw_amount, 49999.99), 2)
        # Ensure non-zero
        if amount == 0:
            amount = 0.50

        # Refunds are negative
        tx_type_value = tx_type.value if hasattr(tx_type, "value") else str(tx_type)
        if tx_type_value == "refund":
            amount = -abs(amount)

        # Status: mostly completed for normal transactions
        status = random.choices(
            [TransactionStatus.COMPLETED, TransactionStatus.PENDING, TransactionStatus.FAILED],
            weights=[0.85, 0.10, 0.05],
            k=1,
        )[0]

        return cls(
            transaction_id=str(uuid.uuid4()),
            timestamp=datetime.now(timezone.utc).isoformat(),
            account_id=account.account_id,
            amount=amount,
            currency=currency.value if hasattr(currency, "value") else str(currency),
            merchant_name=merchant_name,
            merchant_category=cat_value,
            transaction_type=tx_type_value,
            location_country=account.country,
            status=status.value if hasattr(status, "value") else str(status),
        )

    def to_dict(self) -> dict:
        """Serialize to a dictionary matching the raw-transactions schema.

        Returns:
            Dictionary with exactly the 10 required fields.
        """
        return {
            "transaction_id": self.transaction_id,
            "timestamp": self.timestamp,
            "account_id": self.account_id,
            "amount": self.amount,
            "currency": self.currency,
            "merchant_name": self.merchant_name,
            "merchant_category": self.merchant_category,
            "transaction_type": self.transaction_type,
            "location_country": self.location_country,
            "status": self.status,
        }

    def to_json(self) -> str:
        """Serialize to a JSON string.

        Returns:
            JSON string representation of the transaction.
        """
        return json.dumps(self.to_dict())
