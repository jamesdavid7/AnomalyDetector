from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Any
import uuid
from datetime import datetime
import math


@dataclass
class Transaction:
    transaction_id: str
    account_id: str
    customer_id: str
    merchant_name: str
    store_name: str
    card_number: str
    card_type: str
    card_expire_date: str
    transaction_type: str
    transaction_status: str
    amount: float
    currency: str
    terminal_currency: str
    timestamp_initiated: str
    timestamp_completed: str
    failure_reason_code: str
    failure_description: str
    retry_count: int
    device_id: str
    ip_address: str
    geo_location: str
    is_voided: bool
    settlement_status: str
    voided_timestamp: str
    banking_charge: float
    settled_amount: float
    settlement_timestamp: str
    created_by: str
    created_at: str
    is_anomaly: bool
    anomaly_type: str
    has_high_amount: bool
    has_long_duration: bool
    has_odd_hour: bool
    has_expiring_card: bool
    has_geo_far: bool
    rule_anomalies: str
    is_anomaly_suspected_supervised: bool
    transaction_duration: float
    settlement_delay_days: int
    currency_mismatch_flag: bool
    status_fail_flag: bool
    charge_percent: float
    amount_per_minute: float
    iso_anomaly: bool
    iso_score: float
    is_anomaly_suspected_unsupervised: bool
    iso_anomaly_reason: str
    open_ai_anomaly: str
    classification: str
    explanation: str
    suggested_action: str
    anomaly_score: float

    # ---------- Factory ----------
    @classmethod
    def create(cls, **kwargs) -> "Transaction":
        return cls(
            transaction_id=str(uuid.uuid4()),
            created_at=str(datetime.utcnow().isoformat()),
            **kwargs
        )

    # ---------- Utils ----------
    @staticmethod
    def iso_to_epoch(ts: str) -> int:
        """Convert ISO timestamp (e.g. '2025-07-31T14:22:16') to epoch seconds"""
        try:
            return int(datetime.fromisoformat(ts).timestamp())
        except Exception:
            return 0

    @staticmethod
    def safe_decimal(value: float) -> Decimal:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return Decimal("0")
        return Decimal(str(value))

    # ---------- DynamoDB Conversion ----------
    @staticmethod
    def to_dynamodb_item(transaction: "Transaction") -> Dict[str, Any]:
        """Convert Transaction object into a DynamoDB item dictionary."""

        # Ensure required keys
        if not transaction.store_name or not transaction.transaction_id:
            raise ValueError("Transaction missing required key: store_name or transaction_id")

        return {
            "transaction_id": transaction.transaction_id,
            "store_name": transaction.store_name,   # ✅ Partition Key
            "account_id": transaction.account_id,
            "customer_id": transaction.customer_id,
            "merchant_name": transaction.merchant_name,
            "amount": Transaction.safe_decimal(transaction.amount),
            "currency": transaction.currency,
            "transaction_status": transaction.transaction_status,
            "timestamp_initiated": Transaction.iso_to_epoch(transaction.timestamp_initiated),
            "timestamp_completed": Transaction.iso_to_epoch(transaction.timestamp_completed),
            "is_anomaly": bool(transaction.is_anomaly),
            "anomaly_type": transaction.anomaly_type,
            "iso_score": Transaction.safe_decimal(transaction.iso_score),
            "anomaly_score": Transaction.safe_decimal(transaction.anomaly_score),
            "created_at": transaction.created_at,
        }

    @staticmethod
    def to_transaction(item: Dict[str, Any]) -> "Transaction":
        """Convert DynamoDB item dict to Transaction object with proper types"""
        return Transaction(
            transaction_id=item.get("transaction_id", str(uuid.uuid4())),
            account_id=item.get("account_id", ""),
            customer_id=item.get("customer_id", ""),
            merchant_name=item.get("merchant_name", ""),
            store_name=item.get("store_name", ""),
            card_number=item.get("card_number", ""),
            card_type=item.get("card_type", ""),
            card_expire_date=item.get("card_expire_date", ""),
            transaction_type=item.get("transaction_type", ""),
            transaction_status=item.get("transaction_status", ""),
            amount=float(item.get("amount", 0.0)),
            currency=item.get("currency", ""),
            terminal_currency=item.get("terminal_currency", ""),
            timestamp_initiated=item.get("timestamp_initiated", ""),
            timestamp_completed=item.get("timestamp_completed", ""),
            failure_reason_code=item.get("failure_reason_code", ""),
            failure_description=item.get("failure_description", ""),
            retry_count=int(item.get("retry_count", 0)),
            device_id=item.get("device_id", ""),
            ip_address=item.get("ip_address", ""),
            geo_location=item.get("geo_location", ""),
            is_voided=bool(item.get("is_voided", False)),
            settlement_status=item.get("settlement_status", ""),
            voided_timestamp=item.get("voided_timestamp", ""),
            banking_charge=float(item.get("banking_charge", 0.0)),
            settled_amount=float(item.get("settled_amount", 0.0)),
            settlement_timestamp=item.get("settlement_timestamp", ""),
            created_by=item.get("created_by", ""),
            created_at=item.get("created_at", datetime.utcnow().isoformat()),
            is_anomaly=bool(item.get("is_anomaly", False)),
            anomaly_type=item.get("anomaly_type", ""),
            has_high_amount=bool(item.get("has_high_amount", False)),
            has_long_duration=bool(item.get("has_long_duration", False)),
            has_odd_hour=bool(item.get("has_odd_hour", False)),
            has_expiring_card=bool(item.get("has_expiring_card", False)),
            has_geo_far=bool(item.get("has_geo_far", False)),
            rule_anomalies=item.get("rule_anomalies", ""),
            is_anomaly_suspected_supervised=bool(item.get("is_anomaly_suspected_supervised", False)),
            transaction_duration=float(item.get("transaction_duration", 0.0)),
            settlement_delay_days=int(item.get("settlement_delay_days", 0)),
            currency_mismatch_flag=bool(item.get("currency_mismatch_flag", False)),
            status_fail_flag=bool(item.get("status_fail_flag", False)),
            charge_percent=float(item.get("charge_percent", 0.0)),
            amount_per_minute=float(item.get("amount_per_minute", 0.0)),
            iso_anomaly=bool(item.get("iso_anomaly", False)),
            iso_score=float(item.get("iso_score", 0.0)),
            is_anomaly_suspected_unsupervised=bool(item.get("is_anomaly_suspected_unsupervised", False)),
            iso_anomaly_reason=item.get("iso_anomaly_reason", ""),
            open_ai_anomaly=item.get("open_ai_anomaly", ""),
            classification=item.get("classification", ""),
            explanation=item.get("explanation", ""),
            suggested_action=item.get("suggested_action", ""),
            anomaly_score=float(item.get("anomaly_score", 0.0)),
        )

    # ---------- Dict Conversion for API ----------
    def to_dict(self) -> Dict[str, Any]:
        """Convert Transaction object back into a plain dict for API responses."""
        return {k: (float(v) if isinstance(v, Decimal) else v) for k, v in self.__dict__.items()}

