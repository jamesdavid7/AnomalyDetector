import decimal
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class BatchAnomalyTransaction:
    transaction_id: str
    account_id: str
    customer_id: str
    merchant_name: str
    store_name: str
    card_type: str
    card_expire_date: str
    transaction_type: str
    transaction_amount: decimal.Decimal
    transaction_status: str
    currency: str
    timestamp_initiated: int
    timestamp_completed: int
    retry_count: int
    device_id: str
    ip_address: str
    geo_location: str
    created_by: str
    created_at: int

    # anomaly detection specific
    is_anomaly: bool = False
    anomaly_type: Optional[str] = None
    detections: List[dict] = None
    anomaly_score: decimal.Decimal = 0.0
    classification: Optional[str] = None
    explanation: Optional[str] = None
    suggested_action: Optional[str] = None

    @staticmethod
    def to_item(transaction: 'BatchAnomalyTransaction') -> dict:
        """Convert AnomalyTransaction model → DynamoDB item"""
        return {
            "transaction_id": transaction.transaction_id,
            "account_id": transaction.account_id,
            "customer_id": transaction.customer_id,
            "merchant_name": transaction.merchant_name,
            "store_name": transaction.store_name,
            "card_type": transaction.card_type,
            "card_expire_date": transaction.card_expire_date,
            "transaction_type": transaction.transaction_type,
            "transaction_amount": transaction.transaction_amount,
            "transaction_status": transaction.transaction_status,
            "currency": transaction.currency,
            "timestamp_initiated": transaction.timestamp_initiated,
            "timestamp_completed": transaction.timestamp_completed,
            "retry_count": transaction.retry_count,
            "device_id": transaction.device_id,
            "ip_address": transaction.ip_address,
            "geo_location": transaction.geo_location,
            "created_by": transaction.created_by,
            "created_at": transaction.created_at,
            "is_anomaly": transaction.is_anomaly,
            "detections": transaction.detections or [],
            "anomaly_type": transaction.anomaly_type,
            "anomaly_score": transaction.anomaly_score,
            "classification": transaction.classification,
            "explanation": transaction.explanation,
            "suggested_action": transaction.suggested_action
        }

    @staticmethod
    def from_item(item: dict) -> 'BatchAnomalyTransaction':
        """Convert DynamoDB item → AnomalyTransaction model"""
        return BatchAnomalyTransaction(
            transaction_id=item["transaction_id"],
            account_id=item.get("account_id", ""),  # ✅ safe default
            customer_id=item.get("customer_id", ""),
            merchant_name=item.get("merchant_name", ""),
            store_name=item.get("store_name", ""),
            card_type=item.get("card_type", ""),
            card_expire_date=item.get("card_expire_date", ""),
            transaction_type=item.get("transaction_type", ""),
            transaction_amount=decimal.Decimal(item.get("transaction_amount", 0.0)),
            transaction_status=item.get("transaction_status",""),
            currency=item.get("currency", ""),
            timestamp_initiated=item.get("timestamp_initiated"),
            timestamp_completed=item.get("timestamp_completed"),
            retry_count=int(item.get("retry_count", 0)),
            device_id=item.get("device_id", ""),
            ip_address=item.get("ip_address", ""),
            geo_location=item.get("geo_location", ""),
            created_by=item.get("created_by", ""),
            created_at=item.get("created_at"),
            is_anomaly=item.get("is_anomaly", False),
            detections=item.get("detections", []),
            anomaly_type=item.get("anomaly_type"),
            anomaly_score=decimal.Decimal(item.get("anomaly_score", 0.0)),
            classification=item.get("classification"),
            explanation=item.get("explanation"),
            suggested_action=item.get("suggested_action")
        )

