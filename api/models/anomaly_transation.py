from dataclasses import dataclass
from datetime import datetime

@dataclass
class AnomalyTransaction:
    transaction_id: str
    customer_name: str
    merchant_name: str
    store_name: str
    transaction_amount: float
    is_anomaly: bool
    detections: list
    created_at: str = datetime.utcnow().isoformat()

    @staticmethod
    def to_item(transaction: 'AnomalyTransaction') -> dict:
        """Convert AnomalyTransaction model → DynamoDB item"""
        return {
            "transaction_id": transaction.transaction_id,
            "customer_name": transaction.customer_name,
            "merchant_name": transaction.merchant_name,
            "store_name": transaction.store_name,
            "transaction_amount": transaction.transaction_amount,
            "is_anomaly": transaction.is_anomaly,
            "detections": transaction.detections,
            "created_at": transaction.created_at,
        }

    @staticmethod
    def from_item(item: dict) -> 'AnomalyTransaction':
        """Convert DynamoDB item → AnomalyTransaction model"""
        return AnomalyTransaction(
            transaction_id=item["transaction_id"],
            customer_name=item["customer_name"],
            merchant_name=item["merchant_name"],
            store_name=item["store_name"],
            transaction_amount=float(item["transaction_amount"]),
            is_anomaly=item["is_anomaly"],
            detections=item.get("detections", []),
            created_at=item.get("created_at")
        )