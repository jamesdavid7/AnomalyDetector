import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any


@dataclass
class MetricData:
    anomaly_type: str
    count: int

@dataclass
class Metric:
    metric_id: str
    file_name: str
    created_at: str
    metric_data: List[MetricData]

    @classmethod
    def create(cls, file_name: str, data: List[MetricData]) -> 'Metric':
        return cls(
            metric_id=str(uuid.uuid4()),
            created_at=str(datetime.utcnow()),
            file_name=file_name,
            metric_data=data
        )

    @staticmethod
    def to_dynamodb_item(metric: 'Metric') -> Dict[str, Any]:
        return {
            "metric_id": metric.metric_id,
            "file_name": metric.file_name,
            "created_at": metric.created_at.isoformat() if isinstance(metric.created_at,
                                                                      datetime) else metric.created_at,
            "metric_data": [
                {
                    "anomaly_type": m.anomaly_type,
                    "count": int(m.count)
                } for m in metric.metric_data
            ]
        }

    @staticmethod
    def to_metric(item: Dict[str, Any]) -> 'Metric':
        metric_data = [
            MetricData(
                anomaly_type=d["anomaly_type"],
                count=int(d["count"])
            )
            for d in item.get("metric_data", [])
        ]

        return Metric(
            metric_id=item.get("metric_id", str(uuid.uuid4())),
            file_name=item["file_name"],
            created_at=item.get("created_at", datetime.utcnow().isoformat()),
            metric_data=metric_data
        )