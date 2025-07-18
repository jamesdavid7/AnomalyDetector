# DynamoDB Example
import uuid

from config.constatns import TABLE_ANOMALY_METRICS
from utils.dynamodb_utils import DynamoDBUtils

db = DynamoDBUtils(table_name=TABLE_ANOMALY_METRICS)
# item = {
#     'metric_id': str(uuid.uuid4()),
#     'metric_data': [
#         {
#             'anomaly_type': 'duplicate',
#             'count': 44
#         },
#         {
#             'anomaly_type': 'delayed_settlement',
#             'count': 30
#         },
#         {
#             'anomaly_type': 'card_type',
#             'count': 26
#         }
#     ]
# }
# db.insert_item(item)

key = {"metric_id": "dd6c4127-38b4-49ed-bdbf-5b78cdbb57d5"}
item = db.get_item(key)

