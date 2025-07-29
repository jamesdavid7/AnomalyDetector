from botocore.exceptions import ClientError

from api.config.aws_config import AWSConfig
from api.models.metric import Metric


class MetricDataRepo:
    def __init__(self,table_name):
        self.table = AWSConfig.get_dynamodb_resource().Table(table_name)

    def insert_item(self, metric: Metric):
        try:
            item = Metric.to_dynamodb_item(metric)
            self.table.put_item(Item=item)
            print(f"Inserted item: {item}")
            return True
        except Exception as e:
            print(f"Error inserting item: {e}")
            return False

    def get_item(self, key: dict):
        try:
            response = self.table.get_item(Key=key)
            item = response.get('Item')
            if item:
                print(f"Retrieved item: {item}")
                return item
            else:
                print("Item not found")
                return None
        except Exception as e:
            print(f"Error retrieving item: {e}")
            return None

    def get_all_items(self) -> list[Metric]:
        try:
            items = []
            response = self.table.scan()

            raw_items = response.get("Items", [])
            items.extend([Metric.to_metric(item) for item in raw_items])

            while "LastEvaluatedKey" in response:
                response = self.table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
                raw_items = response.get("Items", [])
                items.extend([Metric.to_metric(item) for item in raw_items])

            return items

        except ClientError as e:
            raise Exception(f"Failed to scan DynamoDB table: {e}")
