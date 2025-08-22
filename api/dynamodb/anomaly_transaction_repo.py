from botocore.exceptions import ClientError

from api.config.aws_config import AWSConfig
from api.models.anomaly_transation import AnomalyTransaction


class AnomalyTransactionRepository:
    def __init__(self,table_name="anomaly_transaction"):
        self.table = AWSConfig.get_dynamodb_resource().Table(table_name)

    def save(self, transaction: AnomalyTransaction):
        item = AnomalyTransaction.to_item(transaction)
        self.table.put_item(Item=item)

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
        except ClientError as e:
            # re-raise so Flask sees it as 500
            raise Exception(f"DynamoDB error: {e.response['Error']['Message']}")
        except Exception as e:
            raise Exception(f"Unexpected error: {str(e)}")

    def get_all_items(self) -> list[AnomalyTransaction]:
        try:
            items = []
            response = self.table.scan()

            raw_items = response.get("Items", [])
            items.extend([AnomalyTransaction.from_item(item) for item in raw_items])

            while "LastEvaluatedKey" in response:
                response = self.table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
                raw_items = response.get("Items", [])
                items.extend([AnomalyTransaction.from_item(item) for item in raw_items])

            return items

        except ClientError as e:
            # re-raise so Flask sees it as 500
            raise Exception(f"DynamoDB error: {e.response['Error']['Message']}")
        except Exception as e:
            raise Exception(f"Unexpected error: {str(e)}")
