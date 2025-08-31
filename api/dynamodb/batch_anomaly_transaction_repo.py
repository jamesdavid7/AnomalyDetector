from decimal import Decimal
from typing import Optional, Tuple

from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from api.config.aws_config import AWSConfig
from api.config.constatns import TABLE_BATCH_ANOMALY_TRANSACTION
from api.models.batch_anomaly_transaction import BatchAnomalyTransaction


class BatchAnomalyTransactionRepository:
    def __init__(self, table_name=TABLE_BATCH_ANOMALY_TRANSACTION):
        self.table = AWSConfig.get_dynamodb_resource().Table(table_name)

    def _to_dynamo(self, item):
        """ Recursively convert floats to Decimals for DynamoDB """
        if isinstance(item, float):
            return Decimal(str(item))
        elif isinstance(item, int):
            # keep integers as-is (DynamoDB stores them as Number)
            return item
        elif isinstance(item, dict):
            return {k: self._to_dynamo(v) for k, v in item.items()}
        elif isinstance(item, list):
            return [self._to_dynamo(v) for v in item]
        return item

    def save(self, transaction: BatchAnomalyTransaction):
        # Convert transaction to dict
        item = BatchAnomalyTransaction.to_item(transaction)
        # Ensure all floats → Decimal
        item = self._to_dynamo(item)
        # Save to DynamoDB
        self.table.put_item(Item=item)

    def save_all(self, transactions):
        """Save multiple transactions at once using batch_writer"""
        with self.table.batch_writer() as batch:
            for txn in transactions:
                batch.put_item(Item=txn.__dict__)

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
            raise Exception(f"DynamoDB error: {e.response['Error']['Message']}")
        except Exception as e:
            raise Exception(f"Unexpected error: {str(e)}")

    def get_all_items(self) -> list[BatchAnomalyTransaction]:
        try:
            items = []
            response = self.table.scan()

            raw_items = response.get("Items", [])
            items.extend([BatchAnomalyTransaction.from_item(item) for item in raw_items])

            while "LastEvaluatedKey" in response:
                response = self.table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
                raw_items = response.get("Items", [])
                items.extend([BatchAnomalyTransaction.from_item(item) for item in raw_items])

            return items

        except ClientError as e:
            raise Exception(f"DynamoDB error: {e.response['Error']['Message']}")
        except Exception as e:
            raise Exception(f"Unexpected error: {str(e)}")

    def get_paginated_items(
            self,
            limit: int = 10,
            last_evaluated_key: Optional[dict] = None,
            sort_order: str = "desc"
    ) -> Tuple[list[BatchAnomalyTransaction], Optional[dict]]:
        """
        Fetch paginated anomaly transactions, sorted by created_at.
        """
        try:
            scan_kwargs = {"Limit": limit}
            if last_evaluated_key:
                scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

            response = self.table.scan(**scan_kwargs)

            items = [BatchAnomalyTransaction.from_item(item) for item in response.get("Items", [])]

            # ✅ Sort items by created_at (string ISO timestamp → sortable)
            items.sort(
                key=lambda x: x.created_at,
                reverse=(sort_order == "desc")
            )

            next_key = response.get("LastEvaluatedKey")

            return items, next_key

        except ClientError as e:
            raise Exception(f"DynamoDB error: {e.response['Error']['Message']}")
        except Exception as e:
            raise Exception(f"Unexpected error: {str(e)}")


    def scan_by_txn_id(self, transaction_id: str):
        try:
            response = self.table.scan(
                FilterExpression=Attr("transaction_id").eq(transaction_id)
            )
            items = response.get("Items", [])
            return BatchAnomalyTransaction.from_item(items[0]) if items else None
        except Exception as e:
            raise Exception(f"DynamoDB scan error: {str(e)}")
