from botocore.exceptions import ClientError
from api.config.aws_config import AWSConfig
from api.models.transaction import Transaction


class TransactionRepo:
    def __init__(self, table_name: str):
        self.table = AWSConfig.get_dynamodb_resource().Table(table_name)

    def insert_transaction(self, transaction: Transaction) -> bool:
        """Insert a transaction into DynamoDB"""
        try:
            item = Transaction.to_dynamodb_item(transaction)
            self.table.put_item(Item=item)
            print(f"✅ Inserted transaction: {transaction.transaction_id}")
            return True
        except Exception as e:
            print(f"❌ Error inserting transaction: {e}")
            return False

    def get_transactions_paginated(self, limit: int = 10, last_evaluated_key: dict = None):
        try:
            scan_kwargs = {"Limit": limit}
            if last_evaluated_key:
                scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

            response = self.table.scan(**scan_kwargs)

            items = response.get("Items", [])
            transactions = [Transaction.to_transaction(item) for item in items]

            return {
                "transactions": [t.to_dict() for t in transactions],
                "last_evaluated_key": response.get("LastEvaluatedKey")
            }
        except ClientError as e:
            raise Exception(f"❌ Failed to scan DynamoDB table: {e}")

    def get_transaction(self, key: dict) -> Transaction | None:
        """Get a transaction by key (e.g. PK/SK)"""
        try:
            response = self.table.get_item(Key=key)
            item = response.get("Item")
            if item:
                return Transaction.to_transaction(item)
            else:
                print("⚠️ Transaction not found")
                return None
        except Exception as e:
            print(f"❌ Error retrieving transaction: {e}")
            return None

    def get_all_transactions(self) -> list[Transaction]:
        """Scan all transactions from DynamoDB"""
        try:
            transactions = []
            response = self.table.scan()

            raw_items = response.get("Items", [])
            transactions.extend([Transaction.to_transaction(item) for item in raw_items])

            while "LastEvaluatedKey" in response:
                response = self.table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
                raw_items = response.get("Items", [])
                transactions.extend([Transaction.to_transaction(item) for item in raw_items])

            return transactions

        except ClientError as e:
            raise Exception(f"❌ Failed to scan DynamoDB table: {e}")
