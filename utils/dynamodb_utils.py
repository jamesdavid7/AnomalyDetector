from config.aws_config import get_boto3_session

class DynamoDBUtils:
    def __init__(self, table_name, region='us-east-1'):
        self.session = get_boto3_session(region)
        self.dynamodb = self.session.resource('dynamodb')
        self.table = self.dynamodb.Table(table_name)

    def insert_item(self, item: dict):
        try:
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
