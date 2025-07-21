import os
import boto3
from dotenv import load_dotenv

load_dotenv()

class AWSConfig:
    _session = None

    @staticmethod
    def _get_session():
        if AWSConfig._session is None:
            AWSConfig._session = boto3.Session(
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=os.getenv("AWS_DEFAULT_REGION")
            )
        return AWSConfig._session

    @staticmethod
    def get_s3_client():
        return AWSConfig._get_session().client('s3')

    @staticmethod
    def get_dynamodb_resource():
        return AWSConfig._get_session().resource('dynamodb')