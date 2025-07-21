import os
from io import BytesIO

from botocore.exceptions import ClientError

from config.aws_config import AWSConfig


class S3Utils:
    def __init__(self, bucket_name):
        self.s3 = AWSConfig.get_s3_client()
        self.bucket = bucket_name

    def send_file_to_s3(self, file_path, directory_name=None):
        file_name = os.path.basename(file_path)
        # If directory_name is provided (e.g., "input"), use it to build the key
        if directory_name:
            s3_target_path = f"{directory_name}/{file_name}"
            ensure_directory_exists(directory_name)
        else:
            s3_target_path = file_name  # upload to root of bucket

        try:
            self.s3.upload_file(file_path, self.bucket, s3_target_path)
            print(f"Uploaded {file_path} to s3://{self.bucket}/{s3_target_path}")
            return file_name
        except Exception as e:
            print(f"Error uploading to S3: {e}")
            return None

    def download_file_data(self, file_name: str) -> BytesIO:
        s3_client = AWSConfig.get_s3_client()
        buffer = BytesIO()

        try:
            s3_client.download_fileobj(Bucket=self.bucket, Key=file_name, Fileobj=buffer)
            buffer.seek(0)
            return buffer

        except ClientError as e:
            raise Exception(f"Failed to download {file_name} from S3: {e}")

"""
Check if the given directory exists. If not, create it.
"""
def ensure_directory_exists(directory):
    if os.path.isdir(directory):
        print("Directory exists." + directory)
    else:
        print("Creating directory..." + directory)
        os.makedirs(directory)

