import os

from config.aws_config import get_boto3_session

class S3Utils:
    def __init__(self, bucket_name, region='us-east-1'):
        self.session = get_boto3_session(region)
        self.s3 = self.session.client('s3')
        self.bucket = bucket_name

    def upload_file(self, file_path, object_name=None):
        if object_name is None:
            object_name = file_path.split("/")[-1]

        ensure_directory_exists(object_name)

        try:
            self.s3.upload_file(file_path, self.bucket, object_name)
            print(f"Uploaded {file_path} to s3://{self.bucket}/{object_name}")
            return True
        except Exception as e:
            print(f"Error uploading to S3: {e}")
            return False


"""
Check if the given directory exists. If not, create it.
"""
def ensure_directory_exists(directory):
    if os.path.isdir(directory):
        print("Directory exists." + directory)
    else:
        print("Creating directory..." + directory)
        os.makedirs(directory)

