import boto3

def get_boto3_session(region_name='us-east-1'):
    # If using IAM role or credentials are configured via environment/CLI
    session = boto3.Session(region_name=region_name)
    return session