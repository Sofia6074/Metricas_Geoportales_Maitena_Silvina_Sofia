"""
This module processes logs using Polars.
It includes downloading CSV files from AWS S3, cleaning the data, and calculating metrics.
"""

import json
import boto3
import polars as pl
from botocore.exceptions import ClientError

from Utils.log_cleaner import log_cleaner
from metrics.index import run_all_metrics


def get_aws_credentials():
    """
    Retrieves AWS credentials from AWS Secrets Manager.
    """
    secret_name = "prod/AppBeta/AWS_Credentials"
    region_name = "us-east-2"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as client_error:
        raise client_error

    secret = get_secret_value_response['SecretString']
    secret_dict = json.loads(secret)

    return secret_dict

def download_file_from_s3(
    bucket_name: str, object_key: str, file_path: str,
    aws_access_key_id: str, aws_secret_access_key: str
):
    """
    Downloads a file from an S3 bucket and saves it to the specified location.
    """
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    s3_client.download_file(bucket_name, object_key, file_path)

def read_logs(file_path: str) -> pl.DataFrame:
    """
    Reads a CSV file using Polars and returns it as a DataFrame.
    """
    try:
        logs_dataframe = pl.read_csv(file_path, has_header=False, separator=',', quote_char='"')
        return logs_dataframe
    except Exception as e:
        print("An error occurred while reading the CSV file with Polars:")
        print(e)
        raise

if __name__ == "__main__":
    aws_secrets = get_aws_credentials()
    AWS_ACCESS_KEY = aws_secrets["AWS_ACCESS_KEY_ID"]
    AWS_SECRET_KEY = aws_secrets["AWS_SECRET_ACCESS_KEY"]
    BUCKET_NAME = 'file-bucket-container'
    OBJECT_KEY = 'filebeat-geoportal-access100MB.csv'
    FILE_PATH = 'filebeat-geoportal-access100MB.csv'

    download_file_from_s3(
        BUCKET_NAME, OBJECT_KEY, FILE_PATH, AWS_ACCESS_KEY, AWS_SECRET_KEY
    )

    logs_df = read_logs(FILE_PATH)

    logs_df = log_cleaner(logs_df)

    run_all_metrics(logs_df)

    print("Processing completed")
