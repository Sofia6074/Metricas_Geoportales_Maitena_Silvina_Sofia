"""
Este módulo realiza el procesamiento de logs usando Polars.
Incluye la descarga de archivos CSV desde AWS S3, limpieza de datos y cálculo de métricas.
"""

import json
import boto3
import polars as pl
from botocore.exceptions import ClientError
from scripts_py.classes.logger import Logger
from scripts_py.common.log_cleaner import log_cleaner
from metrics.metrics_init import run_all_metrics

def get_aws_credentials():
    """
    Obtiene las credenciales de AWS desde AWS Secrets Manager.
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
    Descarga un archivo desde un bucket de S3 y lo guarda en la ubicación especificada.
    """
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    s3_client.download_file(bucket_name, object_key, file_path)

def read_logs(file_path: str) -> pl.DataFrame:
    """
    Lee un archivo CSV usando Polars y lo devuelve como un DataFrame.
    """
    logger_instance = Logger(__name__).get_logger()

    try:
        logs_dataframe = pl.read_csv(file_path, has_header=False, separator=',', quote_char='"')
        return logs_dataframe
    except Exception:
        logger_instance.error(
            "Ocurrió un error al leer el archivo CSV con Polars",
            exc_info=True
        )
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

    print("Procesamiento finalizado")
