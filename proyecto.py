"""
Este módulo realiza el procesamiento de logs usando PySpark.
Incluye la creación de una sesión de Spark, la lectura de archivos
CSV, la limpieza de datos y el cálculo de métricas.
"""

import sys
import os
from logging import Logger

from pyspark.sql import SparkSession

from scripts_py.common.log_cleaner import log_cleaner

scripts_py_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'scripts_py')
sys.path.append(scripts_py_path)

logger_instance = Logger(__name__).get_logger()

def create_spark_session(app_name: str, host: str) -> SparkSession:
    """
    Crea y devuelve una sesión de Spark con la configuración especificada.
    """
    try:
        spark_session = SparkSession.builder \
            .appName(app_name) \
            .config("spark.driver.host", host) \
            .getOrCreate()
        spark_session.sparkContext.setLogLevel("ERROR")
        return spark_session
    except Exception as exc:  # pylint: disable=W0703, W0612
        logger_instance.error(
            "Ocurrió un error al crear la sesión de Spark",
            exc_info=True
        )
        sys.exit(1)


if __name__ == "__main__":
    ## PRIMER PASO: Obtener el archivo de Kaggle
    LOG_PATH = '/Users/admin/Documents/TesisArchivo/filtered_logs.csv'

    ## SEGUNDO PASO: Leer el archivo con Spark
    spark = create_spark_session("Geoportales", "127.0.0.1")
    logs_df = spark.read.csv(LOG_PATH, header=False, sep=',', quote='"', escape='"')

    ## TERCER PASO: Particionar el archivo según los meses

    ## CUARTO PASO: Limpieza de datos
    log_cleaner(logs_df)

    ## QUINTO PASO: Cálculo de métricas

    spark.stop()
