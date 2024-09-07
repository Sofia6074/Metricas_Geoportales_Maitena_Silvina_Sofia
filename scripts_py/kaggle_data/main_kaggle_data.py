# import os
# import sys
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, regexp_extract
# from scripts_py.classes.logger import Logger
# from scripts_py.common.clean_data import inicializar_total_registros, filtrar_datos
# from scripts_py.kaggle_data.load_kaggle_data import download_data
#
# LOG_PATTERN = r'(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)" Time (\d+)'
# logger_instance = Logger(__name__).get_logger()
#
#
# def create_spark_session(app_name: str, host: str) -> SparkSession:
#     try:
#         spark = SparkSession.builder \
#             .appName(app_name) \
#             .config("spark.driver.host", host) \
#             .getOrCreate()
#         return spark
#     except Exception as e:
#         logger_instance.error("Ocurrió un error al crear la sesión de Spark", exc_info=True)
#         sys.exit(1)
#
#
# def parse_logs(df):
#     return df.withColumn("ip", regexp_extract(col("_c0"), LOG_PATTERN, 1)) \
#         .withColumn("timestamp", regexp_extract(col("_c0"), LOG_PATTERN, 2)) \
#         .withColumn("request_method", regexp_extract(col("_c0"), LOG_PATTERN, 3)) \
#         .withColumn("status_code", regexp_extract(col("_c0"), LOG_PATTERN, 4).cast("integer")) \
#         .withColumn("response_size", regexp_extract(col("_c0"), LOG_PATTERN, 5).cast("integer")) \
#         .withColumn("url", regexp_extract(col("_c0"), LOG_PATTERN, 6)) \
#         .withColumn("user_agent", regexp_extract(col("_c0"), LOG_PATTERN, 7)) \
#         .withColumn("response_time", regexp_extract(col("_c0"), LOG_PATTERN, 8).cast("integer"))
#
#
# def main():
#     csv_directory = 'C:/WebServerLogs/'
#     csv_file_name = 'filebeat-geoportal-access.csv'
#     csv_file_path = os.path.join(csv_directory, csv_file_name)
#
#     print("Descargando los datos desde Kaggle...")
#     # download_data(csv_file_path)
#
#     spark = create_spark_session("Geoportales", "127.0.0.1")
#     logs_df = csv_to_df(spark, csv_file_path)
#     parsed_logs_df = parse_logs(logs_df)
#
#     parsed_logs_df.show(10)
#
#     print("Limpiando los datos de kaggle...")
#     inicializar_total_registros(parsed_logs_df)
#     parsed_logs_df = filtrar_datos(parsed_logs_df)
#
#     # Normalize data
#     # print("Normalizando los datos...")
#     # df_normalized = normalizar_datos(df)
#
#     # df_normalized.write_csv(csv_file_path +
#     # 'cleaned_normalized_kaggle_data.csv')
#     spark.stop()

# Añade una línea en blanco al final del archivo para evitar el error de Pylint (C0304)
