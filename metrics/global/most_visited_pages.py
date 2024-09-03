# """
# Este módulo cuenta las páginas más visitadas en los logs del servidor web y las muestra.
# """
#
# import os
# import sys
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, split, trim, desc
#
#
# def create_spark_session(app_name: str, host: str) -> SparkSession:
#     # """
#     # Crea y devuelve una sesión de Spark con la configuración especificada.
#     # """
#     # try:
#     #     spark_session = SparkSession.builder \
#     #         .appName(app_name) \
#     #         .config("spark.driver.host", host) \
#     #         .getOrCreate()
#     #     spark_session.sparkContext.setLogLevel("ERROR")
#     #     return spark_session
#     # except Exception:  # pylint: disable=W0703
#     #     print("Ocurrió un error al crear la sesión de Spark")
#     #     sys.exit(1)
#
#
# # Ruta del archivo CSV
# CSV_DIRECTORY = 'C:/WebServerLogs/'
# CSV_FILE_NAME = 'parsed_logs_with_headers.csv'
# CSV_FILE_PATH = os.path.join(CSV_DIRECTORY, CSV_FILE_NAME)
#
# #spark = create_spark_session("Geoportales", "127.0.0.1")
# #df = spark.read.csv(CSV_FILE_PATH, header=True, sep=',', quote='"', escape='"')
#
# parsed_logs_df = df.withColumn("request_method", split(trim(col("request")), " ")[0]) \
#                    .withColumn("request_url", split(trim(col("request")), " ")[1]) \
#                    .withColumn("http_version", split(trim(col("request")), " ")[2]) \
#                    .select(
#                        col("ip"),
#                        col("timestamp"),
#                        col("request_method"),
#                        col("request_url"),
#                        col("http_version"),
#                        col("status_code").cast("integer"),
#                        col("response_size").cast("integer"),
#                        col("referrer"),
#                        col("user_agent"),
#                        col("response_time").cast("integer")
#                    )
#
# # Contar las URL más visitadas
# most_visited_pages = parsed_logs_df.groupBy("request_url").count().orderBy(desc("count"))
#
# print("URLs más visitadas:")
# most_visited_pages.show(10, truncate=False)
#
# spark.stop()
