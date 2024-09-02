import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, trim, desc


def create_spark_session(app_name: str, host: str) -> SparkSession:
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.driver.host", host) \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception as e:
        print("Ocurrió un error al crear la sesión de Spark")
        sys.exit(1)


# Ruta del archivo CSV
csv_directory = 'C:/WebServerLogs/'
csv_file_name = 'parsed_logs_with_headers.csv'
csv_file_path = os.path.join(csv_directory, csv_file_name)

spark = create_spark_session("Geoportales", "127.0.0.1")
df = spark.read.csv(csv_file_path, header=True, sep=',', quote='"', escape='"')

parsed_logs_df = df.withColumn("request_method", split(trim(col("request")), " ")[0]) \
                   .withColumn("request_url", split(trim(col("request")), " ")[1]) \
                   .withColumn("http_version", split(trim(col("request")), " ")[2]) \
                   .select(
                       col("ip"),
                       col("timestamp"),
                       col("request_method"),
                       col("request_url"),
                       col("http_version"),
                       col("status_code").cast("integer"),
                       col("response_size").cast("integer"),
                       col("referrer"),
                       col("user_agent"),
                       col("response_time").cast("integer")
                   )

# Contar las URL más visitadas
most_visited_pages = parsed_logs_df.groupBy("request_url").count().orderBy(desc("count"))

print("URLs más visitadas:")
most_visited_pages.show(10, truncate=False)


