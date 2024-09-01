from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lag, when, unix_timestamp, regexp_replace,
    regexp_extract, count, expr
)

def create_spark_session():
    return SparkSession.builder \
        .appName("PySpark and Polars Example") \
        .getOrCreate()


def load_and_clean_logs(spark, log_file_path):
    logs_df = spark.read.csv(log_file_path, header=False, sep=',', quote='"', escape='"')
    logs_df = logs_df.withColumn("_c0", regexp_replace(col("_c0"), '""', '"')) \
        .withColumn("_c0", regexp_replace(col("_c0"), '%3A', ':')) \
        .withColumn("_c0", regexp_replace(col("_c0"), '%2C', ','))
    return logs_df


def extract_log_fields(logs_df, log_pattern):
    return logs_df.withColumn("ip", regexp_extract(col("_c0"), log_pattern, 1)) \
        .withColumn("timestamp", regexp_extract(col("_c0"), log_pattern, 2)) \
        .withColumn("request_method", regexp_extract(col("_c0"), log_pattern, 3)) \
        .withColumn("request_url", regexp_extract(col("_c0"), log_pattern, 4)) \
        .withColumn("http_version", regexp_extract(col("_c0"), log_pattern, 5)) \
        .withColumn("status_code", regexp_extract(col("_c0"), log_pattern, 6).cast("integer")) \
        .withColumn("response_size", regexp_extract(col("_c0"), log_pattern, 7).cast("integer")) \
        .withColumn("referer", regexp_extract(col("_c0"), log_pattern, 8)) \
        .withColumn("user_agent", regexp_extract(col("_c0"), log_pattern, 9)) \
        .withColumn("response_time", regexp_extract(col("_c0"), log_pattern, 10).cast("integer"))


def calculate_zoom_levels(map_requests_df):
    map_requests_df = map_requests_df.withColumn(
        "request_decoded", regexp_replace(col("request_url"), "%3A", ":")
    )
    return map_requests_df.withColumn(
        "zoom_level",
        when(
            col("request_decoded").rlike("TileMatrix=EPSG:\\d+:\\d+"),
            regexp_extract(col("request_decoded"), "TileMatrix=EPSG:\\d+:(\\d+)", 1).cast("double")
        ).otherwise(None)
    )


def calculate_sessions(map_requests_df):
    map_requests_df = map_requests_df.withColumn("session_id", col("ip") + "_" + col("user_agent"))

    window_spec = Window.partitionBy("session_id") \
        .orderBy(unix_timestamp("timestamp", "dd/MMM/yyyy:HH:mm:ss Z"))

    map_requests_df = map_requests_df.withColumn(
        "prev_timestamp", lag("timestamp").over(window_spec)
    )
    map_requests_df = map_requests_df.withColumn(
        "time_diff",
        unix_timestamp("timestamp", "dd/MMM/yyyy:HH:mm:ss Z") -
        unix_timestamp("prev_timestamp", "dd/MMM/yyyy:HH:mm:ss Z")
    )
    map_requests_df = map_requests_df.withColumn(
        "new_session", when(col("time_diff") > 1800, 1).otherwise(0)
    )
    map_requests_df = map_requests_df.withColumn(
        "session_number", count("new_session").over(window_spec.rowsBetween(Window.unboundedPreceding, 0))
    )
    return map_requests_df.withColumn(
        "unique_session_id", expr("concat(ip, '_', user_agent, '_', session_number)")
    )


def find_stable_zoom(map_requests_df):
    window_spec = Window.partitionBy("session_id") \
        .orderBy(unix_timestamp("timestamp", "dd/MMM/yyyy:HH:mm:ss Z"))

    stable_zoom_df = map_requests_df.withColumn("prev_zoom_level", lag("zoom_level").over(window_spec)) \
        .withColumn("next_zoom_level", lag("zoom_level", -1).over(window_spec)) \
        .filter(
        (col("zoom_level") == col("prev_zoom_level")) &
        (col("zoom_level") > col("next_zoom_level"))
    )
    return stable_zoom_df


def main():
    log_file_path = 'C:/WebServerLogs/filtered_logs.csv'
    log_pattern = (
        r'(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] "(GET|POST|PUT|DELETE) (.*?) (HTTP/\d\.\d)" (\d+) (\d+) "(.*?)" "(.*?)" Time (\d+)'
    )

    spark = create_spark_session()
    logs_df = load_and_clean_logs(spark, log_file_path)
    logs_df = extract_log_fields(logs_df, log_pattern)

    map_requests_df = logs_df.filter(col("request_url").rlike("wms|wmts"))
    map_requests_df = calculate_zoom_levels(map_requests_df)
    map_requests_df = calculate_sessions(map_requests_df)

    stable_zoom_df = find_stable_zoom(map_requests_df)
    stable_zoom_counts = stable_zoom_df.groupBy("unique_session_id", "zoom_level").count()
    max_stable_zoom_per_session = stable_zoom_counts.orderBy("unique_session_id", col("count").desc())

    print("Zoom Estable por Sesión:")
    max_stable_zoom_per_session.show(10)

    total_stable_zoom = stable_zoom_counts.groupBy("zoom_level") \
        .sum("count") \
        .orderBy(col("sum(count)").desc())

    print("Zoom Más Estable en Todas las Sesiones:")
    total_stable_zoom.show(10)


if __name__ == "__main__":
    main()
