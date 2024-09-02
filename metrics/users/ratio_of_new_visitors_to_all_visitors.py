"""
This module calculates the ratio of new visitors to all visitors
based on web server log data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, regexp_replace

def create_spark_session():
    """Creates and returns a Spark session."""
    return SparkSession.builder \
        .appName("Ratio New Visitors To All Visitors") \
        .getOrCreate()

def load_and_clean_logs(spark, log_file_path):
    """
    Loads and cleans the log data from a CSV file.

    Args:
        spark (SparkSession): The Spark session.
        log_file_path (str): Path to the log file.

    Returns:
        DataFrame: Cleaned log data.
    """
    logs_df = spark.read.csv(log_file_path, header=False, sep=',', quote='"', escape='"')
    logs_df = logs_df.withColumn("_c0", regexp_replace(col("_c0"), '""', '"')) \
        .withColumn("_c0", regexp_replace(col("_c0"), '%3A', ':')) \
        .withColumn("_c0", regexp_replace(col("_c0"), '%2C', ','))
    return logs_df

def extract_log_fields(logs_df, log_pattern):
    """
    Extracts fields from the log data based on the provided pattern.

    Args:
        logs_df (DataFrame): The log data.
        log_pattern (str): The regular expression pattern to extract log fields.

    Returns:
        DataFrame: Log data with extracted fields.
    """
    return logs_df.withColumn("ip_address", regexp_extract(col("_c0"), log_pattern, 1)) \
        .withColumn("timestamp", regexp_extract(col("_c0"), log_pattern, 2)) \
        .withColumn("request_method", regexp_extract(col("_c0"), log_pattern, 3)) \
        .withColumn("request_url", regexp_extract(col("_c0"), log_pattern, 4)) \
        .withColumn("http_version", regexp_extract(col("_c0"), log_pattern, 5)) \
        .withColumn("status_code", regexp_extract(col("_c0"), log_pattern, 6).cast("integer")) \
        .withColumn("response_size", regexp_extract(col("_c0"), log_pattern, 7).cast("integer")) \
        .withColumn("referer", regexp_extract(col("_c0"), log_pattern, 8)) \
        .withColumn("user_agent", regexp_extract(col("_c0"), log_pattern, 9)) \
        .withColumn("response_time", regexp_extract(col("_c0"), log_pattern, 10).cast("integer"))

def main():
    """
    Main function to calculate and print the ratio of new visitors
    to all visitors.
    """
    log_file_path = '/Users/admin/Documents/TesisArchivo/filtered_logs.csv'
    log_pattern = (
        r'(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] "(GET|POST|PUT|DELETE) '
        r'(.*?) (HTTP/\d\.\d)" (\d+) (\d+) "(.*?)" "(.*?)" Time (\d+)'
    )

    spark = create_spark_session()
    logs_df = load_and_clean_logs(spark, log_file_path)
    df = extract_log_fields(logs_df, log_pattern)

    total_visitors = df.select("ip_address").distinct().count()

    visitor_counts = df.groupBy("ip_address").count()

    new_visitors = visitor_counts.filter(col("count") == 1).count()

    if total_visitors > 0:
        ratio_new_to_all = new_visitors / total_visitors
    else:
        ratio_new_to_all = 0

    print(f"Ratio of New Visitors to All Visitors: {ratio_new_to_all}")

    spark.stop()

if __name__ == "__main__":
    main()
