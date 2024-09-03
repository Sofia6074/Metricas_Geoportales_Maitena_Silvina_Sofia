"""
Este módulo calcula las tasas de éxito y error a partir de los logs.
"""

from pyspark.sql import functions as F

def calculate_error_rate_success_rate(logs_df):
    """
    Calcula las tasas de éxito y error a partir de los logs.
    """
    logs_df = logs_df.withColumn("status_code", F.col("status_code").cast("int"))

    success_df = logs_df.filter((F.col("status_code") >= 200) & (F.col("status_code") < 300))
    error_df = logs_df.filter((F.col("status_code") >= 400) & (F.col("status_code") < 600))

    total_requests = logs_df.count()

    success_count = success_df.count()
    error_count = error_df.count()

    success_rate = (success_count / total_requests) * 100
    error_rate = (error_count / total_requests) * 100

    print(f"Success Rate: {success_rate:.2f}%")
    print(f"Error Rate: {error_rate:.2f}%")
