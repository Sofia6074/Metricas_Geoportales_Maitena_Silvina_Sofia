"""
Este módulo calcula el tiempo promedio que los usuarios pasan en el sitio por página.
"""

from pyspark.sql import functions as F
from pyspark.sql import Window
from metrics.metrics_utils import calculate_sessions

def calculate_average_time_spent_on_site(logs_df):
    """
    Calcula el tiempo promedio que los usuarios pasan en el sitio por página.
    """
    session_df = calculate_sessions(logs_df)

    window_spec = Window.partitionBy("session_id").orderBy("timestamp")
    session_df = session_df.withColumn(
        "time_spent",
        F.unix_timestamp(F.lead("timestamp").over(window_spec)) -
        F.unix_timestamp(F.col("timestamp"))
    )

    valid_sessions = session_df.filter(
        F.col("time_spent").isNotNull() & (F.col("time_spent") > F.lit(0))
    )

    average_time_per_page = valid_sessions.groupBy("session_id").agg(
        F.mean("time_spent").alias("average_time_per_page")
    )

    global_average_time_per_page = average_time_per_page.agg(
        F.mean("average_time_per_page").alias("global_average_time_per_page")
    ).collect()[0]["global_average_time_per_page"]

    print(f"User Average Time Spent per Page: {global_average_time_per_page}")
