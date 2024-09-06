"""
Este módulo calcula el tiempo promedio que los usuarios pasan en el sitio por página.
"""

import polars as pl
from metrics.metrics_utils import calculate_sessions

def calculate_average_time_spent_on_site(logs_df):
    """
    Calcula el tiempo promedio que los usuarios pasan en el sitio por página.
    """
    session_df = calculate_sessions(logs_df)

    session_df = session_df.with_columns([
        (pl.col("timestamp").shift(-1) - pl.col("timestamp")).alias("time_spent")
    ])

    valid_sessions = session_df.filter(
        (pl.col("time_spent").is_not_null()) & (pl.col("time_spent") > 0)
    )
    average_time_per_page = valid_sessions.group_by("session_id").agg(
        pl.col("time_spent").mean().alias("average_time_per_page")
    )

    global_average_time_per_page = average_time_per_page.select(
        pl.col("average_time_per_page").mean().alias("global_average_time_per_page")
    )[0, "global_average_time_per_page"]

    print(f"User Average Time Spent per Page: {global_average_time_per_page}")
