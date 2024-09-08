"""
This module contains the function to calculate the average time users spend per page on the site.
"""

import polars as pl
from metrics.metrics_utils import format_average_time, filter_session_outliers


def calculate_average_time_spent_per_page(logs_df):
    """
    Calculates the average time users spend per page on the site.
    """
    session_df = filter_session_outliers(logs_df)

    valid_sessions = session_df.filter(
        (pl.col("time_spent").is_not_null()) & (pl.col("time_spent") > 0)
    )

    avg_time_per_page = valid_sessions.group_by("session_id").agg(
        pl.col("time_spent").mean().alias("avg_time_per_page")
    )

    global_avg_time_per_page = avg_time_per_page.select(
        pl.col("avg_time_per_page").mean().alias("global_avg_time_per_page")
    )[0, "global_avg_time_per_page"]

    print(f"User Average Time Spent per Page: {format_average_time(global_avg_time_per_page)}")
