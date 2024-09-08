"""
This module contains the function to calculate the average time users spend on the site.
"""

import polars as pl
from metrics.metrics_utils import format_average_time, filter_session_outliers


def calculate_average_time_spent_on_site(logs_df):
    """
    Calculates the average time users spend on the site.
    """
    session_df = filter_session_outliers(logs_df)

    valid_sessions = session_df.filter(
        (pl.col("time_spent").is_not_null()) & (pl.col("time_spent") > 0)
    )

    total_time_per_session = valid_sessions.group_by("session_id").agg(
        pl.col("time_spent").sum().alias("total_time_spent_on_site")
    )

    global_avg_time_spent = total_time_per_session.select(
        pl.col("total_time_spent_on_site").mean().alias("global_avg_time_spent_on_site")
    )[0, "global_avg_time_spent_on_site"]

    print(f"User Average Time Spent on Site: {format_average_time(global_avg_time_spent)}")
