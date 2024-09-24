"""
This module contains the function to calculate the average
time users spend on the site.
"""

import polars as pl

from metrics.metrics_utils import format_average_time


def calculate_average_time_spent_on_site(logs_df):
    """
    Calculates the average time users spend on the site.
    """

    grouped_df = logs_df.group_by("unique_session_id").agg([
        pl.col("time_spent").mean().alias("global_avg_time_spent_on_site")
    ])

    global_avg_time_spent = grouped_df.select(
        pl.col("global_avg_time_spent_on_site").mean()
    )[0, 0]

    print(f"User Average Time Spent on Site: "
          f"{format_average_time(global_avg_time_spent)}")
    return global_avg_time_spent
