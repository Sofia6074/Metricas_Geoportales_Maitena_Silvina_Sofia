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

    global_avg_time_spent = logs_df.select(
        pl.col("time_spent").mean().alias("global_avg_time_spent_on_site")
    )[0, "global_avg_time_spent_on_site"]

    print(f"User Average Time Spent on Site: "
          f"{format_average_time(global_avg_time_spent)}")
