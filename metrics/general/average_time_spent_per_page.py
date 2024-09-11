"""
This module contains the function to calculate the average time users spend per page on the site.
"""

import polars as pl
from metrics.metrics_utils import format_average_time, filter_session_outliers, get_base_url


def calculate_average_time_spent_per_page(logs_df):
    """
    Calculates the average time users spend per page on the site.
    """
    session_df = filter_session_outliers(logs_df)

    session_df = get_base_url(session_df)

    avg_time_per_page = session_df.group_by("base_url").agg(
        pl.col("time_spent").mean().alias("avg_time_per_page")
    )

    global_avg_time_per_page = avg_time_per_page.select(
        pl.col("avg_time_per_page").mean().alias("global_avg_time_per_page")
    )[0, "global_avg_time_per_page"]

    print(f"User Average Time Spent per Page: {format_average_time(global_avg_time_per_page)}")
