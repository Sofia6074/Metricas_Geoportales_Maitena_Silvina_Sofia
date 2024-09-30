"""
This module contains the function to calculate the average pages viewed per session
from the filtered logs.
"""

import polars as pl
from metrics.metrics_utils import filter_empty_urls

def calculate_average_pages_viewed_per_session(logs_df):
    """
    Calculates the average pages viewed per session from the logs.

    :param logs_df: DataFrame containing the website requests.
    :return: None
    """

    logs_df_without_null_url = filter_empty_urls(logs_df)

    pages_per_session = logs_df_without_null_url.group_by("unique_session_id").agg(
        pl.col("request_url").count().alias("pages_viewed")
    )

    average_pages_viewed = pages_per_session.select(
        pl.col("pages_viewed").mean().alias("avg_pages_viewed")
    ).to_dict(as_series=False)["avg_pages_viewed"][0]

    print(f"Average Pages Viewed per Session: {average_pages_viewed:.2f}")
    return average_pages_viewed
