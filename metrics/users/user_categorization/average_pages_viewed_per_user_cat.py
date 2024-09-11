"""
This module contains the function to calculate the average pages viewed per session
per user categorization from the filtered logs.
"""

import polars as pl
from metrics.metrics_utils import filter_empty_urls, calculate_sessions


def calculate_average_pages_viewed_per_session_by_profile(logs_df):
    """
    Calculates the average pages viewed per session for each user profile within a time period.

    :param logs_df: DataFrame containing the website requests.
    :return: None
    """

    logs_df_without_null_url = filter_empty_urls(logs_df)

    sessions_df = calculate_sessions(logs_df_without_null_url)

    pages_per_session = sessions_df.group_by(["unique_session_id", "user_profile"]).agg(
        pl.col("request_url").count().alias("pages_viewed")
    )

    average_pages_viewed_by_profile = pages_per_session.group_by("user_profile").agg(
        pl.col("pages_viewed").mean().alias("avg_pages_viewed")
    )

    print(average_pages_viewed_by_profile)
