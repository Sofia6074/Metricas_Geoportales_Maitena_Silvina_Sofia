"""
This module contains the function to calculate the average pages viewed per session
per user categorization from the filtered logs.
"""

import polars as pl
from metrics.metrics_utils import get_base_url, filter_empty_urls


def calculate_average_pages_viewed_per_session_by_profile(logs_df):
    """
    Calculates the average pages viewed per session
    for each user profile within a time period.
    """

    logs_df_filtered = filter_empty_urls(logs_df)
    base_df = get_base_url(logs_df_filtered)

    data_frame_with_sessions = base_df.with_columns([
        (pl.col("unique_session_id") + "_" +
         pl.col("base_url")).alias("unique_url")
    ])
    data_frame_with_sessions = data_frame_with_sessions.with_columns([
        (~pl.col("base_url").str.contains(
            pl.col("base_url").shift(1).str.replace_all(r"\{", r"\{").str.replace_all(r"\}", r"\}")))
        .cum_sum()
        .fill_null(0)
        .alias("url_segment")
    ])
    data_frame_with_sessions = data_frame_with_sessions.with_columns([
        (pl.col("unique_url") + "_" +
         pl.col("url_segment").cast(pl.Utf8)).alias("unique_url_id")
    ])
    data_frame_with_sessions = data_frame_with_sessions.with_columns([
        (pl.col("unique_url_id") != pl.col("unique_url_id").shift(1)).alias("is_new_page")
    ])

    filtered_data = data_frame_with_sessions.filter(pl.col("is_new_page"))

    pages_per_session = filtered_data.group_by(["unique_session_id", "user_profile"]).agg([
        pl.count("is_new_page").alias("pages_viewed")
    ])

    average_pages_viewed_by_profile = pages_per_session.group_by("user_profile").agg(
        pl.col("pages_viewed").mean().alias("avg_pages_viewed")
    )

    print("Average pages viewed by profile: " + average_pages_viewed_by_profile)
    return average_pages_viewed_by_profile
