"""
This module contains the function to calculate the average pages viewed per session
from the filtered logs.
"""

import polars as pl
from metrics.metrics_utils import filter_empty_urls, get_base_url


def calculate_average_pages_viewed_per_session(logs_df):
    """
    Calculates the average pages viewed per session from the logs.

    :param logs_df: DataFrame containing the website requests.
    :return: The average pages viewed per session (float).
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

    pages_per_session = filtered_data.group_by("unique_session_id").agg([
        pl.count("is_new_page").alias("pages_viewed")
    ])

    average_pages_viewed = pages_per_session["pages_viewed"].mean()
    print(f"Average Pages Viewed per Session: {average_pages_viewed:.2f}")
    return average_pages_viewed
