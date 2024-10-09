"""
This module contains the function to calculate the average time
users spend per page on the site per user categorization.
"""

import polars as pl
from metrics.metrics_utils import format_average_time, get_base_url


def calculate_average_time_spent_per_page_per_user_category(logs_df):
    """
    Calculates the average time users spend per page on the site
    per user categorization.
    """
    session_df = get_base_url(logs_df)

    data_frame_with_sessions = session_df.with_columns([
        (pl.col("unique_session_id") + "_" +
         pl.col("base_url")).alias("unique_url")
    ])

    data_frame_with_sessions = data_frame_with_sessions.filter(pl.col("time_diff").is_not_null())

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

    avg_time_per_page_by_category = data_frame_with_sessions.group_by(
        ["base_url", "unique_session_id", "user_profile"]).agg([
        pl.col("time_diff").mean().alias("avg_time_per_page")
    ])

    avg_time_per_user_profile = avg_time_per_page_by_category.group_by("user_profile").agg([
        pl.col("avg_time_per_page").mean().alias("avg_time_per_user")
    ])

    print(f"User Average Time Spent per Page per Category:")
    print(avg_time_per_user_profile)

    return avg_time_per_user_profile
