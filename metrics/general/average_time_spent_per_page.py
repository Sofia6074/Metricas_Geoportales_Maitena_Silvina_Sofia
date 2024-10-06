"""
This module contains the function to calculate the average time users spend per page on the site.
"""

import polars as pl

from metrics.metrics_utils import format_average_time, get_base_url

def calculate_average_time_spent_per_page(logs_df):
    """
    Calculates the average time users spend per page on the site.
    """

    # Extract the base URL for each request
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

    print(data_frame_with_sessions)

    entry_pages_df = data_frame_with_sessions.group_by('unique_url_id').agg([
        pl.col('time_diff').sum().alias('time_spent_on_page')
    ])

    global_avg_time_spent = entry_pages_df.select(
        pl.col("time_spent_on_page").mean()
    )[0, 0]

    print(f"User Average Time Spent per Page: {format_average_time(global_avg_time_spent)}")
    return global_avg_time_spent
