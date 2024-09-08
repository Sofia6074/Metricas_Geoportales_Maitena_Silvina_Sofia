"""
This module contains utilities for metrics calculation.
"""

import polars as pl


def calculate_sessions(map_requests_df):
    """
    Calculates unique sessions based on the IP and user agent.
    """
    map_requests_df = map_requests_df.with_columns([
        (pl.col("ip") + "_" + pl.col("user_agent")).alias("session_id")
    ])

    map_requests_df = map_requests_df.sort(by=["session_id", "timestamp"])

    map_requests_df = map_requests_df.with_columns([
        pl.col("timestamp").shift(1).over("session_id").alias("prev_timestamp")
    ])

    map_requests_df = map_requests_df.with_columns([
        (pl.col("timestamp") - pl.col("prev_timestamp")).alias("time_diff")
    ])

    # If the time between two timestamps is greater than
    # 1800 seconds (30 minutes), it is a new session
    map_requests_df = map_requests_df.with_columns([
        pl.when(pl.col("time_diff") > 1800).then(1).otherwise(0).alias("new_session")
    ])

    map_requests_df = map_requests_df.with_columns([
        pl.col("new_session").cum_sum().over("session_id").alias("session_number")
    ])

    map_requests_df = map_requests_df.with_columns([
        (pl.col("ip") + "_" +
         pl.col("user_agent") + "_" +
         pl.col("session_number").cast(pl.Utf8)).alias("unique_session_id")
    ])

    return map_requests_df


def filter_empty_urls(logs_df):
    """
    Filters empty URLs in the DataFrame.
    """
    return logs_df.filter(pl.col("request_url").is_not_null())


def format_average_time(average_time):
    """
    Formats the average time in hours, minutes, and seconds.
    """
    hours, remainder = divmod(average_time.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    formatted_string = f"{int(hours)} hours {int(minutes)} minutes {int(seconds)} seconds"
    return formatted_string


def filter_session_outliers(logs_df):
    """
    Filters out session outliers greater than 12 hours
    (12 * 60 minutes * 60 seconds * 1_000_000 microseconds).
    """
    session_df = calculate_sessions(logs_df)
    session_df = session_df.with_columns([
        (pl.col("timestamp").shift(-1) - pl.col("timestamp")).alias("time_spent")
    ])

    time_threshold = 12 * 60 * 60 * 1_000_000
    session_filtered_df = session_df.filter(
        (pl.col("time_spent").is_not_null()) &
        (pl.col("time_spent") > 0) &
        (pl.col("time_spent") <= time_threshold)
    )
    return session_filtered_df
