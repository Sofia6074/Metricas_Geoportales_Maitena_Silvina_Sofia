import polars as pl
from datetime import timedelta

"""
This module contains utilities for metrics calculation.
"""


def calculate_sessions(data_frame):
    """
    Calculates unique sessions based on:
        - IP
        - User Agent
        - 30-minute threshold between searches.
    """
    data_frame_with_sessions = data_frame.with_columns([
        pl.concat_str([pl.col("ip"), pl.col("user_agent")]).alias("session_id")
    ])

    data_frame_with_sessions = data_frame_with_sessions.sort(
        by=["session_id", "timestamp"])

    data_frame_with_sessions = data_frame_with_sessions.with_columns([
        pl.col("timestamp").diff().over("session_id").alias("time_diff")
    ])

    # Crear una nueva sesión si la diferencia entre una request
    # y la anterior es mayor a 30 minutos
    data_frame_with_sessions = data_frame_with_sessions.with_columns([
        (pl.col("time_diff") > timedelta(minutes=30))
        .cum_sum().over("session_id")
        .alias("session_segment")
    ])

    data_frame_with_sessions = data_frame_with_sessions.with_columns([
        (pl.col("session_id") + "_" +
         pl.col("session_segment").cast(pl.Utf8)).alias("unique_session_id")
    ])

    return data_frame_with_sessions


def filter_empty_urls(logs_df):
    """
    Filters empty URLs in the DataFrame.
    """
    return logs_df.filter(pl.col("request_url").is_not_null())


def format_average_time(average_time):
    """
    Formats the average time in hours, minutes, and seconds.
    Handles cases where the input is None.
    """
    if average_time is None:
        return "No time data available"

    hours, remainder = divmod(average_time.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    formatted_string = f"{int(hours)} hours {int(minutes)} minutes {int(seconds)} seconds"

    return formatted_string


def filter_session_outliers(logs_df):
    """
    Filters out session outliers greater than 12 hours (43,200 seconds).
    """
    session_df = calculate_sessions(logs_df)

    session_df = session_df.sort(['unique_session_id', 'timestamp'])

    session_df = session_df.with_columns(
        (pl.col("timestamp").diff()
         .over("unique_session_id")).alias("time_spent")
    )

    # Filter out unrealistic session gaps greater than 12 hours
    session_filtered_df = session_df.filter(
        (pl.col("unique_session_id").is_not_null()) &
        (pl.col("time_spent").is_not_null()) &
        (pl.col("time_spent") > timedelta(seconds=10)) &
        (pl.col("time_spent") <= timedelta(hours=12))
    )

    return session_filtered_df


def classify_device_type(logs_df):
    """
    Classifies the type of device from which the user connects
    (mobile, tablet, or desktop). This is determined based on
    the user agent string present in the logs.
    """

    mobile_patterns = (
        r"Mobile|Android|iPhone|BlackBerry|Opera Mini|Windows Phone|webOS|"
        r"Redmi|Samsung"
    )
    tablet_patterns = r"iPad|Tablet|Nexus 7|Nexus 10|Kindle|Galaxy Tab"
    desktop_patterns = r"Windows NT|Macintosh|X11|Linux x86_64"

    logs_df = logs_df.with_columns(pl.lit("unknown").alias("device_type"))

    logs_df = logs_df.with_columns(
        pl.when(pl.col("user_agent").str.contains(mobile_patterns))
        .then(pl.lit("mobile"))
        .otherwise(pl.col("device_type"))
        .alias("device_type")
    )

    logs_df = logs_df.with_columns(
        pl.when(pl.col("user_agent").str.contains(tablet_patterns))
        .then(pl.lit("tablet"))
        .otherwise(pl.col("device_type"))
        .alias("device_type")
    )

    logs_df = logs_df.with_columns(
        pl.when(pl.col("user_agent").str.contains(desktop_patterns))
        .then(pl.lit("desktop"))
        .otherwise(pl.col("device_type"))
        .alias("device_type")
    )

    return logs_df
