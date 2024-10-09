"""
This module contains utilities for metrics calculation.
"""

from datetime import timedelta, datetime
import polars as pl

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
    formatted_string = (f"{int(hours)} hours {int(minutes)}"
                        f" minutes {int(seconds)} seconds")

    return formatted_string

def get_base_url(logs_df):
    """
    Filters out the base url from the query params
    """
    return logs_df.with_columns(
        pl.col("request_url")
        .str.split_exact("?", 1)
        .struct.rename_fields(["base_url", "query_params"])
        .alias("fields")
    ).unnest("fields")


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

    logs_df = logs_df.with_columns(pl.lit("other").alias("device_type"))

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

    logs_df = logs_df.with_columns(
        pl.when(pl.col("user_agent").str.contains(mobile_patterns))
        .then(pl.lit("mobile"))
        .otherwise(pl.col("device_type"))
        .alias("device_type")
    )

    return logs_df


def custom_serializer(obj):
    """
    Custom JSON serializer for datetime and timedelta objects
    """
    if isinstance(obj, timedelta):
        return obj.total_seconds()
    if isinstance(obj, datetime):
        if obj.tzinfo is not None:
            obj = obj.astimezone(None)
        return obj.isoformat()
    if isinstance(obj, pl.DataFrame):
        return obj.to_dicts()
    raise TypeError(
        f"Object of type {obj.__class__.__name__} is not JSON serializable"
    )

