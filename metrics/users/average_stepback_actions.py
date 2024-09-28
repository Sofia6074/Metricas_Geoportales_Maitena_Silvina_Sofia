"""
This module calculates the average number of stepback actions per session.
"""

import polars as pl

def calculate_average_stepback_actions(logs_df):
    """
    Calculates the average number of times users step back per session by detecting
    if a user returns to a previously visited URL, considering a 3-second time gap
    between requests.
    """

    # Filter out requests to GeoServer to focus on user interactions
    # (excluding map service requests)
    session_df = logs_df.filter(
        ~pl.col("request_url").str.contains("geoserver")
    )

    session_df = session_df.sort("unique_session_id","timestamp")  # Ordenar por timestamp

    session_df = session_df.with_columns([
        pl.col("timestamp").cast(pl.Datetime).alias("timestamp"),
        pl.col("request_url").shift(1).over("session_id").alias("previous_url"),
        (pl.col("timestamp") - pl.col("timestamp").shift(1).over("session_id"))
            .alias("time_diff_seconds")
    ])

    session_df = session_df.with_columns(
        (pl.col("time_diff_seconds").cast(pl.Float64) / 1_000_000).alias("time_diff_seconds")
    )

    # Filter stepbacks, considering only those with a time difference of more than 3 seconds
    stepbacks = session_df.filter(
        (pl.col("request_url") == pl.col("previous_url")) &
        (pl.col("time_diff_seconds") > 3)
    ).group_by("session_id").agg(
        pl.count().alias("stepback_count")
    )

    average_stepback = stepbacks.select(
        pl.col("stepback_count").mean().alias("average_stepback_actions")
    ).with_columns(pl.col("average_stepback_actions").round(2))[0, "average_stepback_actions"]

    print(f"Average Stepback Actions per Session: {average_stepback}")
    return average_stepback
