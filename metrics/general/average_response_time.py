"""
This module contains the function to calculate the average, maximum, and minimum
response time from a DataFrame of logs.
"""

import polars as pl

def calculate_average_response_time(logs_df):
    """
    Calculates the average, maximum, and minimum response time using Polars.
    """

    q_low_logs_df = logs_df.select(
        pl.col("response_time").quantile(0.05)
    ).item()

    q_high_logs_df = logs_df.select(
        pl.col("response_time").quantile(0.95)
    ).item()

    data_frame = logs_df.filter(
        (pl.col("response_time") > q_low_logs_df) &
        (pl.col("response_time") < q_high_logs_df)
    )

    avg_response_time = data_frame.select(
        (pl.col("response_time") / 1000).mean().alias("avg_response_time")
    )[0, "avg_response_time"]

    max_response_time = data_frame.select(
        (pl.col("response_time") / 1000).max().alias("max_response_time")
    )[0, "max_response_time"]

    min_response_time = data_frame.select(
        (pl.col("response_time") / 1000).min().alias("min_response_time")
    )[0, "min_response_time"]

    print(f"Average Response Time: {avg_response_time:.2f} s")
    print(f"Maximum Response Time: {max_response_time:.2f} s")
    print(f"Minimum Response Time: {min_response_time:.2f} s")

    return {
        "avg_response_time": avg_response_time,
        "max_response_time": max_response_time,
        "min_response_time": min_response_time,
    }
