"""
This module calculates the success and error rates from logs.
"""

import polars as pl

def calculate_error_rate_success_rate(logs_df):
    """
    Calculates the success and error rates from logs.
    """

    logs_df = logs_df.with_columns([
        pl.col("status_code").cast(pl.Int32)
    ])

    success_df = logs_df.filter(
        (pl.col("status_code") >= 200) & (pl.col("status_code") < 300)
    )
    error_df = logs_df.filter(
        (pl.col("status_code") >= 400) & (pl.col("status_code") < 600)
    )
    total_requests = logs_df.height

    success_count = success_df.height
    error_count = error_df.height

    success_rate = (success_count / total_requests) * 100 if total_requests > 0 else 0
    error_rate = (error_count / total_requests) * 100 if total_requests > 0 else 0

    print(f"Success Rate: {success_rate:.2f}%")
    print(f"Error Rate: {error_rate:.2f}%")
