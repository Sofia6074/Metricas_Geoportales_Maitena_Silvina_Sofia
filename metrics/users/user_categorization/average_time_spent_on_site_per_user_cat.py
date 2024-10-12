"""
This module contains the function to calculate the average time users spend on the site,
including calculation per user category.
"""

import polars as pl


def calculate_average_time_spent_on_site_per_user_category(logs_df):
    """
    Calculates the average time users spend on the site per user profile.
    """

    session_time_df = logs_df.group_by(["unique_session_id", "user_profile"]).agg([
        pl.col("time_spent").mean().alias("total_time_spent")
    ])

    avg_time_per_user_profile = session_time_df.group_by("user_profile").agg([
        pl.col("total_time_spent").mean().alias("avg_time_spent_on_site")
    ])

    print("\nCalculate average time spent on site per user category:")
    print(avg_time_per_user_profile)

    return avg_time_per_user_profile
