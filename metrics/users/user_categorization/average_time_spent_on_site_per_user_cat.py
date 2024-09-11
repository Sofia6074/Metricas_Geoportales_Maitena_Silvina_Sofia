"""
This module contains the function to calculate the average time users spend on the site,
including calculation per user profile.
"""

import polars as pl


def calculate_weighted_average_time_spent_on_site(logs_df):
    """
    Calculates the weighted average time users spend on the site per user profile.
    The weight is based on the number of sessions per profile.
    """

    valid_sessions = logs_df.filter(
        (pl.col("time_spent").is_not_null()) & (pl.col("time_spent") > 0)
    )

    total_time_per_session = valid_sessions.group_by(["unique_session_id", "user_profile"]).agg(
        pl.col("time_spent").sum().alias("total_time_spent_on_site")
    )

    profile_agg = total_time_per_session.group_by("user_profile").agg([
        pl.col("total_time_spent_on_site").sum().alias("total_time_spent_per_profile"),
        pl.count("unique_session_id").alias("session_count_per_profile")
    ])

    profile_agg = profile_agg.with_columns(
        (pl.col("total_time_spent_per_profile") / pl.col("session_count_per_profile")).alias(
            "avg_time_spent_per_profile")
    )

    print(profile_agg)
