"""
This module classifies users into profiles (high, medium, occasional) based on
their visits and time spent on the website. It also performs additional
calculations related to user profiles.
"""

import polars as pl
from metrics.metrics_utils import calculate_sessions
from metrics.users.user_categorization.average_pages_viewed_per_user_cat import \
    calculate_average_pages_viewed_per_session_by_profile


def print_user_profile_counts(logs_df):
    """
    Prints the percentage of users per profile.
    """
    total_users = logs_df.select(pl.col("ip")).n_unique()
    profile_counts = logs_df.group_by("user_profile").agg([
        pl.col("ip").n_unique().alias("count")
    ])

    profile_counts = profile_counts.with_columns([
        (pl.col("count") / total_users * 100).alias("percentage")
    ])

    print(profile_counts)


def calculate_user_categorized_metrics(logs_df):
    """
    Placeholder for additional calculations based on categorized users.
    """
    # This space is reserved for future additional calculations.
    # logs_df will be used here in the future.
    calculate_average_pages_viewed_per_session_by_profile(logs_df)


def classify_user_profiles(logs_df):
    """
    Classifies users into three profiles based on the number
    of visits and total time spent.
    """

    sessions_df = calculate_sessions(logs_df)

    user_stats = sessions_df.group_by("ip").agg([
        pl.col("session_id").count().alias("visits"),
        pl.col("time_diff").sum().alias("total_time_spent")
    ])

    high_profile_threshold = user_stats.select(pl.col("visits").quantile(0.8)).item()
    average_profile_threshold = user_stats.select(pl.col("visits").quantile(0.5)).item()

    user_stats = user_stats.with_columns([
        pl.when(
            (pl.col("visits") > high_profile_threshold) &
            (pl.col("total_time_spent") > high_profile_threshold)
        ).then(1)  # High profile
        .when(
            (pl.col("visits") > average_profile_threshold) &
            (pl.col("total_time_spent") > average_profile_threshold)
        ).then(2)  # Medium profile
        .otherwise(3).alias("user_profile")  # Occasional users
    ])

    logs_df = logs_df.join(user_stats.select(["ip", "user_profile"]), on="ip", how="left")

    logs_df.write_csv("user_profile.csv")

    print_user_profile_counts(logs_df)

    calculate_user_categorized_metrics(logs_df)

    return logs_df
