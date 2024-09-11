"""
This module contains the function to calculate the average time
users spend per page on the site per user categorization.
"""

import polars as pl
from metrics.metrics_utils import format_average_time, get_base_url


def calculate_average_time_spent_per_page_per_user_cat(logs_df):
    """
    Calculates the average time users spend per page on the site
    per user categorization.
    """
    logs_df = get_base_url(logs_df)

    avg_time_per_page_by_category = logs_df.group_by(["base_url", "user_profile"]).agg(
        pl.col("time_spent").mean().alias("avg_time_per_page")
    )

    global_avg_time_per_page_by_category = (
        avg_time_per_page_by_category.group_by("user_profile").agg(
        pl.col("avg_time_per_page").mean().alias("global_avg_time_per_page")
    ))

    print("User Average Time Spent per Page by Profile:")
    for profile in global_avg_time_per_page_by_category.rows():
        user_profile = profile[0]
        global_avg_time = profile[1]
        print(f"Profile {user_profile}: {format_average_time(global_avg_time)}")
