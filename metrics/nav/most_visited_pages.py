"""
This module calculates the most visited pages in navigation logs.
"""

import polars as pl
from metrics.metrics_utils import filter_empty_urls


def count_url_frequency(logs_df):
    """
    Counts the visit frequency per URL.
    """
    return logs_df.group_by("request_url").agg(
        pl.col("request_url").count().alias("visit_count")
    )


def calculate_nav_most_visited_pages(logs_df):
    """
    Calculates the most visited pages in navigation logs.
    """
    sorted_url_frequency_df = count_url_frequency(
        filter_empty_urls(logs_df)
    ).sort("visit_count", descending=True)

    print(sorted_url_frequency_df.head(10))
