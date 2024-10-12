"""
This module calculates the most visited pages in navigation logs.
"""

import polars as pl
from metrics.metrics_utils import filter_empty_urls, get_base_url


def count_url_frequency(logs_df):
    """
    Counts the visit frequency per URL.
    """
    return logs_df.group_by("base_url").agg(
        pl.col("base_url").count().alias("count")
    )


def calculate_nav_most_visited_pages(logs_df):
    """
    Calculates the most visited pages in navigation logs.
    """
    logs_df = filter_empty_urls(logs_df)
    logs_df = get_base_url(logs_df)
    sorted_url_frequency_df = count_url_frequency(logs_df).sort("count", descending=True)

    print(sorted_url_frequency_df.head(10))
    return sorted_url_frequency_df.head(10)
