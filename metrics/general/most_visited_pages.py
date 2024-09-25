"""
This module counts the most visited pages in the web server logs and displays them.
"""
import polars as pl

from metrics.metrics_utils import get_base_url


def calculate_most_visited_pages(logs_df):
    """
    Calculates the most visited pages in the web server logs.
    """
    logs_df = get_base_url(logs_df)

    most_visited_pages = (
        logs_df
        .group_by("base_url")
        .agg(pl.count())
        .sort("count", descending=True)
    )

    print("Most visited URLs:")
    print(most_visited_pages.head(10))
