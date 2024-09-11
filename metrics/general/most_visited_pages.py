"""
This module counts the most visited pages in the web server logs and displays them.
"""
from pyspark.sql.functions import desc

from metrics.metrics_utils import get_base_url


def calculate_most_visited_pages(logs_df):
    """
    Calculates the most visited pages in the web server logs.
    """
    logs_df = get_base_url(logs_df)

    most_visited_pages = logs_df.groupBy("base_url").count().orderBy(desc("count"))

    print("Most visited URLs:")
    most_visited_pages.show(10, truncate=False)
