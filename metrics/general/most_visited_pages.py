"""
This module counts the most visited pages in the web server logs and displays them.
"""
from pyspark.sql.functions import desc

def calculate_most_visited_pages(logs_df):
    """
    Calculates the most visited pages in the web server logs.
    """
    most_visited_pages = logs_df.groupBy("request_url").count().orderBy(desc("count"))

    print("Most visited URLs:")
    most_visited_pages.show(10, truncate=False)
