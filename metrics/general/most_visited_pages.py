"""
Este módulo cuenta las páginas más visitadas en los logs del servidor web y las muestra.
"""
from pyspark.sql.functions import desc

def calculate_most_visited_pages(logs_df):
    """
    Calcula las páginas más visitadas en los logs del servidor web.
    """
    most_visited_pages = logs_df.groupBy("request_url").count().orderBy(desc("count"))

    print("URLs más visitadas:")
    most_visited_pages.show(10, truncate=False)
