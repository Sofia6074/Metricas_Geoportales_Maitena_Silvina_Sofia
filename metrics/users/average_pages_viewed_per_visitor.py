"""
Este módulo calcula el promedio de páginas vistas por visitante.
"""

from pyspark.sql import functions as F

def calculate_average_pages_viewed_per_visitor(logs_df):
    """
    Calcula el promedio de páginas vistas por visitante a partir de los logs.
    """
    pages_per_visitor = (logs_df.groupBy("ip")
                                .agg(F.count("request_url")
                                .alias("pages_viewed")))
    average_pages_viewed =(pages_per_visitor
                                .agg(F.mean("pages_viewed")
                                .alias("avg_pages_viewed"))
                                .collect()[0]["avg_pages_viewed"])

    print(f"Average Pages Viewed per Visitor: {average_pages_viewed:.2f}")
