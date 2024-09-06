"""
Este módulo calcula el promedio de páginas vistas por visitante.
"""

import polars as pl

def calculate_average_pages_viewed_per_visitor(logs_df):
    """
    Calcula el promedio de páginas vistas por visitante a partir de los logs.
    """
    pages_per_visitor = logs_df.group_by("ip").agg(
        pl.col("request_url").count().alias("pages_viewed")
    )

    average_pages_viewed = pages_per_visitor.select(
        pl.col("pages_viewed").mean().alias("avg_pages_viewed")
    ).to_dict(as_series=False)["avg_pages_viewed"][0]

    print(f"Average Pages Viewed per Visitor: {average_pages_viewed:.2f}")
