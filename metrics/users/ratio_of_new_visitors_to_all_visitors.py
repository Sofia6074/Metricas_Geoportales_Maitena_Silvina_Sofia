"""
Este módulo calcula la proporción de nuevos visitantes respecto al total de visitantes.
"""

import polars as pl

def calculate_ratio_of_new_visitors_to_all_visitors(logs_df):
    """
    Calcula y muestra la proporción de nuevos visitantes respecto al total de visitantes.

    """
    total_visitors = logs_df.select("ip").unique().height

    visitor_counts = logs_df.group_by("ip").agg(
        pl.col("ip").count().alias("visit_count")
    )

    new_visitors = visitor_counts.filter(pl.col("visit_count") == 1).height

    ratio_new_to_all = new_visitors / total_visitors if total_visitors > 0 else 0

    print(f"Ratio of New Visitors to All Visitors: {ratio_new_to_all:.2f}")
