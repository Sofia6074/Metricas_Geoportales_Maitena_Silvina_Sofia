"""
Este módulo calcula la proporción de nuevos visitantes respecto al total de visitantes.
"""

from pyspark.sql.functions import col

def calculate_ratio_of_new_visitors_to_all_visitors(logs_df):
    """
    Calcula y muestra la proporción de nuevos visitantes respecto al total de visitantes.
    """
    total_visitors = logs_df.select("ip").distinct().count()

    visitor_counts = logs_df.groupBy("ip").count()

    new_visitors = visitor_counts.filter(col("count") == 1).count()

    ratio_new_to_all = new_visitors / total_visitors if total_visitors > 0 else 0

    print(f"Ratio of New Visitors to All Visitors: {ratio_new_to_all}")
