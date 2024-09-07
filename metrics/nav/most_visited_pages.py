"""
Este módulo calcula las páginas más visitadas en los logs de navegación.
"""

import polars as pl

def filtrar_urls_vacias(logs_df):
    """
    Filtra las URLs vacías en el DataFrame.
    """
    return logs_df.filter(pl.col("request_url").is_not_null())

def contar_frecuencia_url(logs_df):
    """
    Cuenta la frecuencia de visitas por URL.
    """
    return logs_df.group_by("request_url").agg(
        pl.col("request_url").count().alias("visit_count")
    )

def calculate_nav_most_visited_pages(logs_df):
    """
    Calcula las páginas más visitadas en los logs de navegación.
    """
    df_frecuencia_url_ordenado = contar_frecuencia_url(
        filtrar_urls_vacias(logs_df)
    ).sort("visit_count", descending=True)

    print(df_frecuencia_url_ordenado.head(10))
