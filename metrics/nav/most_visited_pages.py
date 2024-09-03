"""
Este módulo calcula las páginas más visitadas en los logs de navegación.
"""

from pyspark.sql.functions import col, count

def filtrar_urls_vacias(logs_df):
    """
    Filtra las URLs vacías en el DataFrame.
    """
    return logs_df.filter(col("request_url").isNotNull())

def contar_frecuencia_url(logs_df):
    """
    Cuenta la frecuencia de visitas por URL.
    """
    return logs_df.groupBy("request_url").agg(
        count("request_url").alias("visit_count")
    )

def calculate_nav_most_visited_pages(logs_df):
    """
    Calcula las páginas más visitadas en los logs de navegación.
    """
    df_frecuencia_url_ordenado = contar_frecuencia_url(
        filtrar_urls_vacias(logs_df)
    ).orderBy(col("visit_count").desc())

    df_frecuencia_url_ordenado.show(10)
