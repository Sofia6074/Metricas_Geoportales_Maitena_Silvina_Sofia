"""
Este módulo contiene la función para calcular el promedio de páginas vistas por sesión
a partir de los logs filtrados.
"""

import polars as pl

from metrics.metrics_utils import filter_empty_urls, calculate_sessions


def calculate_average_pages_viewed_per_session(logs_df):
    """
    Calcula el promedio de páginas vistas por sesión a partir de los logs.

    :param logs_df: DataFrame de logs que contiene las solicitudes del sitio web.
    :return: None
    """

    logs_df_without_null_url = filter_empty_urls(logs_df)

    sessions_df = calculate_sessions(logs_df_without_null_url)

    pages_per_session = sessions_df.group_by("unique_session_id").agg(
        pl.col("request_url").count().alias("pages_viewed")
    )

    average_pages_viewed = pages_per_session.select(
        pl.col("pages_viewed").mean().alias("avg_pages_viewed")
    ).to_dict(as_series=False)["avg_pages_viewed"][0]

    print(f"Average Pages Viewed per Session: {average_pages_viewed:.2f}")
