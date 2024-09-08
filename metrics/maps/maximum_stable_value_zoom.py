"""
Este módulo calcula el valor máximo estable del zoom en los logs de mapas.
"""

import polars as pl
from metrics.metrics_utils import calculate_sessions


def calculate_zoom_levels(map_requests_df):
    """
    Calcula los niveles de zoom a partir de las solicitudes del mapa.
    """

    map_requests_df = map_requests_df.with_columns(
        pl.col("request_url").str.replace("%3A", ":").alias("request_decoded")
    )

    map_requests_df = map_requests_df.with_columns(
        pl.when(pl.col("request_decoded").str.contains(r"(?i)TileMatrix=EPSG"))
        .then(pl.col("request_decoded").str.extract(r"(?i)TileMatrix=EPSG:\d+:(\d+)", 1).cast(pl.Float64))
        .otherwise(None).alias("zoom_level")
    )
    return map_requests_df


def find_stable_zoom(map_requests_df):
    """
    Encuentra el nivel de zoom estable basado en la lógica de subir el zoom y volver a uno inferior.
    """

    map_requests_df = map_requests_df.with_columns([
        pl.col("zoom_level").shift(1).alias("prev_zoom_level"),
        pl.col("zoom_level").shift(-1).alias("next_zoom_level")
    ])

    # Identificar cuando el zoom aumenta y luego vuelve a un nivel anterior
    stable_zoom_df = map_requests_df.filter(
        (pl.col("zoom_level") < pl.col("prev_zoom_level")) &
        (pl.col("prev_zoom_level") > pl.col("next_zoom_level"))
    )

    return stable_zoom_df


def calculate_maximum_stable_value_zoom(logs_df):
    """
    Calcula el valor máximo estable del zoom en los logs.
    """

    map_requests_df = logs_df.filter(pl.col("request_url").str.contains("wms|wmts"))
    map_requests_df = calculate_zoom_levels(map_requests_df)
    map_requests_df = calculate_sessions(map_requests_df)
    stable_zoom_df = find_stable_zoom(map_requests_df)

    stable_zoom_counts = stable_zoom_df.group_by(["unique_session_id", "zoom_level"]).agg(
        pl.count("zoom_level").alias("count")
    )
    max_stable_zoom_per_session = stable_zoom_counts.sort(
        ["unique_session_id", "count"], descending=True
    )

    print("Zoom Estable por Sesión:")
    print(max_stable_zoom_per_session.head(10))

    total_stable_zoom = stable_zoom_counts.group_by("zoom_level").agg(
        pl.sum("count").alias("total_count")
    ).sort("total_count", descending=True)

    print("Zoom Más Estable en Todas las Sesiones:")
    print(total_stable_zoom.head(10))
