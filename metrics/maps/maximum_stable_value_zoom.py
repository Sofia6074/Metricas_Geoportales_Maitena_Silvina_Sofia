"""
Este módulo calcula el valor máximo estable del zoom en los logs de mapas.
"""

from pyspark.sql import Window
from pyspark.sql.functions import (
    col, lag, when, unix_timestamp, regexp_replace,
    regexp_extract
)
from metrics.metrics_utils import calculate_sessions

def calculate_zoom_levels(map_requests_df):
    """
    Calcula los niveles de zoom a partir de las solicitudes del mapa.
    """
    map_requests_df = map_requests_df.withColumn(
        "request_decoded", regexp_replace(col("request_url"), "%3A", ":")
    )
    return map_requests_df.withColumn(
        "zoom_level",
        when(
            col("request_decoded").rlike("TileMatrix=EPSG:\\d+:\\d+"),
            regexp_extract(col("request_decoded"), "TileMatrix=EPSG:\\d+:(\\d+)", 1).cast("double")
        ).otherwise(None)
    )

def find_stable_zoom(map_requests_df):
    """
    Encuentra el nivel de zoom estable para cada sesión.
    """
    window_spec = Window.partitionBy("session_id").orderBy(
        unix_timestamp("timestamp", "dd/MMM/yyyy:HH:mm:ss Z")
    )

    stable_zoom_df = map_requests_df.withColumn(
        "prev_zoom_level", lag("zoom_level").over(window_spec)
    ).withColumn(
        "next_zoom_level", lag("zoom_level", -1).over(window_spec)
    ).filter(
        (col("zoom_level") == col("prev_zoom_level")) &
        (col("zoom_level") > col("next_zoom_level"))
    )
    return stable_zoom_df

def calculate_maximum_stable_value_zoom(logs_df):
    """
    Calcula el valor máximo estable del zoom en los logs.
    """
    map_requests_df = logs_df.filter(col("request_url").rlike("wms|wmts"))
    map_requests_df = calculate_zoom_levels(map_requests_df)
    map_requests_df = calculate_sessions(map_requests_df)

    stable_zoom_df = find_stable_zoom(map_requests_df)
    stable_zoom_counts = stable_zoom_df.groupBy(
        "unique_session_id", "zoom_level"
    ).count()
    max_stable_zoom_per_session = stable_zoom_counts.orderBy(
        "unique_session_id", col("count").desc()
    )

    print("Zoom Estable por Sesión:")
    max_stable_zoom_per_session.show(10)

    total_stable_zoom = stable_zoom_counts.groupBy("zoom_level").sum("count").orderBy(
        col("sum(count)").desc()
    )

    print("Zoom Más Estable en Todas las Sesiones:")
    total_stable_zoom.show(10)
