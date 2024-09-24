"""
This module calculates the maximum stable zoom value in map logs.
"""

import polars as pl

from metrics.metrics_utils import filter_session_outliers
from scripts_py.common.log_cleaner import convert_timestamp


def calculate_zoom_levels(map_requests_df):
    """
    Calculates the zoom levels from the map requests.
    """
    map_requests_df = map_requests_df.with_columns(
        pl.col("request_url").str.replace("%3A", ":").alias("request_decoded")
    )
    map_requests_df = map_requests_df.with_columns(
        pl.when(pl.col("request_decoded").str.contains(r"(?i)TileMatrix=EPSG"))
        .then(pl.col("request_decoded").str
              .extract(r"(?i)TileMatrix=EPSG:\d+:(\d+)", 1).cast(pl.Float64))
        .otherwise(None).alias("zoom_level")
    )

    map_requests_df = map_requests_df.filter(pl.col("zoom_level").is_not_null())

    return map_requests_df


def find_stable_zoom(map_requests_df):
    """
    Finds the stable zoom level based on the logic of
    increasing zoom and then returning to a lower level.
    """
    map_requests_df = map_requests_df.sort(["unique_session_id", "timestamp"])

    map_requests_df = map_requests_df.with_columns([
        pl.col("zoom_level").shift(1).alias("prev_zoom_level"),
        pl.col("zoom_level").shift(-1).alias("next_zoom_level")
    ])

    # Identify when the zoom increases and then returns to a lower level
    stable_zoom_df = map_requests_df.filter(
        (pl.col("zoom_level") < pl.col("prev_zoom_level")) &
        (pl.col("prev_zoom_level") > pl.col("next_zoom_level"))
    )

    return stable_zoom_df


def calculate_maximum_stable_value_zoom(logs_df):
    """
    Calculates the maximum stable zoom value in the logs.
    """
    logs_df = convert_timestamp(logs_df)
    map_requests_df = filter_session_outliers(logs_df)
    map_requests_df = map_requests_df.sort(["unique_session_id", "timestamp"])

    map_requests_df = map_requests_df.filter(pl.col("request_url").str.contains(r"(?i)wms|wmts"))
    map_requests_df = calculate_zoom_levels(map_requests_df)

    stable_zoom_df = find_stable_zoom(map_requests_df)

    stable_zoom_counts = stable_zoom_df.group_by(["unique_session_id", "zoom_level"]).agg(
        pl.count("zoom_level").alias("count")
    )
    max_stable_zoom_per_session = stable_zoom_counts.sort(
        ["unique_session_id", "count", "zoom_level"], descending=True
    )

    print("Stable Zoom per Session:")
    print(max_stable_zoom_per_session.head(10))

    total_stable_zoom = stable_zoom_counts.group_by("zoom_level").agg(
        pl.sum("count").alias("total_count")
    ).sort(["total_count", "zoom_level"], descending=True)

    print("Most Stable Zoom Across All Sessions:")
    print(total_stable_zoom.head(10))