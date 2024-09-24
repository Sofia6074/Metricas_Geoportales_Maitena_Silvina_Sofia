import polars as pl

from metrics.maps.maximum_stable_value_zoom import calculate_zoom_levels
from metrics.metrics_utils import filter_session_outliers


def calculate_maximum_zoom(logs_df):
    """
    Calculates the maximum zoom level reached in the logs.
    """
    logs_df = filter_session_outliers(logs_df)
    map_requests_df = logs_df.filter(pl.col("request_url").str.contains("wms|wmts"))
    map_requests_df = calculate_zoom_levels(map_requests_df)
    max_zoom_level = map_requests_df.select(pl.col("zoom_level").max()).item()

    print(f"Maximum zoom level: {max_zoom_level}")
    return max_zoom_level
