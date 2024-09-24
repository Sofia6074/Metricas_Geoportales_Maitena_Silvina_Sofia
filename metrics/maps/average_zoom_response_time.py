import polars as pl

from metrics.maps.maximum_stable_value_zoom import calculate_zoom_levels
from metrics.metrics_utils import filter_session_outliers
from scripts_py.common.log_cleaner import convert_timestamp


def calculate_average_response_time_during_zoom(logs_df):
    """
    Calculates the average response time when the user performs a zoom action.
    """
    logs_df = convert_timestamp(logs_df)
    logs_df = filter_session_outliers(logs_df)

    map_requests_df = logs_df.filter(
        pl.col("request_url").str.contains("wms|wmts"))
    map_requests_df = calculate_zoom_levels(map_requests_df)
    zoom_requests_df = map_requests_df.filter(pl.col("zoom_level").is_not_null())

    lower_bound = zoom_requests_df['response_time'].quantile(0.01)
    upper_bound = zoom_requests_df['response_time'].quantile(0.99)
    zoom_requests_df = zoom_requests_df.filter(
        (pl.col("response_time") >= lower_bound) &
        (pl.col("response_time") <= upper_bound)
    )

    average_response_time = zoom_requests_df.select(
        pl.col("response_time").mean()).item()

    print(f"Average response time during zoom: {average_response_time} ms")
    return average_response_time
