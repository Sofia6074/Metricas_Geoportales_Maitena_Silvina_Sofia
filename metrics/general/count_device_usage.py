"""
This script counts how many times each device type was used.
"""

import polars as pl

from metrics.metrics_utils import classify_device_type
from scripts_py.classes.logger import Logger

logger_instance = Logger(__name__).get_logger()


def count_device_usage(logs_df):
    """
    Combines session calculation with device classification to count
    how many times each device type was used.
    """

    device_df = classify_device_type(logs_df)

    device_usage_count = device_df.group_by("device_type").agg(
        pl.count().alias("device_usage_count")
    )

    all_device_types = ["mobile", "tablet", "desktop"]
    missing_device_types = set(all_device_types) - set(device_usage_count["device_type"].to_list())

    if missing_device_types:
        missing_rows = pl.DataFrame({
            "device_type": list(missing_device_types),
            "device_usage_count": [0] * len(missing_device_types)
        }).with_columns([
            pl.col("device_usage_count").cast(pl.UInt32)  # Ensure type consistency
        ])
        device_usage_count = device_usage_count.with_columns([
            pl.col("device_usage_count").cast(pl.UInt32)  # Ensure type consistency
        ])
        device_usage_count = pl.concat([device_usage_count, missing_rows])

    logger_instance.info("Device Usage Count:")
    logger_instance.info(device_usage_count)
    return device_usage_count.to_dicts()
