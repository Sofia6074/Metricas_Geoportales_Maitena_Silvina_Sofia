"""
Contains the code to calculate the times downloadable resources are downloaded
"""

import polars as pl

def downloadable_resources_hits(logs_df):
    """
    Identifies and counts the number of times resources are downloaded,
    excluding irrelevant services.
    """

    downloadable_patterns = [
        ".pdf", ".zip", ".csv", ".shp", ".xls", ".xlsx",
        ".kml", ".kmz", ".tif", ".las", ".jpg",
        "download=true", "request=GetFeature"
    ]

    exclusion_patterns = [
        "geocoding", "search"
    ]

    exclusion_condition = pl.lit(False)
    for pattern in exclusion_patterns:
        exclusion_condition = (
                exclusion_condition |
                pl.col("request_url").str.contains(pattern)
        )
    logs_df = logs_df.filter(~exclusion_condition)

    combined_condition = pl.lit(False)
    for pattern in downloadable_patterns:
        combined_condition = (
                combined_condition |
                pl.col("request_url").str.contains(pattern)
        )
    downloads_df = logs_df.filter(combined_condition)

    download_counts = downloads_df.group_by("request_url").agg([
        pl.count().alias("download_count")
    ])

    total_downloads = download_counts["download_count"].sum()
    print(f"Total Downloads: {total_downloads}")
    return total_downloads
