import polars as pl
from scripts_py.classes.logger import Logger

logger_instance = Logger(__name__).get_logger()

def downloadable_resources_hits_ratio(logs_df):
    """
    Identifies and counts the number of times resources are downloaded, excluding irrelevant services.
    """

    # Contenido descargable
    downloadable_patterns = [
        ".pdf", ".zip", ".csv", ".shp", ".xls", ".xlsx", ".kml", ".kmz", ".tif", ".las", ".jpg",
        "download=true", "request=GetFeature"
    ]

    # Para evitar los servicios de consulta
    exclusion_patterns = [
        "geocoding", "search"
    ]

    exclusion_condition = pl.lit(False)
    for pattern in exclusion_patterns:
        exclusion_condition = exclusion_condition | pl.col("request_url").str.contains(pattern)
    logs_df = logs_df.filter(~exclusion_condition)

    combined_condition = pl.lit(False)
    for pattern in downloadable_patterns:
        combined_condition = combined_condition | pl.col("request_url").str.contains(pattern)
    downloads_df = logs_df.filter(combined_condition)

    download_counts = downloads_df.group_by("request_url").agg([
        pl.count().alias("download_count")
    ])

    total_downloads = download_counts["download_count"].sum()
    logger_instance.info(f"Descargas totales: {total_downloads}")
