"""
Este módulo proporciona funciones para limpiar y transformar
los datos de los logs del servidor web usando Polars.
"""

import polars as pl
from scripts_py.classes.logger import Logger

logger = Logger(__name__).get_logger()


def initialize_total_entries(data_frame):
    """
    Initializes and returns the total number of entries in the dataframe.
    """
    initial_total_entries = data_frame.height
    logger.info("Total registros inicializados: %d", initial_total_entries)
    return initial_total_entries


def format_logs(data_frame: pl.DataFrame) -> pl.DataFrame:
    """
    Formats the logs by extracting relevant fields from the raw log string.
    """
    logger.info("Formateando los logs")

    log_pattern = (
        r'(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] '
        r'"(\S+)\s(\S+)\s(HTTP/\d\.\d)" (\d+) (\d+) "(.*?)" "(.*?)" Time (\d+)'
    )

    # Reemplazos iniciales
    data_frame = data_frame.with_columns(
        pl.col("column_1").str.replace_all('""', '"')
    ).with_columns(
        pl.col("column_1").str.replace_all('%3A', ':')
    ).with_columns(
        pl.col("column_1").str.replace_all('%2C', ',')
    )

    # Extracción de datos utilizando expresiones regulares
    data_frame = data_frame.with_columns([
        pl.col("column_1").str.extract(log_pattern, 1).alias("ip"),
        pl.col("column_1").str.extract(log_pattern, 2).alias("timestamp"),
        pl.col("column_1").str.extract(log_pattern, 3).alias("request_method"),
        pl.col("column_1").str.extract(log_pattern, 4).alias("request_url"),
        pl.col("column_1").str.extract(log_pattern, 5).alias("http_version"),
        pl.col("column_1").str.extract(log_pattern, 6).cast(pl.Int32).alias("status_code"),
        pl.col("column_1").str.extract(log_pattern, 7).cast(pl.Int64).alias("response_size"),
        pl.col("column_1").str.extract(log_pattern, 8).alias("referer"),
        pl.col("column_1").str.extract(log_pattern, 9).alias("user_agent"),
        pl.col("column_1").str.extract(log_pattern, 10).cast(pl.Int64).alias("response_time")
    ])

    data_frame = data_frame.drop("column_1")

    return data_frame


def remove_duplicates(data_frame: pl.DataFrame) -> pl.DataFrame:
    """
    Removes duplicate rows from the dataframe.
    """
    logger.info("Eliminando entradas duplicadas")
    return data_frame.unique()


def handle_null_values(data_frame: pl.DataFrame) -> pl.DataFrame:
    """
    Handles null values by filling or dropping them.
    """
    logger.info("Manejando valores nulos")

    data_frame = data_frame.with_columns([
        pl.col("status_code").fill_null(0),
        pl.col("response_size").fill_null(0),
        pl.col("response_time").fill_null(0)
    ])

    return data_frame.drop_nulls(subset=["ip", "timestamp", "request_method", "request_url"])


def normalize_urls(data_frame: pl.DataFrame) -> pl.DataFrame:
    """
    Normalizes the URLs by converting them to lowercase.
    """
    logger.info("Normalizando URLs")
    return data_frame.with_columns(pl.col("request_url").str.to_lowercase())


def convert_timestamp(data_frame: pl.DataFrame) -> pl.DataFrame:
    """
    Converts the timestamp to datetime format.
    """
    logger.info("Convirtiendo timestamp a formato datetime")
    return data_frame.with_columns(
        pl.col("timestamp").str.strptime(pl.Datetime, format="%d/%b/%Y:%H:%M:%S %z", strict=False)
    )


def count_filters_robots(data_frame: pl.DataFrame):
    """
    Counts the number of entries associated with known bots and crawlers.
    """
    total_entries = data_frame.height

    entries_googlebot = data_frame.filter(
        pl.col("user_agent").str.contains('Googlebot')
    ).height
    entries_baiduspider = (data_frame.filter(
        pl.col("user_agent").str.contains('Baiduspider')
    ).height)
    entries_semrushbot = (data_frame.filter(
        pl.col("user_agent").str.contains('SemrushBot')
    ).height)
    entries_agesic_crawler = data_frame.filter(
        pl.col("user_agent").str.contains('agesic-crawler')
    ).height

    percentile_googlebot = (entries_googlebot / total_entries) * 100
    percentile_baiduspider = (entries_baiduspider / total_entries) * 100
    percentile_semrushbot = (entries_semrushbot / total_entries) * 100
    percentile_agesic_crawler = (entries_agesic_crawler / total_entries) * 100

    logger.info(
        "Registros 'Googlebot': %d (%.2f%%)", entries_googlebot, percentile_googlebot
    )
    logger.info(
        "Registros 'Baiduspider': %d (%.2f%%)", entries_baiduspider, percentile_baiduspider
    )
    logger.info(
        "Registros 'SemrushBot': %d (%.2f%%)", entries_semrushbot, percentile_semrushbot
    )
    logger.info(
        "Registros 'agesic-crawler': %d (%.2f%%)", entries_agesic_crawler, percentile_agesic_crawler
    )


def filter_robots_and_crawlers(data_frame: pl.DataFrame) -> pl.DataFrame:
    """
    Filters out known bots and crawlers from the dataframe.
    """
    logger.info("Eliminando los siguientes registros de bots y crawlers:")
    return data_frame.filter(
        ~pl.col("user_agent").str.contains('Googlebot') &
        ~pl.col("user_agent").str.contains('Baiduspider') &
        ~pl.col("user_agent").str.contains('agesic-crawler') &
        ~pl.col("user_agent").str.contains('SemrushBot')
    )


def filter_static_files(data_frame: pl.DataFrame) -> pl.DataFrame:
    """
    Filters out static files (e.g., CSS, JS, images) from the dataframe.
    """
    logger.info("Eliminando los siguientes archivos estáticos:")
    static_file_patterns = [
        r'/plugins/system/jcemediabox/', r'\.css$', r'\.js$',
        r'\.png$', r'\.jpg$', r'\.gif$', r'favicon\.ico$'
    ]

    for pattern in static_file_patterns:
        data_frame = data_frame.filter(~pl.col("request_url").str.contains(pattern))

    return data_frame


def remove_internal_requests(data_frame: pl.DataFrame) -> pl.DataFrame:
    """
    Removes internal requests (e.g., from localhost)
    or requests with method OPTIONS and URL * from the dataframe.
    """
    logger.info("Eliminando peticiones internas")
    return data_frame.filter(
        ~((pl.col("ip") == "127.0.0.1") |
          ((pl.col("request_method") == "OPTIONS") & (pl.col("request_url") == "*")))
    )


def preview_logs(data_frame: pl.DataFrame, num=5):
    """
    Displays a preview of the first few rows of the dataframe.
    """
    logger.info("Revisando los primeros registros del DataFrame")
    print(data_frame.head(num))


def log_cleaner(data_frame: pl.DataFrame) -> pl.DataFrame:
    """
    Cleans the log data by applying various filters and transformations.
    """
    try:
        initialize_total_entries(data_frame)
        data_frame = format_logs(data_frame)
        data_frame = remove_duplicates(data_frame)
        data_frame = handle_null_values(data_frame)
        data_frame = filter_robots_and_crawlers(data_frame)
        data_frame = filter_static_files(data_frame)
        data_frame = remove_internal_requests(data_frame)
        data_frame = normalize_urls(data_frame)
        data_frame = convert_timestamp(data_frame)
        preview_logs(data_frame)
        initialize_total_entries(data_frame)
        logger.info("Se han limpiado los datos correctamente")
    except (ValueError, TypeError) as exc:
        logger.error("Error específico al limpiar los datos: %s", str(exc))
    except Exception as exc:  # pylint: disable=W0703
        logger.error("Error inesperado al limpiar los datos: %s", str(exc))

    return data_frame
