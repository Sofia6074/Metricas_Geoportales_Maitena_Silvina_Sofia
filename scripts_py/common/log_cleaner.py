"""
Este módulo proporciona funciones para limpiar y transformar los datos de los logs del servidor web.
"""

from pyspark.sql.functions import (
    col, regexp_extract, regexp_replace, lower, to_timestamp
)
from scripts_py.classes.logger import Logger

logger = Logger(__name__).get_logger()

def initialize_total_entries(data_frame):
    """
    Initializes and returns the total number of entries in the dataframe.
    """
    initial_total_entries = data_frame.count()
    logger.info("Total registros inicializados: %d", initial_total_entries)
    return initial_total_entries

def format_logs(data_frame):
    """
    Formats the logs by extracting relevant fields from the raw log string.
    """
    logger.info("Formateando los logs")

    log_pattern = (
        r'(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] "(GET|POST|PUT|DELETE) (.*?) (HTTP/\d\.\d)" '
        r'(\d+) (\d+) "(.*?)" "(.*?)" Time (\d+)"'
    )

    data_frame = data_frame.withColumn("_c0", regexp_replace(col("_c0"), '""', '"')) \
                           .withColumn("_c0", regexp_replace(col("_c0"), '%3A', ':')) \
                           .withColumn("_c0", regexp_replace(col("_c0"), '%2C', ','))

    return data_frame.withColumn("ip", regexp_extract(col("_c0"), log_pattern, 1)) \
                     .withColumn("timestamp", regexp_extract(col("_c0"), log_pattern, 2)) \
                     .withColumn("request_method", regexp_extract(col("_c0"), log_pattern, 3)) \
                     .withColumn("request_url", regexp_extract(col("_c0"), log_pattern, 4)) \
                     .withColumn("http_version", regexp_extract(col("_c0"), log_pattern, 5)) \
                     .withColumn("status_code", regexp_extract(col("_c0"), log_pattern, 6)
                                 .cast("integer")) \
                     .withColumn("response_size", regexp_extract(col("_c0"), log_pattern, 7)
                                 .cast("integer")) \
                     .withColumn("referer", regexp_extract(col("_c0"), log_pattern, 8)) \
                     .withColumn("user_agent", regexp_extract(col("_c0"), log_pattern, 9)) \
                     .withColumn("response_time", regexp_extract(col("_c0"), log_pattern, 10)
                                 .cast("integer"))

def remove_duplicates(data_frame):
    """
    Removes duplicate rows from the dataframe.
    """
    logger.info("Eliminando entradas duplicadas")
    return data_frame.dropDuplicates()

def handle_null_values(data_frame):
    """
    Handles null values by filling or dropping them.
    """
    logger.info("Manejando valores nulos")
    data_frame = data_frame.na.fill({'status_code': 0, 'response_size': 0, 'response_time': 0})
    return data_frame.na.drop(subset=["ip", "timestamp", "request_method", "request_url"])

def normalize_urls(data_frame):
    """
    Normalizes the URLs by converting them to lowercase.
    """
    logger.info("Normalizando URLs")
    return data_frame.withColumn("request_url", lower(col("request_url")))

def convert_timestamp(data_frame):
    """
    Converts the timestamp to datetime format.
    """
    logger.info("Convirtiendo timestamp a formato datetime")
    return data_frame.withColumn(
        "timestamp", to_timestamp(col("timestamp"), "dd/MMM/yyyy:HH:mm:ss Z")
    )

def count_filters_robots(data_frame):
    """
    Counts the number of entries associated with known bots and crawlers.
    """
    total_entries = data_frame.count()

    entries_googlebot = data_frame.filter(col("user_agent").contains('Googlebot')).count()
    entries_baiduspider = data_frame.filter(col("user_agent").contains('Baiduspider')).count()
    entries_agesic_crawler = data_frame.filter(col("user_agent").contains('agesic-crawler')).count()

    percentile_googlebot = (entries_googlebot / total_entries) * 100
    percentile_baiduspider = (entries_baiduspider / total_entries) * 100
    percentile_agesic_crawler = (entries_agesic_crawler / total_entries) * 100

    logger.info(
        "Registros 'Googlebot': %d (%.2f%%)", entries_googlebot, percentile_googlebot
    )
    logger.info(
        "Registros 'Baiduspider': %d (%.2f%%)", entries_baiduspider, percentile_baiduspider
    )
    logger.info(
        "Registros 'agesic-crawler': %d (%.2f%%)", entries_agesic_crawler, percentile_agesic_crawler
    )

def filter_googlebot(data_frame):
    """
    Filters out requests made by Googlebot from the dataframe.
    """
    return data_frame.filter(~col("user_agent").contains('Googlebot'))

def filter_baiduspider(data_frame):
    """
    Filters out requests made by Baiduspider from the dataframe.
    """
    return data_frame.filter(~col("user_agent").contains('Baiduspider'))

def filter_agesic_crawler(data_frame):
    """
    Filters out requests made by Agesic Crawler from the dataframe.
    """
    return data_frame.filter(~col("user_agent").contains('agesic-crawler'))

def filter_robots_and_crawlers(data_frame):
    """
    Filters out known bots and crawlers from the dataframe.
    """
    logger.info("Eliminando los siguientes registros de bots y crawlers:")
    count_filters_robots(data_frame)
    data_frame = filter_googlebot(data_frame)
    data_frame = filter_baiduspider(data_frame)
    data_frame = filter_agesic_crawler(data_frame)
    return data_frame

def filter_jcemediabox(data_frame):
    """
    Filters jcemediabox static files from the dataframe.
    """
    return data_frame.filter(~col("request_url").contains('/plugins/system/jcemediabox/'))

def filter_css(data_frame):
    """
    Filters css static files from the dataframe.
    """
    regex = r'\.css$'
    return data_frame.filter(~col("request_url").rlike(regex))

def filter_js(data_frame):
    """
    Filters js static files from the dataframe.
    """
    regex = r'\.js$'
    return data_frame.filter(~col("request_url").rlike(regex))

def filter_png(data_frame):
    """
    Filters png static files from the dataframe.
    """
    regex = r'\.png$'
    return data_frame.filter(~col("request_url").rlike(regex))

def filter_jpg(data_frame):
    """
    Filters jpg static files from the dataframe.
    """
    regex = r'\.jpg$'
    return data_frame.filter(~col("request_url").rlike(regex))

def filter_gif(data_frame):
    """
    Filters gif static files from the dataframe.
    """
    regex = r'\.gif$'
    return data_frame.filter(~col("request_url").rlike(regex))

def filter_favicon(data_frame):
    """
    Filters favicon static files from the dataframe.
    """
    regex = r'favicon\.ico$'
    return data_frame.filter(~col("request_url").rlike(regex))

def count_static_files(data_frame):
    """
    Counts the number of static files (e.g., CSS, JS, images) in the dataframe.
    """
    total_entries = data_frame.count()

    filters = {
        "jcemediabox": r'/plugins/system/jcemediabox/',
        "css": r'\.css$',
        "js": r'\.js$',
        "png": r'\.png$',
        "jpg": r'\.jpg$',
        "gif": r'\.gif$',
        "favicon": r'favicon\.ico$'
    }

    def count_entries_url(data_frame, pattern):
        """
        Counts the number of entries in the dataframe that match the given URL pattern.
        """
        return data_frame.filter(col("request_url").rlike(pattern)).count()

    for nombre, patron in filters.items():
        entries = count_entries_url(data_frame, patron)
        percentage = (entries / total_entries) * 100 if entries else 0
        logger.info("Registros '%s': %d (%.2f%%)", nombre, entries, percentage)

def filter_static_files(data_frame):
    """
    Filters out static files (e.g., CSS, JS, images) from the dataframe.
    """
    logger.info("Eliminando los siguientes archivos estaticos:")
    count_static_files(data_frame)
    data_frame = filter_jcemediabox(data_frame)
    data_frame = filter_css(data_frame)
    data_frame = filter_js(data_frame)
    data_frame = filter_png(data_frame)
    data_frame = filter_jpg(data_frame)
    data_frame = filter_gif(data_frame)
    data_frame = filter_favicon(data_frame)
    return data_frame

def remove_internal_requests(data_frame):
    """
    Removes internal requests (e.g., from localhost) from the dataframe.
    """
    logger.info("Eliminando peticiones internas")
    return data_frame.filter(~((col("ip") == "127.0.0.1") &
                               (col("request_method") == "OPTIONS") &
                               (col("request_url") == "*")))

def preview_logs(data_frame, num=5):
    """
    Displays a preview of the first few rows of the dataframe.
    """
    logger.info("Revisando los primeros registros del DataFrame")
    data_frame.show(num, truncate=True)

def log_cleaner(data_frame):
    """
    Cleans the log data by applying various filters and transformations.
    """
    try:
        data_frame = format_logs(data_frame)
        data_frame = remove_duplicates(data_frame)
        data_frame = handle_null_values(data_frame)
        data_frame = filter_robots_and_crawlers(data_frame)
        data_frame = filter_static_files(data_frame)
        data_frame = remove_internal_requests(data_frame)
        data_frame = normalize_urls(data_frame)
        data_frame = convert_timestamp(data_frame)
        preview_logs(data_frame)
        logger.info("Se han limpiado los datos correctamente")
    except (ValueError, TypeError) as exc:
        logger.error("Error específico al limpiar los datos: %s", str(exc))
    except Exception as exc: # pylint: disable=W0703
        logger.error("Error inesperado al limpiar los datos: %s", str(exc))

    return data_frame
