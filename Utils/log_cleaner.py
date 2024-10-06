"""
This module provides functions to clean and transform
web server log data using Polars.
"""
from datetime import timedelta

import polars as pl

def initialize_total_entries(data_frame):
    """
    Initializes and returns the total number of entries in the dataframe.
    """
    initial_total_entries = data_frame.height
    print("Total entries initialized: %d", initial_total_entries)
    return initial_total_entries


def format_logs(data_frame: pl.DataFrame) -> pl.DataFrame:
    """
    Formats the logs by extracting relevant fields from the raw log string.
    """
    print("Formatting logs")

    log_pattern = (
        r'(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] '
        r'"(\S+)\s(\S+)\s(HTTP/\d\.\d)" (\d+) (\d+) "(.*?)" "(.*?)" Time (\d+)'
    )

    # Initial replacements
    data_frame = data_frame.with_columns(
        pl.col("column_1").str.replace_all('""', '"')
    ).with_columns(
        pl.col("column_1").str.replace_all('%3A', ':')
    ).with_columns(
        pl.col("column_1").str.replace_all('%2C', ',')
    )

    # Extract data using regular expressions
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
    print("Removing duplicate entries")
    return data_frame.unique()


def handle_null_values(data_frame: pl.DataFrame) -> pl.DataFrame:
    """
    Handles null values by filling or dropping them.
    """
    print("Handling null values")

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
    print("Normalizing URLs")
    return data_frame.with_columns(pl.col("request_url").str.to_lowercase())


def convert_timestamp(data_frame: pl.DataFrame) -> pl.DataFrame:
    """
    Converts the timestamp to datetime format while preserving the original time.
    """
    print("Converting timestamp to datetime format")

    data_frame = data_frame.with_columns(
        pl.col("timestamp")
        .str.strptime(pl.Datetime, format="%d/%b/%Y:%H:%M:%S %z",
                      strict=False)  # Parses the timestamp with timezone info
        .dt.convert_time_zone("America/Montevideo")  # Converts to the correct local timezone
    )

    data_frame = data_frame.sort("timestamp")

    return data_frame


def count_filters_robots(data_frame: pl.DataFrame):
    """
    Counts the number of entries associated with known bots and crawlers.
    """
    total_entries = data_frame.height

    entries_googlebot = data_frame.filter(
        pl.col("user_agent").str.contains('Googlebot')
    ).height
    entries_baiduspider = data_frame.filter(
        pl.col("user_agent").str.contains('Baiduspider')
    ).height
    entries_semrushbot = data_frame.filter(
        pl.col("user_agent").str.contains('SemrushBot')
    ).height
    entries_agesic_crawler = data_frame.filter(
        pl.col("user_agent").str.contains('agesic-crawler')
    ).height

    percentile_googlebot = (entries_googlebot / total_entries) * 100
    percentile_baiduspider = (entries_baiduspider / total_entries) * 100
    percentile_semrushbot = (entries_semrushbot / total_entries) * 100
    percentile_agesic_crawler = (entries_agesic_crawler / total_entries) * 100

    print(
        "Googlebot records: %d (%.2f%%)", entries_googlebot, percentile_googlebot
    )
    print(
        "Baiduspider records: %d (%.2f%%)", entries_baiduspider, percentile_baiduspider
    )
    print(
        "SemrushBot records: %d (%.2f%%)", entries_semrushbot, percentile_semrushbot
    )
    print(
        "agesic-crawler records: %d (%.2f%%)", entries_agesic_crawler, percentile_agesic_crawler
    )


def filter_robots_and_crawlers(data_frame: pl.DataFrame) -> pl.DataFrame:
    """
    Filters out known bots and crawlers from the dataframe.
    """
    print("Removing the following bot and crawler records:")
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
    print("Removing the following static files:")
    static_file_patterns = [
        r'/plugins/system/jcemediabox/', r'\.css$', r'\.js$',
        r'\.png$', r'\.gif$', r'favicon\.ico$'
    ]

    for pattern in static_file_patterns:
        data_frame = data_frame.filter(~pl.col("request_url").str.contains(pattern))

    return data_frame


def remove_internal_requests(data_frame: pl.DataFrame) -> pl.DataFrame:
    """
    Removes internal requests (e.g., from localhost)
    or requests with method OPTIONS and URL * from the dataframe.
    """
    print("Removing internal requests")
    return data_frame.filter(
        ~((pl.col("ip") == "127.0.0.1") |
          ((pl.col("request_method") == "OPTIONS") & (pl.col("request_url") == "*")))
    )


def preview_logs(data_frame: pl.DataFrame, num=5):
    """
    Displays a preview of the first few rows of the dataframe.
    """
    print("Previewing the first rows of the DataFrame")
    print(data_frame.head(num))

def filter_invalid_ips(data_frame: pl.DataFrame) -> pl.DataFrame:
    print("Filtrando IPs malformadas o sospechosas")
    invalid_ip_patterns = [
        r'^0\.',  # Direcciones IP inválidas
        r'^192\.168\.',  # Rango de IPs privadas
        r'^10\.',  # Otro rango privado
        r'^172\.(1[6-9]|2[0-9]|3[0-1])\.'  # Rango privado adicional
    ]
    for pattern in invalid_ip_patterns:
        data_frame = data_frame.filter(~pl.col("ip").str.contains(pattern))
    return data_frame


def filter_invalid_user_agents(data_frame: pl.DataFrame) -> pl.DataFrame:
    print("Filtrando User Agents inválidos")
    data_frame = data_frame.filter(
        pl.col("user_agent").str.contains(r'.{6,}')
    )
    return data_frame

def filter_suspicious_durations(data_frame: pl.DataFrame) -> pl.DataFrame:
    print("Filtrando duraciones sospechosas")
    data_frame = data_frame.filter(
        (pl.col("response_time") > timedelta(milliseconds=10)) &
        (pl.col("response_time") < timedelta(hours=1))
    )
    return data_frame

def filter_invalid_status_codes(data_frame: pl.DataFrame) -> pl.DataFrame:
    print("Filtrando códigos de estado HTTP inválidos")
    data_frame = data_frame.filter(
        (pl.col("status_code") >= 100) & (pl.col("status_code") <= 599)
    )
    return data_frame

def filter_invalid_referers(data_frame: pl.DataFrame) -> pl.DataFrame:
    print("Filtrando referers inválidos")
    data_frame = data_frame.filter(
        pl.col("referer").str.contains(r'^https?://')
    )
    return data_frame


def filter_outliers(data_frame):
    data_frame = data_frame.filter(
        (pl.col("time_spent").is_not_null()) &
        (pl.col("time_diff").is_not_null()) &
        (pl.col("time_spent") > timedelta(seconds=0)) &
        (pl.col("time_diff") > timedelta(seconds=0))
    )

    q_low_time_spent = data_frame.select(
        pl.col("time_spent").quantile(0.01)
    ).item()

    q_high_time_spent = data_frame.select(
        pl.col("time_spent").quantile(0.99)
    ).item()

    q_low_time_diff = data_frame.select(
        pl.col("time_diff").quantile(0.01)
    ).item()

    q_high_time_diff = data_frame.select(
        pl.col("time_diff").quantile(0.99)
    ).item()

    data_frame = data_frame.filter(
        (pl.col("time_spent") > q_low_time_spent) &
        (pl.col("time_spent") < q_high_time_spent) &
        (pl.col("time_diff") > q_low_time_diff) &
        (pl.col("time_diff") < q_high_time_diff)
    )

    return data_frame

def calculate_sessions(data_frame):
    """
    Calculates unique sessions based on:
        - IP
        - User Agent
        - 30-minute threshold between searches.
    """
    print("Calculating unique sessions")
    data_frame_with_sessions = data_frame.with_columns([
        pl.concat_str([pl.col("ip"), pl.col("user_agent")]).alias("session_id")
    ])

    data_frame_with_sessions = data_frame_with_sessions.sort(
        by=["session_id", "timestamp"]
    )

    data_frame_with_sessions = data_frame_with_sessions.with_columns([
        pl.col("timestamp").diff().over("session_id").alias("time_diff")
    ])

    # Crear una nueva sesión si la diferencia entre una request
    # y la anterior es mayor a 30 minutos
    data_frame_with_sessions = data_frame_with_sessions.with_columns([
        (pl.col("time_diff") > timedelta(minutes=30))
        .cum_sum()
        .over("session_id")
        .fill_null(0)
        .alias("session_segment")
    ])

    data_frame_with_sessions = data_frame_with_sessions.with_columns([
        (pl.col("session_id") + "_" +
         pl.col("session_segment").cast(pl.Utf8)).alias("unique_session_id")
    ])

    data_frame_with_sessions_timespent = data_frame_with_sessions.group_by("unique_session_id").agg([
        (pl.col("timestamp").max() - pl.col("timestamp").min())
        .alias("time_spent")
    ])

    data_frame_with_sessions = data_frame_with_sessions.join(data_frame_with_sessions_timespent, on="unique_session_id")

    # data_frame_with_sessions = filter_outliers(data_frame_with_sessions)

    return data_frame_with_sessions



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
        data_frame = filter_invalid_ips(data_frame)
        data_frame = filter_invalid_user_agents(data_frame)
        data_frame = filter_suspicious_durations(data_frame)
        data_frame = filter_invalid_status_codes(data_frame)
        data_frame = filter_invalid_referers(data_frame)
        data_frame = calculate_sessions(data_frame)
        data_frame = filter_outliers(data_frame)
        preview_logs(data_frame)
        print("Data has been successfully cleaned")
    except (ValueError, TypeError) as exc:
        print("Specific error while cleaning data: %s", str(exc))
    except Exception as exc:  # pylint: disable=W0703
        print("Unexpected error while cleaning data: %s", str(exc))

    return data_frame
