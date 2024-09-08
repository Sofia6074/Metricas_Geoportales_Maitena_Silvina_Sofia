"""
Este módulo contiene utilidades para el cálculo metrics.
"""

import polars as pl


def calculate_sessions(map_requests_df):
    """
    Calcula las sesiones únicas basadas en la IP y el agente de usuario.
    """

    map_requests_df = map_requests_df.with_columns([
        (pl.col("ip") + "_" + pl.col("user_agent")).alias("session_id")
    ])

    map_requests_df = map_requests_df.sort(by=["session_id", "timestamp"])

    map_requests_df = map_requests_df.with_columns([
        pl.col("timestamp").shift(1).over("session_id").alias("prev_timestamp")
    ])

    map_requests_df = map_requests_df.with_columns([
        (pl.col("timestamp") - pl.col("prev_timestamp")).alias("time_diff")
    ])

    # Si el tiempo entre dos timestamps es mayor a 1800 segundos (30 minutos), es una nueva sesión
    map_requests_df = map_requests_df.with_columns([
        pl.when(pl.col("time_diff") > 1800).then(1).otherwise(0).alias("new_session")
    ])

    map_requests_df = map_requests_df.with_columns([
        pl.col("new_session").cum_sum().over("session_id").alias("session_number")
    ])

    map_requests_df = map_requests_df.with_columns([
        (pl.col("ip") + "_" +
         pl.col("user_agent") + "_" +
         pl.col("session_number").cast(pl.Utf8)).alias("unique_session_id")
    ])

    return map_requests_df

def filter_empty_urls(logs_df):
    """
    Filtra las URLs vacías en el DataFrame.
    """
    return logs_df.filter(pl.col("request_url").is_not_null())

def format_average_time(average_time):
    hours, remainder = divmod(average_time.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    formatted_string = f"{int(hours)} horas {int(minutes)} minutos {int(seconds)} segundos"
    return formatted_string

def filter_session_outliers(logs_df):
    """
    Filtra los outliers de sesiones mayores a 12 horas (12 * 60 minutos * 60 segundos * 1_000_000 microsegundos)
    """
    time_threshold = 12 * 60 * 60 * 1_000_000
    session_df = logs_df.filter(
        (pl.col("time_spent").is_not_null()) &
        (pl.col("time_spent") > 0) &
        (pl.col("time_spent") <= time_threshold)
    )