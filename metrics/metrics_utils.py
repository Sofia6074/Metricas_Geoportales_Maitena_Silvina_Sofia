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

def filtrar_urls_vacias(logs_df):
    """
    Filtra las URLs vacías en el DataFrame.
    """
    return logs_df.filter(pl.col("request_url").is_not_null())
