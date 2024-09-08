"""
Este módulo contiene la función para calcular el tiempo de respuesta promedio,
máximo y mínimo a partir de un DataFrame de logs.
"""

import polars as pl

def calculate_average_response_time(logs_df):
    """
    Calcula el tiempo de respuesta promedio, máximo y mínimo utilizando Polars.
    """
    avg_response_time = logs_df.select(
        pl.col("response_time").mean().alias("avg_response_time")
    )[0, "avg_response_time"]

    max_response_time = logs_df.select(
        pl.col("response_time").max().alias("max_response_time")
    )[0, "max_response_time"]

    min_response_time = logs_df.select(
        pl.col("response_time").min().alias("min_response_time")
    )[0, "min_response_time"]

    print(f"Average Response Time: {avg_response_time:.2f} ms")
    print(f"Maximum Response Time: {max_response_time:.2f} ms")
    print(f"Minimum Response Time: {min_response_time:.2f} ms")
