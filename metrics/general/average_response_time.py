"""
Este módulo contiene la función para calcular el tiempo de respuesta promedio,
máximo y mínimo a partir de un DataFrame de logs.
"""

from pyspark.sql import functions as F

def calculate_average_response_time(logs_df):
    """
    Calcula el tiempo de respuesta promedio, máximo y mínimo.
    """
    avg_response_time = logs_df.agg(
        F.mean("response_time").alias("avg_response_time")
    ).collect()[0]["avg_response_time"]

    max_response_time = logs_df.agg(
        F.max("response_time").alias("max_response_time")
    ).collect()[0]["max_response_time"]

    min_response_time = logs_df.agg(
        F.min("response_time").alias("min_response_time")
    ).collect()[0]["min_response_time"]

    print(f"Average Response Time: {avg_response_time:.2f} ms")
    print(f"Maximum Response Time: {max_response_time:.2f} ms")
    print(f"Minimum Response Time: {min_response_time:.2f} ms")
