"""
Este módulo contiene utilidades para el cálculo de sesiones en los logs de mapas.
"""

from pyspark.sql import Window
from pyspark.sql.functions import unix_timestamp, col, when, count, expr, lag

def calculate_sessions(map_requests_df):
    """
    Calcula las sesiones únicas basadas en la IP y el agente de usuario.
    """
    map_requests_df = map_requests_df.withColumn(
        "session_id", col("ip") + "_" + col("user_agent")
    )

    window_spec = Window.partitionBy("session_id").orderBy(
        unix_timestamp("timestamp", "dd/MMM/yyyy:HH:mm:ss Z")
    )

    map_requests_df = map_requests_df.withColumn(
        "prev_timestamp", lag("timestamp").over(window_spec)
    )
    map_requests_df = map_requests_df.withColumn(
        "time_diff",
        unix_timestamp("timestamp", "dd/MMM/yyyy:HH:mm:ss Z") -
        unix_timestamp("prev_timestamp", "dd/MMM/yyyy:HH:mm:ss Z")
    )
    map_requests_df = map_requests_df.withColumn(
        "new_session", when(col("time_diff") > 1800, 1).otherwise(0)
    )
    map_requests_df = map_requests_df.withColumn(
        "session_number",
        count("new_session").over(window_spec.rowsBetween(Window.unboundedPreceding, 0))
    )
    return map_requests_df.withColumn(
        "unique_session_id", expr("concat(ip, '_', user_agent, '_', session_number)")
    )
