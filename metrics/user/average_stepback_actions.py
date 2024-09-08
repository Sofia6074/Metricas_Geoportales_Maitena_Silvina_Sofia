import polars as pl
from metrics.metrics_utils import calculate_sessions
from scripts_py.classes.logger import Logger


logger_instance = Logger(__name__).get_logger()

def calculate_average_stepback_actions(logs_df):
    """
    Calcula el promedio de veces que los usuarios vuelven hacia atrás por sesión
    detectando si un usuario regresa a una URL que ya visitó y considerando 3 segundos de tiempo entre solicitudes.
    """

    session_df = calculate_sessions(logs_df)

    # Filtro las solicitudes a GeoServer para centrarnos en la interacción
    # del usuario con la web (que no incluyan solicitudes a servicios de mapas)
    session_df = session_df.filter(
        ~pl.col("request_url").str.contains("geoserver")
    )

    session_df = session_df.with_columns([
        pl.col("timestamp").cast(pl.Datetime).alias("timestamp"),
        pl.col("request_url").shift(1).over("session_id").alias("previous_url"),
        (pl.col("timestamp") - pl.col("timestamp").shift(1).over("session_id")).alias("time_diff")
    ])
    session_df = session_df.with_columns(
        (pl.col("time_diff").cast(pl.Float64) / 1_000_000).alias("time_diff_seconds")
    )

    # Filtrar retrocesos considerando solo los que tienen 3 seg de diferencia de tiempo
    retrocesos = session_df.filter(
        (pl.col("request_url") == pl.col("previous_url")) &
        (pl.col("time_diff_seconds") > 3)
    ).group_by("session_id").agg([pl.count().alias("stepback_count")])

    average_stepback = retrocesos.select(
        pl.col("stepback_count").mean().alias("average_stepback_actions")
    ).with_columns(pl.col("average_stepback_actions").round(2))[0, "average_stepback_actions"]

    logger_instance.info(f"Average Stepback Actions per Session: {average_stepback}")
