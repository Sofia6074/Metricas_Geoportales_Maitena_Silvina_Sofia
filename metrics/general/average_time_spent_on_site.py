import polars as pl
from metrics.metrics_utils import calculate_sessions


def formatear_average_time(average_time):
    hours, remainder = divmod(average_time.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    formatted_string = f"{int(hours)} horas {int(minutes)} minutos {int(seconds)} segundos"
    return formatted_string


def calculate_average_time_spent_on_site(logs_df):
    """
    Calcula el tiempo promedio que los usuarios pasan en el sitio por página.
    """
    session_df = calculate_sessions(logs_df)
    session_df = session_df.with_columns([
        (pl.col("timestamp").shift(-1) - pl.col("timestamp")).alias("time_spent")
    ])

    # Filtrar sesiones con time_spent mayor a 12 horas (12 * 60 minutos * 60 segundos * 1_000_000 microsegundos)
    time_threshold = 12 * 60 * 60 * 1_000_000
    session_df = session_df.filter(
        (pl.col("time_spent").is_not_null()) &
        (pl.col("time_spent") > 0) &
        (pl.col("time_spent") <= time_threshold)
    )

    valid_sessions = session_df.filter(
        (pl.col("time_spent").is_not_null()) & (pl.col("time_spent") > 0)
    )

    average_time_per_page = valid_sessions.group_by("session_id").agg(
        pl.col("time_spent").mean().alias("average_time_per_page")
    )

    global_average_time_per_page = average_time_per_page.select(
        pl.col("average_time_per_page").mean().alias("global_average_time_per_page")
    )[0, "global_average_time_per_page"]

    print(f"User Average Time Spent per Page: {formatear_average_time(global_average_time_per_page)}")
