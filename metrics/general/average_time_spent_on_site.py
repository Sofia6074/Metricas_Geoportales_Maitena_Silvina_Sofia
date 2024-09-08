import polars as pl
from metrics.metrics_utils import calculate_sessions, format_average_time, filter_session_outliers


def calculate_average_time_spent_on_site(logs_df):
    """
    Calcula el tiempo promedio que los usuarios pasan en el sitio.
    """
    session_df = calculate_sessions(logs_df)
    session_df = session_df.with_columns([
        (pl.col("timestamp").shift(-1) - pl.col("timestamp")).alias("time_spent")
    ])

    filter_session_outliers(session_df)

    valid_sessions = session_df.filter(
        (pl.col("time_spent").is_not_null()) & (pl.col("time_spent") > 0)
    )

    total_time_per_session = valid_sessions.group_by("session_id").agg(
        pl.col("time_spent").sum().alias("total_time_spent_on_site")
    )

    global_average_time_spent_on_site = total_time_per_session.select(
        pl.col("total_time_spent_on_site").mean().alias("global_average_time_spent_on_site")
    )[0, "global_average_time_spent_on_site"]

    print(f"User Average Time Spent on Site: {format_average_time(global_average_time_spent_on_site)}")
