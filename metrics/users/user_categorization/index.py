"""
Este módulo clasifica a los usuarios en perfiles (alto, medio, ocasional) basados en
sus visitas y tiempo de permanencia en la página web. También realiza cálculos adicionales
relacionados con los perfiles de usuario.
"""

import polars as pl
from metrics.metrics_utils import calculate_sessions

def print_user_profile_counts(logs_df):
    """
    Imprime el porcentaje de usuarios por perfil.
    """
    total_users = logs_df.select(pl.col("ip")).n_unique()
    profile_counts = logs_df.group_by("user_profile").agg([
        pl.col("ip").n_unique().alias("count")
    ])

    profile_counts = profile_counts.with_columns([
        (pl.col("count") / total_users * 100).alias("percentage")
    ])

    print(profile_counts)

def calculate_user_categorized_metrics(logs_df):
    """
    Placeholder para cálculos adicionales basados en usuarios categorizados.
    """
    # Este espacio está reservado para cálculos adicionales futuros.
    # logs_df se utilizará aquí en el futuro.
    logs_df.head(10)

def classify_user_profiles(logs_df):
    """
    Clasifica a los usuarios en tres perfiles según el número
    de visitas y el tiempo total de permanencia.
    """

    sessions_df = calculate_sessions(logs_df)

    logs_df.write_csv("resultado_logs.csv")

    user_stats = sessions_df.group_by("ip").agg([
        pl.col("session_id").count().alias("visits"),
        pl.col("time_diff").sum().alias("total_time_spent")
    ])

    high_profile_threshold = user_stats.select(pl.col("visits").quantile(0.8)).item()
    average_profile_threshold = user_stats.select(pl.col("visits").quantile(0.5)).item()

    user_stats = user_stats.with_columns([
        pl.when(
            (pl.col("visits") > high_profile_threshold) &
            (pl.col("total_time_spent") > high_profile_threshold)
        ).then(1)  # Perfil alto
        .when(
            (pl.col("visits") > average_profile_threshold) &
            (pl.col("total_time_spent") > average_profile_threshold)
        ).then(2)  # Perfil medio
        .otherwise(3).alias("user_profile")  # Usuarios ocasionales
    ])

    logs_df = logs_df.join(user_stats.select(["ip", "user_profile"]), on="ip", how="left")

    print_user_profile_counts(logs_df)

    calculate_user_categorized_metrics(logs_df)

    return logs_df
