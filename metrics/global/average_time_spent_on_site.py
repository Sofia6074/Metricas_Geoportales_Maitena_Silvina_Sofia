import polars as pl
from datetime import timedelta


def main():
    df = pl.read_csv("/Users/admin/Documents/TesisArchivo/parsed_logs_with_headers.csv")

    df = df.with_columns(
        pl.col("timestamp")
        .str.strptime(pl.Datetime, "%d/%b/%Y:%H:%M:%S %z")
        .alias("datetime")
    )

    df = df.sort(by=["user_agent", "datetime"])

    df = df.with_columns(
        [
            (pl.col("datetime") - pl.col("datetime").shift(1)).alias("time_diff"),
            (pl.col("user_agent") != pl.col("user_agent").shift(1)).alias(
                "new_user_agent"
            ),
        ]
    )

    df = df.with_columns(
        (
            (pl.col("time_diff") > timedelta(minutes=30)) | pl.col("new_user_agent")
        ).alias("new_session")
    )

    df = df.with_columns(pl.col("new_session").cum_sum().alias("session_id"))

    df = df.with_columns(
        (pl.col("datetime").shift(-1) - pl.col("datetime")).alias("time_spent")
    )

    valid_sessions = df.filter(
        pl.col("time_spent").is_not_null() & (pl.col("time_spent") > timedelta(0))
    )

    average_time_per_page = valid_sessions.group_by("session_id").agg(
        pl.col("time_spent").mean().alias("average_time_per_page")
    )

    global_average_time_per_page = average_time_per_page["average_time_per_page"].mean()

    print(f"User Average Time Spent per Page: {global_average_time_per_page}")


if __name__ == "__main__":
    main()
