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

    pages_per_session = df.group_by("session_id").agg(
        pl.col("request").count().alias("pages_viewed")
    )

    average_pages_per_session = pages_per_session["pages_viewed"].mean()

    print(f"Average Pages Viewed per Session: {average_pages_per_session:.2f}")


if __name__ == "__main__":
    main()
