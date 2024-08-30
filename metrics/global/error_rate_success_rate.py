import polars as pl


def main():
    df = pl.read_csv("/Users/admin/Documents/TesisArchivo/parsed_logs_with_headers.csv")

    df = df.with_columns(pl.col("status_code").cast(pl.Int32))

    success_df = df.filter(
        (pl.col("status_code") >= 200) & (pl.col("status_code") < 300)
    )
    error_df = df.filter((pl.col("status_code") >= 400) & (pl.col("status_code") < 600))

    total_requests = df.shape[0]
    success_count = success_df.shape[0]
    error_count = error_df.shape[0]

    success_rate = (success_count / total_requests) * 100
    error_rate = (error_count / total_requests) * 100

    print(f"Success Rate: {success_rate:.2f}%")
    print(f"Error Rate: {error_rate:.2f}%")


if __name__ == "__main__":
    main()
