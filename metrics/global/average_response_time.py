import polars as pl


def main():
    df = pl.read_csv("/Users/admin/Documents/TesisArchivo/parsed_logs_with_headers.csv")

    df = df.with_columns(pl.col("response_time").cast(pl.Float64))

    average_response_time = df["response_time"].mean()

    max_response_time = df["response_time"].max()

    min_response_time = df["response_time"].min()

    print(f"Average Response Time: {average_response_time:.2f} ms")
    print(f"Maximum Response Time: {max_response_time:.2f} ms")
    print(f"Minimum Response Time: {min_response_time:.2f} ms")


if __name__ == "__main__":
    main()
