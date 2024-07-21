import polars as pl;

df = pl.read_csv('archivo.csv')
df_filtrado = df.filter(pl.col("status_code") == 200)
df_cleaned = df_filtrado.with_columns([
    pl.col("timestamp").str.strptime(pl.Datetime, "%d/%b/%Y:%H:%M:%S %z", strict=False).alias("timestamp")
])

print(df_cleaned)
