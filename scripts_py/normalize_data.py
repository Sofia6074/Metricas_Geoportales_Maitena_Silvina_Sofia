import polars as pl


def formatear_fecha(df):
    return df.with_columns([pl.col("timestamp").str.strptime(pl.Datetime, "%d/%b/%Y:%H:%M:%S %z", strict=False).dt.strftime("%d-%m-%Y %H:%M:%S").alias("timestamp")])

def normalizar_datos(df):
    return formatear_fecha(df)