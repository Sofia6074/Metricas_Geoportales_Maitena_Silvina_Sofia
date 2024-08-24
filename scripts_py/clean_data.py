
import polars as pl

def formatear_fecha(df):
    return df.with_columns([pl.col("timestamp").str.strptime(pl.Datetime, "%d/%b/%Y:%H:%M:%S %z", strict=False).dt.strftime("%d-%m-%Y %H:%M:%S").alias("timestamp")])

def eliminar_nulos(df):
    return df.drop_nulls()

def eliminar_peticiones_internas(df):
    return df.filter(~((pl.col("ip") == "127.0.0.1") & (pl.col("request_method") == "OPTIONS") & (pl.col("url") == "*")))
