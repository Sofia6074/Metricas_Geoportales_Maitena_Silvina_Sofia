from polars import pl

def filtrar_URLs_vacias(df):
    return df.filter(pl.col("url").is_not_null())

def count_frecuencia_url(df):
    return df.group_by("url").agg(pl.col("url").count().alias("visit_count"))

df_frecuencia_url_ordenado = count_frecuencia_url(filtrar_URLs_vacias(df_limpio)).sort("visit_count", descending=True)
df_frecuencia_url_ordenado.head(10)

df_frecuencia_url_ordenado.write_csv("frecuencia_paginas_visitadas.csv")
#df_frecuencia_url_ordenado.top_k(10, by="visit_count")