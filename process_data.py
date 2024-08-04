import polars as pl;

# - - - - - FUNCIONES AUXILIARES
def filtrar_googlebot(df):
    return df.filter(~pl.col("user_agent").str.contains('Googlebot'))

def filtrar_baiduspider(df):
    return df.filter(~pl.col("user_agent").str.contains('Baiduspider'))

def filtrar_agesic_crawler(df):
    return df.filter(~pl.col("user_agent").str.contains('agesic-crawler'))

def filtrar_robots_y_crawlers(df):
    df = filtrar_googlebot(df)
    df = filtrar_baiduspider(df)
    df = filtrar_agesic_crawler(df)
    return df

def filtrar_map_server(df):
    # MapServer es una plataforma de código abierto para publicar datos espaciales y aplicaciones interactivas de mapeo en la web.
    # libcurl es una biblioteca utilizada por MapServer para manejar transferencias de datos a través de varios protocolos de red.
    return df.filter(~pl.col("url").str.contains('/wfsPCN1000.cgi'))

def filtrar_datos(df):
    df = filtrar_robots_y_crawlers(df)
    df = filtrar_map_server(df)
    return df

def filtrar_URLs_vacias(df):
    return df.filter(pl.col("url").is_not_null())

def count_frecuencia_url(df):
    return df.group_by("url").agg(pl.col("url").count().alias("visit_count"))

def formatear_fecha(df):
    return df.with_columns([pl.col("timestamp").str.strptime(pl.Datetime, "%d/%b/%Y:%H:%M:%S %z", strict=False).alias("timestamp")])

# - - - - - MAIN
df = pl.read_csv('archivo.csv')

# Limpieza de datos
df_filtrado = filtrar_datos(df)
df_cleaned = formatear_fecha(df_filtrado)
print(df_cleaned) # .glimpse() para ver string completo

# Páginas más visitadas
df_frecuencia_url_ordenado = count_frecuencia_url(filtrar_URLs_vacias(df_cleaned)).sort("visit_count")
print(df_frecuencia_url_ordenado)


# TO-DO:
# • Análisis de navegación de los usuarios.
# • Identificación de errores en respuestas a solicitudes de páginas web.
