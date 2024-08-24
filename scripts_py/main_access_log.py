#run load_local_data_windows.py
#run clean_data_py

df = pl.read_csv(csv_file_path)

"""## Main"""

df = pl.read_csv(csv_file_path)

"""#### Reconocimiento del data frame
Veamos cómo está compuesto el dataframe
"""

df.schema

"""Mostramos las primeras 5 filas del dataframe"""

df.head(5)

"""Veremos la cantidad de registros con los que cuenta el dataset"""

cantidad_registros = df.select(pl.all().count())
cantidad_registros

"""Podemos notar que para las columnas de request_method, status_code, size y user_agent se muestra una cantidad de registros menor comparado con las columnas de ip, timestamp, url y http_version.

Veremos con que cantidad de valores vacíos nos encontramos en el df, para verificar si esta es la razón de que nos aparezcan registros faltantes.
"""

cantidad_nulos = df.null_count()
cantidad_nulos

total_registros = cantidad_registros + cantidad_nulos
total_registros

"""Como sospechábamos, la diferencia entre las cantidades de registros se debe a los valores en nulo.

El porcentaje de valores nulos por columna es el siguiente:
"""

porcentaje_nulos = (cantidad_nulos / total_registros) * 100
porcentaje_nulos