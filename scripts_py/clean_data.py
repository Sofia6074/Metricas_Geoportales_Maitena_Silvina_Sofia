"""#### Limpieza de nulos
Vemos que hay una gran cantidad de celdas con valores en nulo, por lo que vamos a filtrar las filas que tienen valores vacíos en al menos una columna para poder visualizar algunos casos
"""

from functools import reduce

mask = reduce(lambda a, b: a | b, [df[col].is_null() for col in df.columns])
rows_con_nulos = df.filter(mask)
rows_con_nulos.head(10)

"""Encontramos algo que nos llama la atención y es que están apareciendo algunos status code dentro de la columna de urls, lo podemos verificar con la siguiente consulta:"""

unique_status = df.select("url").unique()
print(unique_status.sort("url", descending=True))

"""Vamos a filtrar las filas que contienen códigos de estado en la columna `url` para que que valores tienen esas filas"""

status_codes = ["200","408"]
urls_with_status_codes = df.filter(pl.col("url").is_in(status_codes))

print(urls_with_status_codes)

"""Investigando los casos null en profundidad nos encontramos con que son casos borde. Los de status 200 son casos con datos no legibles o anómalos y los de status 408 parecen ser una solicitud incompleta o malformada debido a un timeout en la solicitud, problemas de parte del cliente o error de red o conexión.

Este tipo de entradas en los logs a menudo se descartan en análisis de tráfico web ya que no proporcionan datos útiles sobre las solicitudes válidas. Sin embargo, pueden ser útiles para detectar problemas de red o errores en los clientes.

Por lo que vamos a filtrar las filas donde la columna `url` contenga los valores `200` o `408`
"""

df_sin_nulos = df.filter(
    ~pl.col("url").str.contains('200') & ~pl.col("url").str.contains('408')
)

print(df_sin_nulos)
df_sin_nulos.write_csv("log_sin_nulos.csv")

"""Verificamos que ya no hayan más casos de datos nulos"""

cantidad_nulos = df_sin_nulos.null_count()
cantidad_nulos

"""#### Limpieza de peticiones internas

Es común que el servidor web realice solicitudes internas a sí mismo para verificar su configuración, comprobar capacidades o realizar tareas de mantenimiento. El uso de * en las solicitudes OPTIONS puede estar asociado con estas operaciones internas.

La URL * en este contexto se refiere a una solicitud genérica que no está dirigida a una URL específica. En el caso de los métodos OPTIONS, * puede estar siendo usado para indicar que la solicitud no está dirigida a un recurso específico o es un marcador de posición para una solicitud interna del servidor.

Dado que las solicitudes provienen de 127.0.0.1 (localhost), parece que estas solicitudes son internas, probablemente generadas por el propio servidor para comprobar su configuración o para otros fines administrativos

Antes de eliminar esos registros, veamos cuántas peticiones internas hay en el dataset y qué porcentaje representan sobre la cantidad total de peticiones con los registros nulos ya filtrados.
"""

peticiones_internas = df_sin_nulos.filter(
    (pl.col("ip") == "127.0.0.1") &
    (pl.col("request_method") == "OPTIONS") &
    (pl.col("url") == "*")
)

cantidad_peticiones_internas = peticiones_internas.shape[0]
total_registros = df_sin_nulos.shape[0]
porcentaje_peticiones_internas = (cantidad_peticiones_internas / total_registros) * 100

print(f"Cantidad de peticiones internas: {cantidad_peticiones_internas}")
print(f"Cantidad de registros (sin nulos): {total_registros}")
print(f"Porcentaje de peticiones internas: {porcentaje_peticiones_internas:.2f}%")

df_sin_nulos

df_no_peticiones_internas = df_sin_nulos.filter(
    ~((pl.col("ip") == "127.0.0.1") & (pl.col("request_method") == "OPTIONS") & (pl.col("url") == "*"))
)

df_no_peticiones_internas.head(10)

"""#### Limpieza de datos estáticos
Los archivos estáticos como .css o .png a menudo no contienen información crítica para el análisis de tráfico web o para la detección de problemas específicos en el comportamiento del usuario. Además muchas veces, los archivos estáticos son solicitados por bots o sistemas automatizados.

Datos innecesarios o erróneos pueden aumentar el tamaño de los datasets, haciendo que el procesamiento sea más lento. Limpiar los datos reduce el tamaño y mejora la eficiencia del procesamiento.
"""

df_filtrado_datos_estaticos = filtrar_datos(df_no_peticiones_internas)

"""#### Limpieza de robots"""

df_filtrado_robots = filtrar_robots_y_crawlers(df_filtrado_datos_estaticos)

"""#### Limpieza de servicios de mapas"""

df_filtrado = filtrar_solicitudes_servicios_mapas(df_filtrado_robots)

cantidad_registros = df_filtrado.select(pl.all().count())
cantidad_registros
