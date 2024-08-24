"""### Normalización"""

df_limpio = formatear_fecha(df_filtrado)
df_limpio.write_csv("log_limpio.csv")
df_limpio