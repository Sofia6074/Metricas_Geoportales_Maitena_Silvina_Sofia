"""
Este módulo carga, limpia y normaliza datos locales del servidor web y los guarda en un archivo CSV.
"""

import polars as pl
from scripts_py.common.log_to_csv import convert_log_to_csv
from scripts_py.common.normalize_data import normalizar_datos

LOG_FILE_PATH = 'C:/WebServerLogs/access.log'
CSV_FILE_PATH = 'C:/WebServerLogs/access.csv'
CLEANED_NORMALIZED_CSV_DIR = 'C:/WebServerLogs/'
CLEANED_NORMALIZED_CSV_FILE = 'cleaned_normalized_local_data.csv'


def main():
    """
    Función principal para cargar, limpiar y normalizar los datos locales.
    """
    print("Cargando los datos locales...")
    convert_log_to_csv(LOG_FILE_PATH, CSV_FILE_PATH)

    data_frame = pl.read_csv(CSV_FILE_PATH)
    print("Limpiando los datos locales...")

    # inicializar_total_registros(data_frame)

    print("Normalizando los datos locales...")
    df_normalized = normalizar_datos(data_frame)
    write_cleaned_normalized_data(df_normalized)


def write_cleaned_normalized_data(data_frame):
    """
    Escribe los datos limpiados y normalizados en un archivo CSV.
    """
    output_file_path = CLEANED_NORMALIZED_CSV_DIR + CLEANED_NORMALIZED_CSV_FILE
    data_frame.write_csv(output_file_path)
    print(f"{CLEANED_NORMALIZED_CSV_FILE} creado en {CLEANED_NORMALIZED_CSV_DIR}")


if __name__ == "__main__":
    main()
