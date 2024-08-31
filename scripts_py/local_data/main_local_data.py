import polars as pl
from scripts_py.common.log_to_csv import convert_log_to_csv
from scripts_py.common.clean_data import filtrar_datos, inicializar_total_registros
from scripts_py.common.normalize_data import normalizar_datos

LOG_FILE_PATH = 'C:/WebServerLogs/access.log'
CSV_FILE_PATH = 'C:/WebServerLogs/access.csv'
CLEANED_NORMALIZED_CSV_DIR = 'C:/WebServerLogs/'
CLEANED_NORMALIZED_CSV_FILE = 'cleaned_normalized_local_data.csv'


def main():
    print("Cargando los datos locales...")
    convert_log_to_csv(LOG_FILE_PATH, CSV_FILE_PATH)

    df = pl.read_csv(CSV_FILE_PATH)
    print("Limpiando los datos locales...")
    inicializar_total_registros(df)
    df = filtrar_datos(df)

    print("Normalizando los datos locales...")
    df_normalized = normalizar_datos(df)
    write_cleaned_normalized_data(df_normalized)


def write_cleaned_normalized_data(df):
    output_file_path = CLEANED_NORMALIZED_CSV_DIR + CLEANED_NORMALIZED_CSV_FILE
    df.write_csv(output_file_path)
    print(f"{CLEANED_NORMALIZED_CSV_FILE} creado en {CLEANED_NORMALIZED_CSV_DIR}")


if __name__ == "__main__":
    main()