import polars as pl
from scripts_py.log_to_csv import convert_log_to_csv
from scripts_py.clean_data import filtrar_datos, inicializar_total_registros
from scripts_py.normalize_data import normalizar_datos


def main():
    # Define file paths
    log_file_path = 'C:/WebServerLogs/access.log'
    csv_file_path = 'C:/WebServerLogs/access.csv'
    csv_cleaned_normalized_file_path = 'C:/WebServerLogs/'

    # Load data
    print("Cargando los datos locales...")
    convert_log_to_csv(log_file_path, csv_file_path)

    df = pl.read_csv(csv_file_path)

    # Clean data
    print("Limpiando los datos...")
    inicializar_total_registros(df)
    df = filtrar_datos(df)

    # Normalize data
    print("Normalizando los datos...")
    df_normalized = normalizar_datos(df)

    df_normalized.write_csv(csv_cleaned_normalized_file_path + 'cleaned_normalized_local_data.csv')
    print("cleaned_normalized_local_data.csv creado")

if __name__ == "__main__":
    main()
