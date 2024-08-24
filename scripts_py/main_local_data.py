
from load_data import load_data
from clean_data import formatear_fecha, eliminar_nulos, eliminar_peticiones_internas
from filter_data import filtrar_robots_y_crawlers, inicializar_total_registros
from normalize_data import normalizar_datos
import polars as pl


def main():
    # Define file paths
    log_file_path = 'C:/WebServerLogs/access.log'
    csv_file_path = 'C:/WebServerLogs/access.csv'
    csv_cleaned_normalized_file_path = 'C:/WebServerLogs/'

    # Load data
    print("Cargando los datos locales...")
    load_data(log_file_path, csv_file_path)
    df = pl.read_csv(csv_file_path)

    # Clean data
    print("Limpiando los datos...")
    inicializar_total_registros(df)
    df = formatear_fecha(df)
    df = eliminar_nulos(df)
    df = eliminar_peticiones_internas(df)
    df = filtrar_robots_y_crawlers(df)

    # Normalize data
    print("Normalizando los datos...")
    df_normalized = normalizar_datos(df)

    df_normalized.write_csv(csv_cleaned_normalized_file_path + 'cleaned_normalized_data.csv')


if __name__ == "__main__":
    main()
