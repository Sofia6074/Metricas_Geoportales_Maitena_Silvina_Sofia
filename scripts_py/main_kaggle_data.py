
import polars as pl
from scripts_py.load_kaggle_data import download_data
from scripts_py.clean_data import filtrar_datos, inicializar_total_registros
from scripts_py.normalize_data import normalizar_datos


def main():
    # Define file paths
    csv_file_path = 'C:/WebServerLogs/'

    # Download data
    print("Descargando los datos desde Kaggle...")
    #download_data(csv_file_path)
    df = pl.read_csv(csv_file_path + 'filebeat-geoportal-access.csv') 
    df.write_csv(csv_file_path + 'cleaned_normalized_kaggle_data.csv')
   
    # Clean data
    #print("Limpiando los datos...")
    #inicializar_total_registros(df)
    #df = filtrar_datos(df)

    # Normalize data
    #print("Normalizando los datos...")
    #df_normalized = normalizar_datos(df)

    #df_normalized.write_csv(csv_file_path + 'cleaned_normalized_kaggle_data.csv')
    print("cleaned_normalized_kaggle_data.csv creado")

if __name__ == "__main__":
    main()
