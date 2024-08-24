import os
from kaggle.api.kaggle_api_extended import KaggleApi

def download_data(csv_file_path):
    dataset_name = 'sofiamartinez222324/infraestructura-de-datos-espaciales-uy'
    kaggle_json_path = os.path.expanduser('~\.kaggle\kaggle.json')
    print("kaggle_json_path: " + kaggle_json_path)

    if not os.path.exists(kaggle_json_path):
        raise FileNotFoundError(f'El archivo kaggle.json no se encuentra en: {kaggle_json_path}.')

    # Permisos para Mac
    os.chmod(kaggle_json_path, 0o600)

    if not os.path.exists(csv_file_path):
        os.makedirs(csv_file_path)

    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset_name, path=csv_file_path, unzip=True)

    downloaded_files = [f for f in os.listdir(csv_file_path) if f.endswith('.csv')]
    if downloaded_files:
        print(f"Archivos descargados correctamente en {csv_file_path}:")
        for file in downloaded_files:
            print(f"- {file}")
        return True
    else:
        print(f"No se encontraron archivos CSV en {csv_file_path} después de la descarga.")
        return False