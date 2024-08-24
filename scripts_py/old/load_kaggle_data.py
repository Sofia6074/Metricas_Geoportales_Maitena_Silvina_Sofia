import os
from kaggle.api.kaggle_api_extended import KaggleApi

kaggle_json_path = os.path.expanduser('~/.kaggle/kaggle.json')

if not os.path.exists(kaggle_json_path):
    raise FileNotFoundError(f'El archivo kaggle.json no se encuentra en {kaggle_json_path}. Coloca el archivo en esta ubicación.')

# Permisos para Mac
os.chmod(kaggle_json_path, 0o600)

api = KaggleApi()
api.authenticate()
dataset_name = 'sofiamartinez222324/infraestructura-de-datos-espaciales-uy'
api.dataset_download_files(dataset_name, path='.', unzip=True)