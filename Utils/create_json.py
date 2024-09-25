import json
from pathlib import Path
from metrics.metrics_utils import custom_serializer


def create_json(data):
    data_folder = Path('frontend/public')
    data_folder.mkdir(exist_ok=True)

    json_file_path = data_folder / 'metrics_results.json'

    with open(json_file_path, 'w') as json_file:
        json.dump(data, json_file, default=custom_serializer)

    print(f'JSON stored in: {json_file_path}')
