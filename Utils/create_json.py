"""
This module contains functionality to create a JSON file from given data.
"""

import json
from pathlib import Path
from metrics.metrics_utils import custom_serializer


def create_json(data):
    """
    Creates a JSON file from the provided data and
    stores it in 'frontend/public'.

    Args:
        data (dict): The data to be serialized and written to the JSON file.
    """
    data_folder = Path('frontend/public')
    data_folder.mkdir(exist_ok=True)

    json_file_path = data_folder / 'metrics_results.json'

    # Specify the encoding explicitly to avoid platform-specific issues
    with open(json_file_path, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, default=custom_serializer)

    print(f'JSON stored in: {json_file_path}')
