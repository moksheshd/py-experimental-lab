import json
import logging
import math
import os
from datetime import datetime

import pandas as pd
import requests

DEBUG = True
url = 'https://api.andover.uat.platform.leucinetech.com/v1/objects'
token = 'eyJhbGciOiJIUzUxMiJ9.eyJsYXN0TmFtZSI6IkwiLCJvcmdhbmlzYXRpb25JZCI6MTYxNzYyNTQwMSwicm9sZXMiOlt7ImlkIjoiMSIsIm5hbWUiOiJBQ0NPVU5UX09XTkVSIn1dLCJlbXBsb3llZUlkIjoiTC1hY2NvdW50Lm93bmVyLjAyIiwiaGFzU2V0Q2hhbGxlbmdlUXVlc3Rpb24iOnRydWUsImZpcnN0TmFtZSI6Ik11c3RhcSIsImlkIjoxNjQzMTAzMzAwLCJmYWNpbGl0eUlkcyI6Wy0xLDE2NDMxMDM1MDMsMTY0MzEwMzUwMiwxNjQzMTAzNTAxLDE2MTYzNjc4MDMsMTYxNjM2NzgwMiwxNjE2MzY3ODAxXSwic2VydmljZUlkIjoiYzZkODI4NWI3MmE4NGVmYjhmYmQ2MDhjN2NhZGE0ODQiLCJmYWNpbGl0aWVzIjpbeyJpZCI6Ii0xIiwibmFtZSI6Ikdsb2JhbCBQb3J0YWwifSx7ImlkIjoiMTYxNjM2NzgwMSIsIm5hbWUiOiJCYW5nYWxvcmUifSx7ImlkIjoiMTYxNjM2NzgwMyIsIm5hbWUiOiJEZWxoaSJ9LHsiaWQiOiIxNjQzMTAzNTAyIiwibmFtZSI6IkxvbmRvbiJ9LHsiaWQiOiIxNjE2MzY3ODAyIiwibmFtZSI6Ik11bWJhaSJ9LHsiaWQiOiIxNjQzMTAzNTAxIiwibmFtZSI6Ik5ldyBZb3JrIn0seyJpZCI6IjE2NDMxMDM1MDMiLCJuYW1lIjoiU3lkbmV5In1dLCJjdXJyZW50RmFjaWxpdHlJZCI6MTY0MzEwMzUwMSwicm9sZU5hbWVzIjpbIkFDQ09VTlRfT1dORVIiXSwiZW1haWwiOiJhby4wMkBtYWlsaW5hdG9yLmNvbSIsImp0aSI6IjVhOGQ3NThjOGVhNTRlMjNiNmRmZDQ0NDE0MTliMmNjIiwidXNlcm5hbWUiOiJhY2NvdW50Lm93bmVyLjAyIiwic3ViIjoiYWNjb3VudC5vd25lci4wMiIsImlhdCI6MTcxODg4MDA4NCwiZXhwIjoxNzE4OTY2NDg0fQ.TK0Np-aElwXtnpaOUlvlA8K-VGC-kKZ6Qq7R1p63cCHJ5LOV3qRBGSRB-3gwET_Ba4DwwSxewoT8VHc4S8vhHA'

headers = {
    'Authorization': f'Bearer {token}'
}

# Get the current date
now = datetime.now()
year = now.strftime("%Y")
month = now.strftime("%m")

base_dir = f"/home/moksh/workspace/leucine/projects/py-experimental-lab/bulk_upload/andover"
file_path = f"{base_dir}/assets.csv"

# Create directory path for year and month
log_dir = f"{base_dir}/logs/{year}/{month}"

# Create directories if they don't exist
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Set log filename with daily date
log_filename = now.strftime(f"{log_dir}/bulk_upload_andover_log_%Y-%m-%d.log")

# Set up logging
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s %(message)s')

key_mappings = {
    'asset': {
        'collection': 'assets',
        'object_type_id': '6673ca23c2a5eb45690d25e3',
        'property': {
            'serial_number': '6673ca23c2a5eb45690d25e5',
            'asset_name': '6673ca23c2a5eb45690d25e4',
            'legacy_number': '6673ca38c2a5eb45690d25eb',
            'next_due_date': '6673cac5c2a5eb45690d25ec',
            'manufacturer': '6673cae1c2a5eb45690d25ee',
            'model': '6673caeec2a5eb45690d25ef'
        },
        'relation': {
        }
    }
}


def _display(message: str, override=False):
    logging.info(message)
    if DEBUG or override:
        print(message)


def _build_search_filter(key, value):
    return json.dumps({"op": "AND", "fields": [{"field": f"searchable.{key}", "op": "EQ", "values": [f"{value}"]}]})


def lambda_handler(event, context):
    process_assets_files(file_path)


def _get_epoch(date_str, format='%m/%d/%Y'):
    date_str = date_str.replace('\r', '')
    datetime_obj = datetime.strptime(date_str, format)
    epoch = int(datetime_obj.timestamp())
    return epoch


def process_assets_files(file_path):
    _display(f"Starting to Process files: {file_path}", True)
    _display("")
    valid_extensions = {".xls", ".xlsx", ".csv"}
    file_extension = os.path.splitext(file_path)[1].lower()
    is_valid_file_extension = file_extension in valid_extensions
    if not is_valid_file_extension:
        _display(f"Skipping file {file_path} due to invalid file extension.")
    with open(file_path, 'rb') as f:
        if file_extension == '.csv':
            content = pd.read_csv(f)
        elif file_extension in ['.xls', '.xlsx']:
            content = pd.read_excel(f)
    for index, row in content.iterrows():
        print(row)
        serial_number = str(row['serial_number']).strip() if pd.notna(row['serial_number']) else ''
        asset_name = str(row['asset_name']).strip() if pd.notna(row['asset_name']) else ''
        legacy_number = str(row['legacy_number']).strip() if pd.notna(row['legacy_number']) else ''
        next_due_date = (
            str(_get_epoch(row['next_due_date'].strip(), '%Y-%m-%d'))
            if row['next_due_date'] and not (
                        isinstance(row['next_due_date'], float) and math.isnan(row['next_due_date'])) and len(
                row['next_due_date']) > 1
            else None
        )
        manufacturer = str(row['manufacturer']).strip() if pd.notna(row['manufacturer']) else ''
        model = str(row['model']).strip() if pd.notna(row['model']) else ''
        _process_asset(serial_number, asset_name, legacy_number, next_due_date, manufacturer, model)
    print(content)


def _process_asset(serial_number, asset_name, legacy_number, next_due_date, manufacturer, model):
    asset_obj = _get_asset(serial_number)
    if asset_obj is None:
        asset_obj = _create_asset(serial_number, asset_name, legacy_number, next_due_date, manufacturer, model)
    return asset_obj


def _get_asset(serial_number):
    asset_obj = None
    asset_collection = key_mappings['asset']['collection']
    legacy_number_id = key_mappings['asset']['property']['serial_number']
    params = {
        'collection': {asset_collection},
        'filters': _build_search_filter(legacy_number_id, serial_number)
    }
    asset_url = f"{url}/partial"
    response = requests.get(asset_url, params=params, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if len(data):
            asset_obj = data[0]
    else:
        print('Error:', response.status_code)
        print('Error:', response.text)
    return asset_obj


def _create_asset(serial_number, asset_name, legacy_number, next_due_date, manufacturer, model):
    asset_obj = None
    asset_url = f"{url}"
    data = {
        'objectTypeId': key_mappings['asset']['object_type_id'],
        'properties': {
            key_mappings['asset']['property']['serial_number']: serial_number,
            key_mappings['asset']['property']['asset_name']: asset_name,
            key_mappings['asset']['property']['legacy_number']: legacy_number,
            key_mappings['asset']['property']['next_due_date']: next_due_date,
            key_mappings['asset']['property']['manufacturer']: manufacturer,
            key_mappings['asset']['property']['model']: model
        },
        'reason': 'Leucine Bot created this object.'
    }
    response = requests.post(asset_url, json=data, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if len(data):
            asset_obj = {
                'id': data['id'],
                'collection': key_mappings['asset']['collection'],
                'externalId': data['externalId'],
                'displayName': data['displayName']
            }
    else:
        print('Error:', response.status_code)
        print('Error:', response.text)
    return asset_obj


lambda_handler(None, None)
