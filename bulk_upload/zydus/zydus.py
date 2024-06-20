import json
import logging
import os
from datetime import datetime

import pandas as pd
import paramiko
import requests

DEBUG = True

url = 'https://api.c.qa.platform.leucinetech.com/v1/objects'
token = 'eyJhbGciOiJIUzUxMiJ9.eyJsYXN0TmFtZSI6IiIsIm9yZ2FuaXNhdGlvbklkIjoxNjE3NjI1NDAxLCJyb2xlcyI6W3siaWQiOiI2IiwibmFtZSI6IlBST0NFU1NfUFVCTElTSEVSIn1dLCJlbXBsb3llZUlkIjoic2hpdmFtX3B1Ymxpc2hlciIsImhhc1NldENoYWxsZW5nZVF1ZXN0aW9uIjp0cnVlLCJmaXJzdE5hbWUiOiJzaGl2YW1fcHVibGlzaGVyIiwiaWQiOjQ3OTkzNjk1MzAxMjY3NDU2MCwiZmFjaWxpdHlJZHMiOlsxNjQzMTAzNTAzLDE2NDMxMDM1MDIsMTY0MzEwMzUwMSwxNjE2MzY3ODAzLDE2MTYzNjc4MDIsMTYxNjM2NzgwMV0sInNlcnZpY2VJZCI6ImM2ZDgyODViNzJhODRlZmI4ZmJkNjA4YzdjYWRhNDg0IiwiZmFjaWxpdGllcyI6W3siaWQiOiIxNjE2MzY3ODAxIiwibmFtZSI6IkJhbmdhbG9yZSJ9LHsiaWQiOiIxNjE2MzY3ODAzIiwibmFtZSI6IkRlbGhpIn0seyJpZCI6IjE2NDMxMDM1MDIiLCJuYW1lIjoiTG9uZG9uIn0seyJpZCI6IjE2MTYzNjc4MDIiLCJuYW1lIjoiTXVtYmFpIn0seyJpZCI6IjE2NDMxMDM1MDEiLCJuYW1lIjoiTmV3IFlvcmsifSx7ImlkIjoiMTY0MzEwMzUwMyIsIm5hbWUiOiJTeWRuZXkifV0sImN1cnJlbnRGYWNpbGl0eUlkIjoxNjE2MzY3ODAxLCJyb2xlTmFtZXMiOlsiUFJPQ0VTU19QVUJMSVNIRVIiXSwianRpIjoiOTU5ZjljNTk5ZTM0NDhjNWJkMGFkNDJjY2E0YjA3NDciLCJ1c2VybmFtZSI6InNoaXZhbV9wdWJsaXNoZXIiLCJzdWIiOiJzaGl2YW1fcHVibGlzaGVyIiwiaWF0IjoxNzEzODEyMDc0LCJleHAiOjE3MTM4MTkyNzR9.tIovzBaJwEGojC5mKvpCdZ25PEPOB7Faap35xT4IbY2QTreeA1BEYZWfI91fJpgmJY3TOG_dN6s5R2zEjYEkgg'

headers = {
    'Authorization': f'Bearer {token}'
}

# Get the current date
now = datetime.now()
year = now.strftime("%Y")
month = now.strftime("%m")

base_dir = f"/home/moksh/workspace/leucine/projects/py-experiment-lab/ftp-etl"

# Create directory path for year and month
log_dir = f"{base_dir}/logs/{year}/{month}"

# Create directories if they don't exist
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Set log filename with daily date
log_filename = now.strftime(f"{log_dir}/etl_log_%Y-%m-%d.log")

# Set up logging
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s %(message)s')

key_mappings = {
    'product': {
        'collection': 'products',
        'object_type_id': '64b915a599fc053904cd8ac4',
        'property': {
            'product_code': '64b915a599fc053904cd8ac6',
            'product_name': '64b915a599fc053904cd8ac5',
            'product_strength': '65b102f867da5d27af539da4'
        },
        'relation': {
        }
    },
    'equipment': {
        'collection': 'equipmentss',
        'object_type_id': '6626390ea4e03262557ab15d',
        'property': {
            'equipment_id': '6626390ea4e03262557ab15e',
            'equipment_name': '6626390ea4e03262557ab15f',
            'serial_number': '6626397ca4e03262557ab167',
            'asset_id': '66263992a4e03262557ab168',
            'model_number': '66263966a4e03262557ab166',
            'manufacturer': '6626393ca4e03262557ab165'
        },
        'relation': {
        }
    },
    'room': {
        'collection': 'rooms',
        'object_type_id': '64f017e9f76e1173399acd22',
        'property': {
            'room_number': '64f017e9f76e1173399acd24',
            'room_name': '64f017e9f76e1173399acd23',
            'process': '64f01800f76e1173399acd2a'
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
    # process_product_files("/home/moksh/misc/temp/kvk/product.xlsx")
    # process_room_files("/home/moksh/misc/temp/kvk/room.xlsx")
    process_equipment_files("/home/moksh/misc/temp/zydus/assets.xlsx")


def _get_epoch(date_str, format='%m/%d/%Y'):
    date_str = date_str.replace('\r', '')
    datetime_obj = datetime.strptime(date_str, format)
    epoch = int(datetime_obj.timestamp())
    return epoch


def process_product_files(file_path):
    _display(f"Starting to Process files: {file_path}", True)
    _display("")
    valid_extensions = {".xls", ".xlsx"}
    is_valid_file_extension = os.path.splitext(file_path)[1].lower() in valid_extensions
    if not is_valid_file_extension:
        _display(f"Skipping file {file_path} due to invalid file extension.")
    with open(file_path, 'rb') as f:
        content = pd.read_excel(f)
    for index, row in content.iterrows():
        product_code = str(row['product_code']).strip() if pd.notna(row['product_code']) else ''
        product_name = str(row['product_name']).strip() if pd.notna(row['product_name']) else ''
        product_strength = str(row['product_strength']).strip() if pd.notna(row['product_strength']) else ''
        _process_product(product_code, product_name, product_strength)
    print(content)
    # lines = [line.split("|") for line in content.strip().split("\n")]


def _process_product(product_code, product_name, product_strength):
    product_obj = _get_product(product_code)
    if product_obj is None:
        product_obj = _create_product(product_code, product_name, product_strength)
    return product_obj


def _get_product(product_code):
    product_obj = None
    product_collection = key_mappings['product']['collection']
    product_code_id = key_mappings['product']['property']['product_code']
    params = {
        'collection': {product_collection},
        'filters': _build_search_filter(product_code_id, product_code)
    }
    product_url = f"{url}/partial"
    response = requests.get(product_url, params=params, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if len(data):
            product_obj = data[0]
    else:
        print('Error:', response.status_code)
        print('Error:', response.text)
    return product_obj


def _create_product(product_code, product_name, product_strength):
    product_obj = None
    product_url = f"{url}"
    data = {
        'objectTypeId': key_mappings['product']['object_type_id'],
        'properties': {
            key_mappings['product']['property']['product_code']: product_code,
            key_mappings['product']['property']['product_name']: product_name,
            key_mappings['product']['property']['product_strength']: product_strength
        }
    }
    response = requests.post(product_url, json=data, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if len(data):
            product_obj = {
                'id': data['id'],
                'collection': key_mappings['product']['collection'],
                'externalId': data['externalId'],
                'displayName': data['displayName']
            }
    else:
        print('Error:', response.status_code)
        print('Error:', response.text)
    return product_obj


def process_room_files(file_path):
    _display(f"Starting to Process files: {file_path}", True)
    _display("")
    valid_extensions = {".xls", ".xlsx"}
    is_valid_file_extension = os.path.splitext(file_path)[1].lower() in valid_extensions
    if not is_valid_file_extension:
        _display(f"Skipping file {file_path} due to invalid file extension.")
    with open(file_path, 'rb') as f:
        content = pd.read_excel(f)
    for index, row in content.iterrows():
        room_number = str(row['room_number']).strip() if pd.notna(row['room_number']) else ''
        room_name = str(row['room_name']).strip() if pd.notna(row['room_name']) else ''
        process = str(row['process']).strip() if pd.notna(row['process']) else ''
        _process_room(room_number, room_name, process)
    print(content)
    # lines = [line.split("|") for line in content.strip().split("\n")]


def _process_room(room_number, room_name, process):
    room_obj = _get_room(room_number)
    if room_obj is None:
        room_obj = _create_room(room_number, room_name, process)
    return room_obj


def _get_room(room_number):
    room_obj = None
    room_collection = key_mappings['room']['collection']
    room_number_id = key_mappings['room']['property']['room_number']
    params = {
        'collection': {room_collection},
        'filters': _build_search_filter(room_number_id, room_number)
    }
    room_url = f"{url}/partial"
    response = requests.get(room_url, params=params, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if len(data):
            room_obj = data[0]
    else:
        print('Error:', response.status_code)
        print('Error:', response.text)
    return room_obj


def _create_room(room_number, room_name, process):
    room_obj = None
    room_url = f"{url}"
    data = {
        'objectTypeId': key_mappings['room']['object_type_id'],
        'properties': {
            key_mappings['room']['property']['room_number']: room_number,
            key_mappings['room']['property']['room_name']: room_name,
            key_mappings['room']['property']['process']: process
        }
    }
    response = requests.post(room_url, json=data, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if len(data):
            room_obj = {
                'id': data['id'],
                'collection': key_mappings['room']['collection'],
                'externalId': data['externalId'],
                'displayName': data['displayName']
            }
    else:
        print('Error:', response.status_code)
        print('Error:', response.text)
    return room_obj


def process_equipment_files(file_path):
    _display(f"Starting to Process files: {file_path}", True)
    _display("")
    valid_extensions = {".xls", ".xlsx"}
    is_valid_file_extension = os.path.splitext(file_path)[1].lower() in valid_extensions
    if not is_valid_file_extension:
        _display(f"Skipping file {file_path} due to invalid file extension.")
    with open(file_path, 'rb') as f:
        content = pd.read_excel(f)
    for index, row in content.iterrows():
        equipment_id = str(row['equipment_id']).strip() if pd.notna(row['equipment_id']) else ''
        equipment_name = str(row['equipment_name']).strip() if pd.notna(row['equipment_name']) else ''
        serial_number = str(row['serial_number']).strip() if pd.notna(row['serial_number']) else ''
        asset_id = str(row['asset_id']).strip() if pd.notna(row['asset_id']) else ''
        model_number = str(row['model_number']).strip() if pd.notna(row['model_number']) else ''
        manufacturer = str(row['manufacturer']).strip() if pd.notna(row['manufacturer']) else ''
        _process_equipment(equipment_id, equipment_name, serial_number, asset_id, model_number, manufacturer)
    print(content)
    # lines = [line.split("|") for line in content.strip().split("\n")]


def _process_equipment(equipment_id, equipment_name, serial_number, asset_id, model_number, manufacturer):
    equipment_obj = _get_equipment(equipment_id)
    if equipment_obj is None:
        equipment_obj = _create_equipment(equipment_id, equipment_name, serial_number, asset_id, model_number,
                                          manufacturer)
    return equipment_obj


def _get_equipment(equipment_id):
    equipment_obj = None
    equipment_collection = key_mappings['equipment']['collection']
    equipment_id_id = key_mappings['equipment']['property']['equipment_id']
    params = {
        'collection': {equipment_collection},
        'filters': _build_search_filter(equipment_id_id, equipment_id)
    }
    equipment_url = f"{url}/partial"
    response = requests.get(equipment_url, params=params, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if len(data):
            equipment_obj = data[0]
    else:
        print('Error:', response.status_code)
        print('Error:', response.text)
    return equipment_obj


def _create_equipment(equipment_id, equipment_name, serial_number, asset_id, model_number, manufacturer):
    equipment_obj = None
    equipment_url = f"{url}"
    data = {
        'objectTypeId': key_mappings['equipment']['object_type_id'],
        'properties': {
            key_mappings['equipment']['property']['equipment_id']: equipment_id,
            key_mappings['equipment']['property']['equipment_name']: equipment_name,
            key_mappings['equipment']['property']['serial_number']: serial_number,
            key_mappings['equipment']['property']['asset_id']: asset_id,
            key_mappings['equipment']['property']['model_number']: model_number,
            key_mappings['equipment']['property']['manufacturer']: manufacturer,
        }
    }
    response = requests.post(equipment_url, json=data, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if len(data):
            equipment_obj = {
                'id': data['id'],
                'collection': key_mappings['equipment']['collection'],
                'externalId': data['externalId'],
                'displayName': data['displayName']
            }
    else:
        print('Error:', response.status_code)
        print('Error:', response.text)
    return equipment_obj


lambda_handler(None, None)
