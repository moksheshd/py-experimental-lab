import json
import os
from datetime import datetime

import boto3
import pandas as pd
import paramiko
import requests

DEBUG = True
# SFTP connection details
HOST = 'sftpval.valent.com'
# USERNAME = 'SAPEBRQA'
# PASSWORD = 'X3Wmbnde4we'

USERNAME = 'SAPEBR'
PASSWORD = 'zuKABN34Bhjk'

PO_DIR = 'Outbound'
INVENTORY_DIR = 'Inventory'

dynamodb = boto3.client('dynamodb')
uat_token = 'eyJhbGciOiJIUzUxMiJ9.eyJsYXN0TmFtZSI6IiIsIm9yZ2FuaXNhdGlvbklkIjoxNjE3NjI1NDAxLCJyb2xlcyI6W3siaWQiOiIxIiwibmFtZSI6IkFDQ09VTlRfT1dORVIifV0sImVtcGxveWVlSWQiOiJCb3QwMSIsImhhc1NldENoYWxsZW5nZVF1ZXN0aW9uIjp0cnVlLCJmaXJzdE5hbWUiOiJMZXVjaW5lIEJvdCIsImlkIjo0Mjk5MjQzMTQ4NjM0NDM5NjgsImZhY2lsaXR5SWRzIjpbLTEsMTYxNjM2NzgwNCwxNjE2MzY3ODAzLDE2MTYzNjc4MDIsMTYxNjM2NzgwMV0sInNlcnZpY2VJZCI6ImM2ZDgyODViNzJhODRlZmI4ZmJkNjA4YzdjYWRhNDg0IiwiZmFjaWxpdGllcyI6W3siaWQiOiItMSIsIm5hbWUiOiJHbG9iYWwgUG9ydGFsIn0seyJpZCI6IjE2MTYzNjc4MDEiLCJuYW1lIjoiVW5pdCAxIn0seyJpZCI6IjE2MTYzNjc4MDIiLCJuYW1lIjoiVW5pdCAyIn0seyJpZCI6IjE2MTYzNjc4MDMiLCJuYW1lIjoiVW5pdCAzIn0seyJpZCI6IjE2MTYzNjc4MDQiLCJuYW1lIjoiVW5pdCA0In1dLCJjdXJyZW50RmFjaWxpdHlJZCI6MTYxNjM2NzgwMiwicm9sZU5hbWVzIjpbIkFDQ09VTlRfT1dORVIiXSwianRpIjoiMTFlOTk2Y2UyNzQ3NDJjZWFiZjI0ZmRjYThhNTczNWMiLCJ1c2VybmFtZSI6ImJvdDAxIiwic3ViIjoiYm90MDEiLCJpYXQiOjE3MDE0MjA1MDYsImV4cCI6MTcwOTk3NDEwNn0.ohOofJoIaSVNENpEmEGtCwGoTD9PwbKKvcuY3QcjMcaK0X5rE-vjyYnQJqzVRhcIYrizI8297DdtjaAwv17i0A'
uat_url = 'https://api.valent.uat.platform.leucinetech.com/v1/objects'
# demo_token = 'eyJhbGciOiJIUzUxMiJ9.eyJsYXN0TmFtZSI6IiIsIm9yZ2FuaXNhdGlvbklkIjoxNjE3NjI1NDAxLCJyb2xlcyI6W3siaWQiOiIxIiwibmFtZSI6IkFDQ09VTlRfT1dORVIifV0sImVtcGxveWVlSWQiOiJCb3QwMSIsImhhc1NldENoYWxsZW5nZVF1ZXN0aW9uIjp0cnVlLCJmaXJzdE5hbWUiOiJMZXVjaW5lIEJvdCIsImlkIjo0Mjk5MjIxNzc1NTA5ODMxNjgsImZhY2lsaXR5SWRzIjpbLTEsMTY0MzEwMzUwMywxNjQzMTAzNTAyLDE2NDMxMDM1MDEsMTYxNjM2NzgwNCwxNjE2MzY3ODAzLDE2MTYzNjc4MDIsMTYxNjM2NzgwMV0sInNlcnZpY2VJZCI6ImM2ZDgyODViNzJhODRlZmI4ZmJkNjA4YzdjYWRhNDg0IiwiZmFjaWxpdGllcyI6W3siaWQiOiItMSIsIm5hbWUiOiJHbG9iYWwgUG9ydGFsIn0seyJpZCI6IjE2NDMxMDM1MDIiLCJuYW1lIjoiTG9uZG9uIn0seyJpZCI6IjE2NDMxMDM1MDEiLCJuYW1lIjoiTmV3IFlvcmsifSx7ImlkIjoiMTY0MzEwMzUwMyIsIm5hbWUiOiJTeWRuZXkifSx7ImlkIjoiMTYxNjM2NzgwMSIsIm5hbWUiOiJVbml0IDEifSx7ImlkIjoiMTYxNjM2NzgwMiIsIm5hbWUiOiJVbml0IDIifSx7ImlkIjoiMTYxNjM2NzgwMyIsIm5hbWUiOiJVbml0IDMifSx7ImlkIjoiMTYxNjM2NzgwNCIsIm5hbWUiOiJVbml0IDQifV0sImN1cnJlbnRGYWNpbGl0eUlkIjoxNjE2MzY3ODAyLCJyb2xlTmFtZXMiOlsiQUNDT1VOVF9PV05FUiJdLCJqdGkiOiI2MDk3Y2IyZTU3OGQ0MzIzYTUzYTA5YzE4OWQzZjYxNyIsInVzZXJuYW1lIjoiYm90MDEiLCJzdWIiOiJib3QwMSIsImlhdCI6MTcwMTQyMDA2MywiZXhwIjoxNzA5OTczNjYzfQ.LPT6Y1WFj_SDvi7aSIeP6ukRBgkuXI-ufW_TEztPdGCtBYPOLsb5cg9UtYC0aNLryIfzodW0joL-G-ExYJjxrw'
# uat_url = 'https://api.valent.demo.platform.leucinetech.com/v1/objects'
url = uat_url
token = uat_token
headers = {
    'Authorization': f'Bearer {token}'
}

key_mappings = {
    'product': {
        'collection': 'products',
        'object_type_id': '64ca640c7de0fe33130a99bc',
        'property': {
            'product_name': '64ca640c7de0fe33130a99bd',
            'product_code': '64ca640c7de0fe33130a99be'
        },
        'relation': {
        }
    },
    'production_order': {
        'collection': 'productionOrders',
        'object_type_id': '64ca635b7de0fe33130a99ac',
        'property': {
            'name': '64ca635b7de0fe33130a99ad',
            'production_order_number': '64ca635b7de0fe33130a99ae',
            'scheduled_start': '64ccc848a8a5a262c13c8085',
            'scheduled_end': '64ccc884a8a5a262c13c8086'
        },
        'relation': {
            'product': '64ccbe15a8a5a262c13c807b',
            'semi_finished_product': '65697c91d8b86e1a63fac2e8',
        }
    },
    'batch': {
        'collection': 'batches',
        'object_type_id': '64ccc000a8a5a262c13c807c',
        'property': {
            'product_name': '64ccc000a8a5a262c13c807d',
            'batch_number': '64ccc000a8a5a262c13c807e'
        },
        'relation': {
            'production_order': '64ccc013a8a5a262c13c8084',
            'product': '65697df2d8b86e1a63fac2ea',
            'semi_finished_product': '65697ddbd8b86e1a63fac2e9',
        }
    },
    'material': {
        'collection': 'materials',
        'object_type_id': '65548c1e41913e1f1c6ecc42',
        'property': {
            'material_name': '65548c1e41913e1f1c6ecc43',
            'material_code': '65548c1e41913e1f1c6ecc44'
        },
        'relation': {
        }
    },
    'material_lot': {
        'collection': 'materialLots',
        'object_type_id': '64ca61b67de0fe33130a9984',
        'property': {
            'material_lot_number': '64ca61b67de0fe33130a9986',
            'material_name': '64ca61b67de0fe33130a9985',
            'unrestricted_quantity': '65671d29d8b86e1a63fac280',
            'in_quality_inspection': '65671d5fd8b86e1a63fac281',
            'blocked': '65671d6cd8b86e1a63fac282',
            'stk_in_transit': '65671d7ed8b86e1a63fac283',
            'restricted_use': '65671d8cd8b86e1a63fac284',
            'uom': '65671dafd8b86e1a63fac285'
        },
        'relation': {
            'material': '65670792d8b86e1a63fac27f',
        }
    },
    'semi_finished_product': {
        'collection': 'semi-finishedProducts',
        'object_type_id': '6554965641913e1f1c6ecc57',
        'property': {
            'semi_finished_product_name': '6554965641913e1f1c6ecc58',
            'semi_finished_product_code': '6554965641913e1f1c6ecc59'
        },
        'relation': {
        }
    },
    'bom_material': {
        'collection': 'bomMaterial',
        'object_type_id': '655492ce41913e1f1c6ecc4a',
        'property': {
            'material_name': '655492ce41913e1f1c6ecc4b',
            'bom_code': '655492ce41913e1f1c6ecc4c',
            'target_quantity': '655492e041913e1f1c6ecc52',
            'uom': '6554930641913e1f1c6ecc54',
        },
        'relation': {
            'production_order': '6554932441913e1f1c6ecc55',
            'material': '6554933641913e1f1c6ecc56',
            'semi_finished_product': '6554967041913e1f1c6ecc5f',
        }
    }
}

sfp_codes = ['15538', '15539', '16024', '16874', '16876', '18571', '20010', '20011', '22915', '23472', '25461', '25932',
             '30445', '30446', '37388', '44493', '53422', '58392']


def _display(message: str, override=False):
    if DEBUG or override:
        print(message)


def _build_search_filter(key, value):
    return json.dumps({"op": "AND", "fields": [{"field": f"searchable.{key}", "op": "EQ", "values": [f"{value}"]}]})


def lambda_handler(event, context):
    sftp = setup_sftp_connection()

    prefix_production_order = "production_order"
    unsynced_po_files = get_unsynced_files(sftp, PO_DIR, prefix_production_order)
    process_po_files(sftp, unsynced_po_files)
    set_last_sync_time(datetime.now(), prefix_production_order)

    prefix_inventory = "inventory"
    unsynced_inventory_files = get_unsynced_files(sftp, INVENTORY_DIR, "inventory")
    process_inventory_files(sftp, unsynced_inventory_files)
    set_last_sync_time(datetime.now(), prefix_inventory)

    close_sftp_connection(sftp)


def setup_sftp_connection():
    # Establish an SSH connection
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(HOST, username=USERNAME, password=PASSWORD)
    sftp = ssh_client.open_sftp()
    return sftp


# Function to get the last sync time
def get_last_sync_time(prefix: str):
    try:
        with open(f"{prefix}_last_sync_time.txt", 'r') as f:
            return datetime.fromisoformat(f.read().strip())
    except FileNotFoundError:
        return datetime.min

# Function to set the last sync time
def set_last_sync_time(sync_time, prefix: str):
    with open(f"{prefix}_last_sync_time.txt", 'w') as f:
        f.write(sync_time.isoformat())


def get_unsynced_files(sftp, directory: str, prefix: str):
    files = sftp.listdir_attr(directory)
    last_sync_time = get_last_sync_time(prefix)
    unsynced_files = []

    for file in files:
        file_time = datetime.fromtimestamp(file.st_mtime)
        if file_time > last_sync_time:
            unsynced_files.append(file.filename)
    return unsynced_files


def _file_exist(file_name: str, prefix: str):
    try:
        # Check if the file exists in DynamoDB
        response = dynamodb.get_item(TableName='FileList', Key={'id': {'S': file_name}})
        return 'Item' in response
    except Exception as e:
        print(f"Error checking file existence: {str(e)}")
        return False

def process_inventory_files(sftp, filenames):
    _display(f"Starting to Process {len(filenames)} files: {filenames}", True)
    for filename in filenames:
        _display("")
        _display(f"------------------- Processing file: {filename} -------------------", True)
        valid_extensions = {".xls", ".xlsx"}
        is_valid_file_extension = os.path.splitext(filename)[1].lower() in valid_extensions
        if not is_valid_file_extension:
            _display(f"Skipping file {filename} due to invalid file extension.")
            continue
        file_path = f'{INVENTORY_DIR}/{filename}'
        with sftp.open(file_path) as f:
            temp = pd.read_excel(f)
        content = temp.to_csv(sep='|', index=False)
        lines = [line.split("|") for line in content.strip().split("\n")[1:]]
        data = _extract_inventory_data(lines)
        for material_code in data:
            material = data[material_code]
            material_code = material['material_code']
            material_name = material['material_name']
            material_obj = _process_material(material_code, material_name)
            _display(f"Material: {material_code}")
            lots = material['lots']
            for lot in lots:
                material_lot_obj = None
                material_lot_number = lot['material_lot_number']
                unrestricted_quantity = lot['unrestricted_quantity']
                in_quality_inspection = lot['in_quality_inspection']
                blocked = lot['blocked']
                stk_in_transit = lot['stk_in_transit']
                restricted_use = lot['restricted_use']
                uom = lot['uom']
                if material_lot_number:
                    material_lot_obj = _process_material_lot(material_lot_number, material_name, unrestricted_quantity, in_quality_inspection, blocked, stk_in_transit, restricted_use, uom, material_obj)
                else:
                    _display(f"ERROR: Cannot create Lot for line item, Material: {material}, Lot: {lot}", True)
                _display(material_lot_obj)


def process_po_files(sftp, filenames):
    _display(f"Starting to Process {len(filenames)} files: {filenames}", True)
    for filename in filenames:
        _display("")
        _display(f"------------------- Processing file: {filename} -------------------", True)
        is_valid_file_extension = os.path.splitext(filename)[1].lower() == '.csv'
        if not is_valid_file_extension:
            _display(f"Skipping file {filename} due to invalid file extension.")
            continue
        file_path = f'{PO_DIR}/{filename}'
        with sftp.open(file_path) as f:
            content = f.read().decode('utf-8')
        lines = [line.split("|") for line in content.strip().split("\n")]
        # Extract data
        data = _extract_po_data(lines)
        header = data['header']
        components = data['components']
        code = header['code']
        name = header['name']
        is_semi_finished_product = code in sfp_codes
        production_order_number = header['production_order_number']
        scheduled_start = header['scheduled_start']
        scheduled_end = header['scheduled_end']
        batch_number = header['batch_number']

        header_product_obj = None
        header_semi_finished_product_obj = None
        if is_semi_finished_product:
            header_semi_finished_product_obj = _process_semi_finished_product(code, name)
            _display(f"Semi-finished Product: {code}")
            _display(header_semi_finished_product_obj)
        else:
            header_product_obj = _process_product(code, name)
            _display(f"Product: {code}")
            _display(header_product_obj)

        production_order_obj = _process_production_order(production_order_number, name, scheduled_start, scheduled_end, header_product_obj, header_semi_finished_product_obj)
        _display(production_order_obj)

        batch_obj = _process_batch(batch_number, name, production_order_obj, header_product_obj, header_semi_finished_product_obj)
        _display(batch_obj)

        for component in components:
            _display(f"Component: {component['bom_code']}")
            bom_code = component['bom_code']
            code = component['code']
            name = component['name']
            target_quantity = component['target_quantity']
            uom = component['uom']
            material_obj = None
            semi_finished_product_obj = None
            is_semi_finished_product = code in sfp_codes
            if is_semi_finished_product:
                semi_finished_product_obj = _process_semi_finished_product(code, name)
                _display(semi_finished_product_obj)
            else:
                material_obj = _process_material(code, name)
                _display(material_obj)
            bom_material_obj = _process_bom_material(name, bom_code, target_quantity, uom, production_order_obj, material_obj, semi_finished_product_obj)
            _display(bom_material_obj)


def _get_epoch(date_str, format='%m/%d/%Y'):
    date_str = date_str.replace('\r', '')
    datetime_obj = datetime.strptime(date_str, format)
    epoch = int(datetime_obj.timestamp())
    return epoch


def close_sftp_connection(sftp):
    sftp.close()
    # sftp.get_transport().close()


def _extract_po_data(lines):
    # Split the content into lines and then by the pipe character
    # lines = [line.split("|") for line in content.strip().split("\n")]
    header = {}
    components = []

    for line in lines:
        if line[0] == "H":
            # Extracting the data based on the corrected mapping from the header
            # Assuming the first line is always the header
            header_data = line
            code = header_data[2] if len(header_data) > 1 else None
            name = header_data[3] if len(header_data) > 1 else None
            production_order_number = header_data[1] if len(header_data) > 1 else None
            scheduled_start = str(_get_epoch(header_data[9])) if len(header_data) > 1 else None
            scheduled_end = str(_get_epoch(header_data[10])) if len(header_data) > 1 else None
            batch_number = header_data[8] if len(header_data) > 1 else None
            header = {
                'name': name,
                'code': code,
                'production_order_number': production_order_number,
                'scheduled_start': scheduled_start,
                'scheduled_end': scheduled_end,
                'batch_number': batch_number
            }
        elif line[0] == "C":
            component_data = line
            code = component_data[3] if len(component_data) > 1 else None
            name = component_data[4] if len(component_data) > 1 else None
            target_quantity = component_data[5] if len(component_data) > 1 else None
            uom = component_data[6] if len(component_data) > 1 else None
            lot = component_data[7] if len(component_data) > 1 and component_data[7].strip() != "" else None
            bom_code = f"{component_data[1]}_{component_data[2]}" if lot is None else f"{component_data[1]}_{component_data[2]}_{lot}"

            component = {
                'bom_code': bom_code,
                'code': code,
                'name': name,
                'target_quantity': target_quantity,
                'uom': uom
            }
            components.append(component)
    return {
        'header': header,
        'components': components
    }


def _process_product(product_code, product_name):
    product_obj = _get_product(product_code)
    if product_obj is None:
        product_obj = _create_product(product_code, product_name)
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


def _create_product(product_code, product_name):
    product_obj = None
    product_url = f"{url}"
    data = {
        'objectTypeId': key_mappings['product']['object_type_id'],
        'properties': {
            key_mappings['product']['property']['product_code']: product_code,
            key_mappings['product']['property']['product_name']: product_name
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


def _process_production_order(production_order_number, name, scheduled_start, scheduled_end, product_obj, semi_finished_product_obj):
    production_order_obj = _get_production_order(production_order_number)
    if production_order_obj is None:
        production_order_obj = _create_production_order(production_order_number, name, scheduled_start, scheduled_end, product_obj, semi_finished_product_obj)
    return production_order_obj


def _get_production_order(production_order_number):
    production_order_obj = None
    production_order_collection = key_mappings['production_order']['collection']
    production_order_number_id = key_mappings['production_order']['property']['production_order_number']
    production_order_url = f"{url}/partial"
    params = {
        'collection': {production_order_collection},
        'filters': _build_search_filter(production_order_number_id, production_order_number)
    }
    response = requests.get(production_order_url, params=params, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if len(data):
            production_order_obj = data[0]
    else:
        print('Error:', response.status_code)
    return production_order_obj


def _create_production_order(production_order_number, name, scheduled_start, scheduled_end, product_obj, semi_finished_product_obj):
    production_order_obj = None
    production_order_url = f"{url}"
    data = {
        'objectTypeId': key_mappings['production_order']['object_type_id'],
        'properties': {
            key_mappings['production_order']['property']['production_order_number']: production_order_number,
            key_mappings['production_order']['property']['name']: name,
            key_mappings['production_order']['property']['scheduled_start']: scheduled_start,
            key_mappings['production_order']['property']['scheduled_end']: scheduled_end
        },
        'relations': {
            key_mappings['production_order']['relation']['product']: [product_obj] if product_obj is not None else None,
            key_mappings['production_order']['relation']['semi_finished_product']: [semi_finished_product_obj] if semi_finished_product_obj is not None else None
        }
    }
    response = requests.post(production_order_url, json=data, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if len(data):
            production_order_obj = {
                'id': data['id'],
                'collection': key_mappings['production_order']['collection'],
                'externalId': data['externalId'],
                'displayName': data['displayName']
            }
    else:
        print('Error:', response.status_code)
        print('Error:', response.text)
    return production_order_obj


def _process_batch(batch_number, product_name, production_order_obj, product_obj, semi_finished_product_obj):
    batch_obj = _get_batch(batch_number)
    if batch_obj is None:
        batch_obj = _create_batch(batch_number, product_name, production_order_obj, product_obj, semi_finished_product_obj)
    return batch_obj


def _get_batch(batch_number):
    batch_obj = None
    batch_collection = key_mappings['batch']['collection']
    batch_number_id = key_mappings['batch']['property']['batch_number']
    batch_url = f"{url}/partial"
    params = {
        'collection': {batch_collection},
        'filters': _build_search_filter(batch_number_id, batch_number)
    }
    response = requests.get(batch_url, params=params, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if len(data):
            batch_obj = data[0]
    else:
        print('Error:', response.status_code)
    return batch_obj


def _create_batch(batch_number, product_name, production_order_obj, product_obj, semi_finished_product_obj):
    batch_obj = None
    batch_url = f"{url}"
    data = {
        'objectTypeId': key_mappings['batch']['object_type_id'],
        'properties': {
            key_mappings['batch']['property']['batch_number']: batch_number,
            key_mappings['batch']['property']['product_name']: product_name,
        },
        'relations': {
            key_mappings['batch']['relation']['production_order']: [production_order_obj],
            key_mappings['batch']['relation']['product']: [product_obj] if product_obj is not None else None,
            key_mappings['batch']['relation']['semi_finished_product']: [semi_finished_product_obj] if semi_finished_product_obj is not None else None
        }
    }
    response = requests.post(batch_url, json=data, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if len(data):
            batch_obj = {
                'id': data['id'],
                'collection': key_mappings['batch']['collection'],
                'externalId': data['externalId'],
                'displayName': data['displayName']
            }
    else:
        print('Error:', response.status_code)
        print('Error:', response.text)
    return batch_obj


def _process_material(material_code, material_name):
    material_obj = _get_material(material_code)
    if material_obj is None:
        material_obj = _create_material(material_code, material_name)
    return material_obj


def _get_material(material_code):
    material_obj = None
    material_collection = key_mappings['material']['collection']
    material_code_id = key_mappings['material']['property']['material_code']
    params = {
        'collection': {material_collection},
        'filters': _build_search_filter(material_code_id, material_code)
    }
    material_url = f"{url}/partial"
    response = requests.get(material_url, params=params, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if len(data):
            material_obj = data[0]
    else:
        print('Error:', response.status_code)
        print('Error:', response.text)
    return material_obj


def _create_material(material_code, material_name):
    material_obj = None
    material_url = f"{url}"
    data = {
        'objectTypeId': key_mappings['material']['object_type_id'],
        'properties': {
            key_mappings['material']['property']['material_code']: material_code,
            key_mappings['material']['property']['material_name']: material_name
        }
    }
    response = requests.post(material_url, json=data, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if len(data):
            material_obj = {
                'id': data['id'],
                'collection': key_mappings['material']['collection'],
                'externalId': data['externalId'],
                'displayName': data['displayName']
            }
    else:
        print('Error:', response.status_code)
        print('Error:', response.text)
    return material_obj


def _process_material_lot(material_lot_number, material_name, unrestricted_quantity, in_quality_inspection, blocked, stk_in_transit, restricted_use, uom, material_obj):
    material_lot_obj = _get_material_lot(material_lot_number)
    if material_lot_obj is None:
        _display(f"Creating Lot [{material_lot_number}] for material: {material_obj['externalId']}", True)
        material_lot_obj = _create_material_lot(material_lot_number, material_name, unrestricted_quantity, in_quality_inspection, blocked, stk_in_transit, restricted_use, uom, material_obj)
    else:
        _display(f"Updating Lot [{material_lot_number}] for material: {material_obj['externalId']}", True)
        material_lot_obj = _update_material_lot(material_lot_obj, material_lot_number, material_name, unrestricted_quantity, in_quality_inspection, blocked, stk_in_transit, restricted_use, uom, material_obj)
    return material_lot_obj


def _get_material_lot(material_lot_number):
    material_lot_obj = None
    material_lot_collection = key_mappings['material_lot']['collection']
    material_lot_number_id = key_mappings['material_lot']['property']['material_lot_number']
    material_lot_url = f"{url}/partial"
    params = {
        'collection': {material_lot_collection},
        'filters': _build_search_filter(material_lot_number_id, material_lot_number)
    }
    response = requests.get(material_lot_url, params=params, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if len(data):
            material_lot_obj = data[0]
    else:
        print('Error:', response.status_code)
        print('Error:', response)
    return material_lot_obj

def _get_particular_material_lot(material_lot_id):
    material_lot_obj = None
    material_lot_url = f"{url}/{material_lot_id}?collection={key_mappings['material_lot']['collection']}"
    response = requests.get(material_lot_url, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if data:
            material_lot_obj = data
    else:
        print('Error:', response.status_code)
        print('Error:', response)
    return material_lot_obj


def _create_material_lot(material_lot_number, material_name, unrestricted_quantity, in_quality_inspection, blocked, stk_in_transit, restricted_use, uom, material_obj):
    material_lot_obj = None
    material_lot_url = f"{url}"
    data = {
        'objectTypeId': key_mappings['material_lot']['object_type_id'],
        'properties': {
            key_mappings['material_lot']['property']['material_lot_number']: material_lot_number,
            key_mappings['material_lot']['property']['material_name']: material_name,
            key_mappings['material_lot']['property']['unrestricted_quantity']: unrestricted_quantity,
            key_mappings['material_lot']['property']['in_quality_inspection']: in_quality_inspection,
            key_mappings['material_lot']['property']['blocked']: blocked,
            key_mappings['material_lot']['property']['stk_in_transit']: stk_in_transit,
            key_mappings['material_lot']['property']['restricted_use']: restricted_use,
            key_mappings['material_lot']['property']['uom']: uom,
        },
        'relations': {
            key_mappings['material_lot']['relation']['material']: [material_obj]
        }
    }
    response = requests.post(material_lot_url, json=data, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if len(data):
            material_lot_obj = {
                'id': data['id'],
                'collection': key_mappings['material_lot']['collection'],
                'externalId': data['externalId'],
                'displayName': data['displayName']
            }
    else:
        print('Error:', response.status_code)
        print('Error:', response)
    return material_lot_obj


def _update_material_lot(material_lot_obj, material_lot_number, material_name, unrestricted_quantity, in_quality_inspection, blocked, stk_in_transit, restricted_use, uom, material_obj):
    # material_lot_obj = None
    material_lot_obj = _get_particular_material_lot(material_lot_obj['id'])
    material_lot_url = f"{url}/{material_lot_obj['id']}"
    relations = material_lot_obj['relations']
    properties = material_lot_obj['properties']
    temp = next((property for property in properties if property["id"] == key_mappings['material_lot']['property']['material_lot_number']), None)
    material_lot_number = temp['value']
    temp = next((property for property in properties if property["id"] == key_mappings['material_lot']['property']['material_name']), None)
    material_name = temp['value']
    temp = next((relation for relation in relations if relation["id"] == key_mappings['material_lot']['relation']['material']), None)
    target = temp['targets'][0]
    material_obj = {
        "collection": target['collection'],
        "displayName": target['displayName'],
        "externalId": f"(ID: {target['externalId']})",
        "id": target['id'],
        "label": target['displayName'],
        "type": "OBJECTS",
        "value": target['id'],
    }
    data = {
        'objectTypeId': key_mappings['material_lot']['object_type_id'],
        'properties': {
            key_mappings['material_lot']['property']['material_lot_number']: material_lot_number,
            key_mappings['material_lot']['property']['material_name']: material_name,
            key_mappings['material_lot']['property']['unrestricted_quantity']: unrestricted_quantity,
            key_mappings['material_lot']['property']['in_quality_inspection']: in_quality_inspection,
            key_mappings['material_lot']['property']['blocked']: blocked,
            key_mappings['material_lot']['property']['stk_in_transit']: stk_in_transit,
            key_mappings['material_lot']['property']['restricted_use']: restricted_use,
            key_mappings['material_lot']['property']['uom']: uom,
        },
        'relations': {
            key_mappings['material_lot']['relation']['material']: [material_obj]
        }
    }
    response = requests.patch(material_lot_url, json=data, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if len(data):
            material_lot_obj = {
                'id': data['id'],
                'collection': key_mappings['material_lot']['collection'],
                'externalId': data['externalId'],
                'displayName': data['displayName']
            }
    else:
        print('Error:', response.status_code)
        print('Error:', response.content)
    return material_lot_obj


def _process_semi_finished_product(semi_finished_product_code, semi_finished_product_name):
    semi_finished_product_obj = _get_semi_finished_product(semi_finished_product_code)
    if semi_finished_product_obj is None:
        semi_finished_product_obj = _create_semi_finished_product(semi_finished_product_code, semi_finished_product_name)
    return semi_finished_product_obj


def _get_semi_finished_product(semi_finished_product_code):
    semi_finished_product_obj = None
    semi_finished_product_collection = key_mappings['semi_finished_product']['collection']
    semi_finished_product_code_id = key_mappings['semi_finished_product']['property']['semi_finished_product_code']
    params = {
        'collection': {semi_finished_product_collection},
        'filters': _build_search_filter(semi_finished_product_code_id, semi_finished_product_code)
    }
    semi_finished_product_url = f"{url}/partial"
    response = requests.get(semi_finished_product_url, params=params, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if len(data):
            semi_finished_product_obj = data[0]
    else:
        print('Error:', response.status_code)
        print('Error:', response.text)
    return semi_finished_product_obj


def _create_semi_finished_product(semi_finished_product_code, semi_finished_product_name):
    semi_finished_product_obj = None
    semi_finished_product_url = f"{url}"
    data = {
        'objectTypeId': key_mappings['semi_finished_product']['object_type_id'],
        'properties': {
            key_mappings['semi_finished_product']['property']['semi_finished_product_code']: semi_finished_product_code,
            key_mappings['semi_finished_product']['property']['semi_finished_product_name']: semi_finished_product_name
        }
    }
    response = requests.post(semi_finished_product_url, json=data, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if len(data):
            semi_finished_product_obj = {
                'id': data['id'],
                'collection': key_mappings['semi_finished_product']['collection'],
                'externalId': data['externalId'],
                'displayName': data['displayName']
            }
    else:
        print('Error:', response.status_code)
        print('Error:', response.text)
    return semi_finished_product_obj


def _process_bom_material(material_name, bom_code, target_quantity, uom, production_order_obj, material_obj,
                          semi_finished_product_obj):
    bom_material_obj = _get_bom_material(bom_code)
    if bom_material_obj is None:
        bom_material_obj = _create_bom_material(material_name, bom_code, target_quantity, uom, production_order_obj,
                                                material_obj, semi_finished_product_obj)
    return bom_material_obj


def _get_bom_material(bom_code):
    bom_material_obj = None
    bom_material_collection = key_mappings['bom_material']['collection']
    bom_code_id = key_mappings['bom_material']['property']['bom_code']
    bom_material_url = f"{url}/partial"
    params = {
        'collection': {bom_material_collection},
        'filters': _build_search_filter(bom_code_id, bom_code)
    }
    response = requests.get(bom_material_url, params=params, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if len(data):
            bom_material_obj = data[0]
    else:
        print('Error:', response.status_code)
    return bom_material_obj


def _create_bom_material(material_name, bom_code, target_quantity, uom, production_order_obj, material_obj,
                         semi_finished_product_obj):
    bom_material_obj = None
    bom_material_url = f"{url}"
    data = {
        'objectTypeId': key_mappings['bom_material']['object_type_id'],
        'properties': {
            key_mappings['bom_material']['property']['material_name']: material_name,
            key_mappings['bom_material']['property']['bom_code']: bom_code,
            key_mappings['bom_material']['property']['target_quantity']: target_quantity,
            key_mappings['bom_material']['property']['uom']: uom,
        },
        'relations': {
            key_mappings['bom_material']['relation']['production_order']: [production_order_obj],
            key_mappings['bom_material']['relation']['material']: [material_obj] if material_obj is not None else None,
            key_mappings['bom_material']['relation']['semi_finished_product']: [
                semi_finished_product_obj] if semi_finished_product_obj is not None else None
        }
    }
    response = requests.post(bom_material_url, json=data, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if len(data):
            bom_material_obj = {
                'id': data['id'],
                'collection': key_mappings['bom_material']['collection'],
                'externalId': data['externalId'],
                'displayName': data['displayName']
            }
    else:
        print('Error:', response.status_code)
        print('Error:', response.text)
    return bom_material_obj



# product_code = "P001"
# product_name = "P001"
# production_order_number = "PO001"
# scheduled_start = "1698949800"
# scheduled_end = "1699122600"
# batch_number = "B001"
# material_code = "M001"
# material_name = "M001"
# semi_finished_product_code = "SFP001"
# semi_finished_product_name = "SFP001"
# bom_code = "123"
# target_quantity = "123.60"
# uom = "KGS"
# #
# product_obj = _process_product(product_code, product_name)
# # print(product_obj)
# production_order_obj = _process_production_order(production_order_number, product_name, scheduled_start, scheduled_end, product_obj)
# # print(production_order_obj)
# # batch_obj = _process_batch(batch_number, product_name, production_order_obj)
# # print(batch_obj)
# material_obj = _process_material(material_code, material_name)
# # print(material_obj)
# # semi_finished_product_obj = _process_semi_finished_product(semi_finished_product_code, semi_finished_product_name)
# # print(semi_finished_product_obj)
# semi_finished_product_obj = None
# bom_material_obj = _process_bom_material(material_name, bom_code, target_quantity, uom, production_order_obj, material_obj,semi_finished_product_obj)
# print(bom_material_obj)
# extrated_content = _extract_data(content)
# print(extrated_content)


# content = """
# ;Plnt;SLoc;Material;Material description;BUn;Batch;   Unrestricted; In Qual. Insp.;        Blocked; Stk in Transit; Restricted-Use
# ;A511;3001;11525;ACID, BENZOIC, FMU, NON-CFR;KG;345799W900;875;10;0;0;0
# ;A511;3001;11525;ACID, BENZOIC, FMU, NON-CFR;KG;346041W900;532.5;0;0;0;0
# ;A511;3001;11525;ACID, BENZOIC, FMU, NON-CFR;KG;347428W900;1,000;0;0;0;0
# ;A511;3001;11910;HYDROCHLORIC ACID 25%;KG;351196W900;625.96;0;0;0;0
# ;A511;3001;11910;HYDROCHLORIC ACID 25%;KG;351197W900;834.619;0;0;0;0
# ;A511;3001;12196;Ultrasil 63;KG;324479W900;0;0;220;0;0
# ;A511;3001;12196;Ultrasil 63;KG;328462W900;114.125;0;0;0;0
# ;A511;3001;12198;ACID, NITRIC, Ultrasil 76;KG;333001W900;502.7;0;0;0;0
# ;A511;3001;12198;ACID, NITRIC, Ultrasil 76;KG;351199W900;903.35;0;0;0;0
# ;A511;3001;12198;ACID, NITRIC, Ultrasil 76;KG;351649W930;80;0;0;0;0
# ;A511;3001;12202;Ultrasil 110;KG;327037W900;0;0;243;0;0
# ;A511;3001;12202;Ultrasil 110;KG;328364W900;145.8;0;0;0;0
# ;A511;3001;12202;Ultrasil 110;KG;341073W900;107.48;0;0;0;0
# ;A511;3001;13130;METHANOL;KG;353695W930;9,687.49;0;0;0;0
# ;A511;3001;14378;AMMONIUM SULFATE;KG;351225W900;635.06;0;0;0;0
# ;A511;3001;14378;AMMONIUM SULFATE;KG;351226W900;907.2;0;0;0;0
# ;A511;3001;14378;AMMONIUM SULFATE;KG;354732W900;1,814.40;0;0;0;0
# ;A511;3001;14919;Ultrasil MP;KG;327048W900;176.56;0;0;0;0
# ;A511;3001;14919;Ultrasil MP;KG;327049W900;200;0;0;0;0
# ;A511;3001;14919;Ultrasil MP;KG;352358W900;221.8;0;0;0;0
# ;A511;1001;16874;AVG FERMENTATION;L;F168740219;70,774;0;0;0;0
# ;A511;1001;16874;AVG FERMENTATION;L;F168740220;1,13,875;0;0;0;0
# ;A511;1001;16874;AVG FERMENTATION;L;F168740221;99,048;0;0;0;0
# ;A511;3001;16875;2-AMINOETHANOL (NON;KG;346173W900;2,746.40;0;0;0;0
# ;A511;3001;20800;DEXTROSE MONOHYDRATE;KG;347422W900;226.82;0;0;0;0
# ;A511;3001;21892;Corn Syrup, D.E. 95 Blend;KG;354724W930;14,313.74;0;0;0;0
# ;A511;3001;21892;Corn Syrup, D.E. 95 Blend;KG;354727W930;32,494.77;0;0;0;0
# ;A511;3001;21892;Corn Syrup, D.E. 95 Blend;KG;354728W930;8,085.32;0;0;0;0
# ;A511;3001;21975;Corn Syrup;KG;354728W930;6,708.62;0;0;0;0
# ;A511;3001;21975;Corn Syrup;KG;354730W930;15,400.10;0;0;0;0
# ;A511;3001;21975;Corn Syrup;KG;354736W930;21,754.27;0;0;0;0
# ;A511;3001;21975;Corn Syrup;KG;354738W930;21,775.59;0;0;0;0
# ;A511;3001;21975;Corn Syrup;KG;354740W930;22,646.94;0;0;0;0
# ;A511;3001;21975;Corn Syrup;KG;354741W930;22,684.59;0;0;0;0
# ;A511;3001;21975;Corn Syrup;KG;354742W930;22,648.30;0;0;0;0
# ;A511;3001;21975;Corn Syrup;KG;354744W930;21,745.20;0;0;0;0
# ;A511;3001;21975;Corn Syrup;KG;355748W930;22,053.64;0;0;0;0
# ;A511;3001;21975;Corn Syrup;KG;355750W930;22,343.94;0;0;0;0
# ;A511;3001;22082;Enzyme, Liquid (FoodPro BSL);KG;339146W930;47.7;0;0;0;0
# ;A511;3001;23189;Sodium Hydroxide 30-32%;KG;354725W930;1,649.24;0;0;0;0
# ;A511;3001;23189;Sodium Hydroxide 30-32%;KG;354732W930;33,679.21;0;0;0;0
# ;A511;3001;23189;Sodium Hydroxide 30-32%;KG;354743W930;33,918.70;0;0;0;0
# ;A511;3001;23265;DEFOAMER, INDSUTROL;KG;319367W900;256.61;0;0;0;0
# ;A511;3001;27740;MAGNESIUM SULFATE CR;KG;352344W900;22.67;0;0;0;0
# ;A511;3001;27740;MAGNESIUM SULFATE CR;KG;352345W900;1,213.46;0;0;0;0
# ;A511;3001;27740;MAGNESIUM SULFATE CR;KG;353481W900;2,222.64;0;0;0;0
# ;A511;3001;29190;FILTER-AID (CELATOM FW-12, DIC ALI;KG;347292W900;351.54;0;0;0;0
# ;A511;3001;29708;FISH PROTEIN CONCENT;KG;347225W900;1,000;0;0;0;0
# ;A511;3001;31432;POTASSIUM SORBATE;KG;339544W900;318.42;0;0;0;0
# ;A511;3001;31432;POTASSIUM SORBATE;KG;344790W900;1,814.39;0;0;0;0
# ;A511;3001;38758;IRON SULFATE (FERROU;KG;273417W930;83.04;0;0;0;0
# ;A511;3001;39102;Enzyme, Liquid (FoodPro CBL);KG;351629W930;77;0;0;0;0
# ;A511;3001;39860;FISH PROTEIN CONCENT,SEAPRO;KG;348625W900;449.15;0;0;0;0
# ;A511;3001;39860;FISH PROTEIN CONCENT,SEAPRO;KG;349699W900;13,947.61;0;0;0;0
# ;A511;3001;39860;FISH PROTEIN CONCENT,SEAPRO;KG;349774W900;9,900.16;0;0;0;0
# ;A511;3001;44141;ACETIC ACID, 70%;KG;353717W930;32,097.45;0;0;0;0
# ;A511;3001;44380;MANGANESE SULFATE;KG;346165W900;5.5;0;0;0;0
# ;A511;3001;44380;MANGANESE SULFATE;KG;350958W900;229;0;0;0;0
# ;A511;3001;44380;MANGANESE SULFATE;KG;351227W900;675;0;0;0;0
# ;A511;3001;44380;MANGANESE SULFATE;KG;353660W900;675;0;0;0;0
# ;A511;3001;45675;METHYLPARABEN;KG;344789W900;422.5;0;0;0;0
# ;A511;3001;45675;METHYLPARABEN;KG;348498W900;677.5;0;0;0;0
# ;A511;3001;45675;METHYLPARABEN;KG;350905W900;525;0;0;0;0
# ;A511;3001;45679;TETRASODIUM EDTA (TRILON BX PO;KG;320584W900;75;0;0;0;0
# ;A511;1001;49466;AMINOETHOXYVINYLGLYCINE, AVG UNMIL;KG;A494660216;133.6;0;0;0;0
# ;A511;1001;49466;AMINOETHOXYVINYLGLYCINE, AVG UNMIL;KG;A494660217;60.5;0;0;0;0
# ;A511;1001;49466;AMINOETHOXYVINYLGLYCINE, AVG UNMIL;KG;A494660218;49.3;0;0;0;0
# ;A511;1001;49466;AMINOETHOXYVINYLGLYCINE, AVG UNMIL;KG;A494660219;0;69.3;0;0;0
# ;A511;1001;49466;AMINOETHOXYVINYLGLYCINE, AVG UNMIL;KG;B494660205;6.3;0;0;0;0
# ;A511;1001;49466;AMINOETHOXYVINYLGLYCINE, AVG UNMIL;KG;B494660216;163.2;0;0;0;0
# ;A511;1001;49466;AMINOETHOXYVINYLGLYCINE, AVG UNMIL;KG;B494660218;0;62.5;0;0;0
# ;A511;1001;49466;AMINOETHOXYVINYLGLYCINE, AVG UNMIL;KG;C494660215;125.6;0;0;0;0
# ;A511;1001;49466;AMINOETHOXYVINYLGLYCINE, AVG UNMIL;KG;C494660216;118;0;0;0;0
# ;A511;1001;49466;AMINOETHOXYVINYLGLYCINE, AVG UNMIL;KG;C494660217;0;0;21.9;0;0
# ;A511;1001;49466;AMINOETHOXYVINYLGLYCINE, AVG UNMIL;KG;C494660218;0;2.8;0;0;0
# ;A511;3001;52142;OIL, SOY BEAN, EDIBLE;KG;334370W900;907.19;0;0;0;0
# ;A511;3001;52142;OIL, SOY BEAN, EDIBLE;KG;335407W900;907.19;0;0;0;0
# ;A511;3001;57574;POLYPROPYLENE GLYCOL;KG;350885W900;492.3;0;0;0;0
# ;A511;3001;57616;POLYSORBATE 20, NOT NF;KG;347439W900;654;0;0;0;0
# ;A511;3001;57616;POLYSORBATE 20, NOT NF;KG;348654W900;1,090;0;0;0;0
# ;A511;3001;58599;POTASSIUM PHOSPHATE;KG;348549W900;102.06;0;0;0;0
# ;A511;3001;58599;POTASSIUM PHOSPHATE;KG;349820W900;111.34;0;0;0;0
# ;A511;3001;58599;POTASSIUM PHOSPHATE;KG;350953W900;3,268.31;0;0;0;0
# ;A511;3001;58661;POTASSIUM SULFATE;KG;351229W900;1,648.59;0;0;0;0
# ;A511;3001;58726;PRESERVATIVE (PROXEL GXL);KG;345014W900;511.07;0;0;0;0
# ;A511;3001;58726;PRESERVATIVE (PROXEL GXL);KG;345016W900;798.33;0;0;0;0
# ;A511;3001;59375;PROPYLPARABEN;KG;342265W900;75;0;0;0;0
# ;A511;3001;59375;PROPYLPARABEN;KG;344659W900;450;0;0;0;0
# ;A511;3001;59375;PROPYLPARABEN;KG;348462W900;750;0;0;0;0
# ;A511;3001;59613;AMMONIA WATER, 18%;KG;353705W930;383.969;0;0;0;0
# ;A511;3001;59613;AMMONIA WATER, 18%;KG;354735W930;22,235.08;0;0;0;0
# ;A511;3001;59613;AMMONIA WATER, 18%;KG;354737W930;4,998.64;0;0;0;0
# ;A511;3001;620106;Bottle, 2.5Gal/10L, F-Style;EA;348487H900;101;0;0;0;0
# ;A511;3001;62632;SOYBEAN FLOUR, SPECI;KG;354726W930;10,723.00;0;0;0;0
# ;A511;3001;62632;SOYBEAN FLOUR, SPECI;KG;354729W930;21,590.98;0;0;0;0
# ;A511;3001;62632;SOYBEAN FLOUR, SPECI;KG;354739W930;22,071.79;0;0;0;0
# ;A511;3001;62632;SOYBEAN FLOUR, SPECI;KG;355745W930;22,235.08;0;0;0;0
# ;A511;3001;633181;Pail Cover, TE, Natural;EA;338445H900;10;0;0;0;0
# ;A511;3001;633200;Cap, 63mm TE, PP/Foam liner, Print;EA;341115H900;0;0;537;0;0
# ;A511;3001;64051;Sulfuric Acid 66 Deg Bulk Tanker;KG;353710W930;1,958.78;0;0;0;0
# ;A511;3001;64051;Sulfuric Acid 66 Deg Bulk Tanker;KG;354733W930;14,480.12;0;0;0;0
# ;A511;3001;64339;SODIUM BICARBONATE;KG;341944W900;317.52;0;0;0;0
# ;A511;3001;64870;SODIUM CHLORIDE, GENERAL PURPOSE;KG;344607W900;4,445.28;0;0;0;0
# ;A511;3001;64870;SODIUM CHLORIDE, GENERAL PURPOSE;KG;345933W900;192.78;0;0;0;0
# ;A511;3001;65641;SODIUM HYPOCHLORITE;KG;348554W900;97.11;0;0;0;0
# ;A511;3001;65641;SODIUM HYPOCHLORITE;KG;350029W900;477.909;0;0;0;0
# ;A511;3001;66981;SODIUM SULFITE;KG;345013W900;619.16;0;0;0;0
# ;A511;3001;66981;SODIUM SULFITE;KG;345015W900;1,111.32;0;0;0;0
# ;A511;3001;68193;SOYBEAN PROTEIN;KG;338363W900;408.26;0;0;0;0
# ;A511;3001;69662;LACTOSE (SUGAR OF MILK), CRUDE;KG;333980W900;1,000;0;0;0;0
# ;A511;3001;69662;LACTOSE (SUGAR OF MILK), CRUDE;KG;335572W900;1,170;0;0;0;0
# ;A511;3001;70262;SURFACTANT, PLURONIC 10R5 (NON -CF;KG;337244W900;816;0;0;0;0
# ;A511;3001;70262;SURFACTANT, PLURONIC 10R5 (NON -CF;KG;340759W900;4,290.65;0;0;0;0
# ;A511;3001;70262;SURFACTANT, PLURONIC 10R5 (NON -CF;KG;343561W900;2,993.74;0;0;0;0
# ;A511;3001;70289;SURFACTANT, SURFYNOL - 485, NO N-C;KG;322079W900;611.737;0;0;0;0
# ;A511;3001;72719;Enzyme, Liquid (Viscozyme L);KG;345403W930;56.7;0;0;0;0
# ;A511;3001;72719;Enzyme, Liquid (Viscozyme L);KG;355749W930;250;0;0;0;0
# ;A511;3001;75148;WATER, TAP;KG;080688W90;6,41,484.76;0;0;0;0
# ;A511;3001;760100;DRUM, PLASTIC, TIGHTHEAD, 55 G;EA;351053H900;8;0;0;0;0
# ;A511;3001;760160;DRUM, FIBER, BLUE, 38-1/2 GAL;EA;352338H900;7;0;0;0;0
# ;A511;3001;760160;DRUM, FIBER, BLUE, 38-1/2 GAL;EA;354698H900;8;0;0;0;0
# ;A511;3001;760202;Pail, 5 gallon, Natural;EA;338446H900;44;0;0;0;0
# ;A511;3001;760202;Pail, 5 gallon, Natural;EA;339536H900;10;0;0;0;0
# ;A511;3001;76990;ULTRASIL 10;KG;349837W900;16;0;0;0;0
# ;A511;3001;76990;ULTRASIL 10;KG;351052W900;53.2;0;0;0;0
# ;A511;3001;76990;ULTRASIL 10;KG;352679W930;691.6;0;0;0;0
# ;A511;3001;78335;AMBEREX 1003 50 LB BAG;KG;341933W900;204.3;0;0;0;0
# ;A511;3001;78339;AMBEREX 695 AG SS;KG;300712W900;249.48;0;0;0;0
# ;A511;3001;78339;AMBEREX 695 AG SS;KG;351173W900;1,179.34;0;0;0;0
# ;A511;3001;907603;BAG, PLASTIC (30" X 54" x 4ML);EA;333049H900;60;0;0;0;0
# ;A511;3001;907603;BAG, PLASTIC (30" X 54" x 4ML);EA;352307H900;100;0;0;0;0
# ;A511;3001;907696;BAG, PLASTIC (38" X 60" X 4ML);EA;343508H900;24;0;0;0;0
# ;A511;3001;907696;BAG, PLASTIC (38" X 60" X 4ML);EA;353478H900;50;0;0;0;0
# ;A511;3001;907762;BAG, PLASTIC (20" X 37" X 4ML);EA;335724H900;450;0;0;0;0
# ;A511;3001;907762;BAG, PLASTIC (20" X 37" X 4ML);EA;337012H900;250;0;0;0;0
# ;A511;3001;985252;IBC 275-Gallon;EA;353498H900;5;0;0;0;0
# ;A511;3001;A511695;Citric Acid Solution 50%;KG;309188CF00;0;0;125;0;0
# ;A511;3001;A523260;ORGANIC DEFOAMER, XFO-378;KG;352441W900;61.3;0;0;0;0
# ;A511;3001;A523260;ORGANIC DEFOAMER, XFO-378;KG;353484W900;1,020.59;0;0;0;0
# ;A511;3001;A523260;ORGANIC DEFOAMER, XFO-378;KG;353485W900;1,062.79;0;0;0;0
# ;A511;3001;A523260;ORGANIC DEFOAMER, XFO-378;KG;354663W900;1,020.59;0;0;0;0
# ;A511;3001;A552143;NON-GMO OIL, SOYBEAN, EDIBLE;KG;350050W900;428.84;0;0;0;0
# ;A511;3001;A552143;NON-GMO OIL, SOYBEAN, EDIBLE;KG;350957W900;952;0;0;0;0
# ;A511;3001;A552143;NON-GMO OIL, SOYBEAN, EDIBLE;KG;351096W900;952.554;0;0;0;0
# ;A511;3001;A559399;PROPYLENE GLYCOL, TOTES;KG;348507W900;4,144;0;0;0;0
# ;A511;3001;A559399;PROPYLENE GLYCOL, TOTES;KG;351126W900;3,112.20;0;0;0;0
# ;A511;3001;A562002;TASTONE 900 AG SS;KG;350002W900;18.156;0;0;0;0
# ;A511;3001;A562002;TASTONE 900 AG SS;KG;352235W900;272.14;0;0;0;0
# ;A511;3001;A562004;AMBERFERM 7020 AG SS;KG;294476W900;235.82;0;0;0;0
# ;A511;3001;A562004;AMBERFERM 7020 AG SS;KG;328476W900;181.43;0;0;0;0
# ;A511;3001;A562006;AMBERFERM 4005 SS;KG;351222W900;90.72;0;0;0;0
# ;A511;3001;A562008;AMBERFERM 4500;KG;353654W900;22.72;0;0;0;0
# ;A511;3001;A562008;AMBERFERM 4500;KG;354773W900;294.838;0;0;0;0
# ;A511;3001;A562010;NUTREX 55 50 LB BAG;KG;341098W900;1,111.38;0;0;0;0
# ;A511;3001;A562011;SP3;KG;354731W930;15,366.39;0;0;0;0
# ;A511;3001;A562012;Exelerate 310;KG;345966W900;251.35;0;0;0;0
# ;A511;3001;A562012;Exelerate 310;KG;351152W900;25;0;0;0;0
# ;A511;3001;A562012;Exelerate 310;KG;355746W930;1,006.72;0;0;0;0
# ;A511;3001;A562012;Exelerate 310;KG;355747W930;1,006.72;0;0;0;0
# ;A511;3001;A562016;Synergex (US);KG;336945W900;156.73;0;0;0;0
# ;A511;3001;A562016;Synergex (US);KG;350049W900;17.1;0;0;0;0
# ;A511;3001;A562016;Synergex (US);KG;351200W900;694.56;0;0;0;0
# ;A511;3001;A562016;Synergex (US);KG;352437W900;308.172;0;0;0;0
# ;A511;3001;A562016;Synergex (US);KG;353643W900;1,837.12;0;0;0;0
# ;A511;3001;A562037;AMBERFERM RAPD-B 1470LB SS;KG;353540W900;666.788;0;0;0;0
# ;A511;3001;A585004;Stabicip;KG;353697W930;1,015.22;0;0;0;0
# ;A511;3001;A585006;ANHYDROUS SODIUM SULFATE (BAGS);KG;349747W900;1.8;0;0;0;0
# ;A511;3001;A585006;ANHYDROUS SODIUM SULFATE (BAGS);KG;351076W900;3,379.30;0;0;0;0
# ;A511;3001;A585006;ANHYDROUS SODIUM SULFATE (BAGS);KG;351092W900;1,134;0;0;0;0
# ;A511;3001;A585008;Enforce LP;KG;328464W900;0;0;241.3;0;0
# ;A511;3001;A585008;Enforce LP;KG;341072W900;723.9;0;0;0;0
# ;A511;X001;A585014;AMALGAM-60 Magnesium Hydroxide Slu;KG;;20,220.93;0;0;0;0
# ;A511;X001;A585016;Nalclear # 7768;KG;;4,212.51;0;0;0;0
# ;A511;X001;A585018;Ultrion # 8187;KG;;18,489.06;0;0;0;0
# ;A511;X001;A585020;Nalco 1404;KG;;2,095.59;0;0;0;0
# ;A511;X001;A585024;Electricity;KWH;;-1,14,361.36;0;0;0;0
# ;A511;X001;A585026;Steam;LB;;-5,71,501.12;0;0;0;0
# ;A511;X001;A585028;Waste Water;GAL;;-2,22,556.88;0;0;0;0
# ;A511;3001;A585030;PROMEX 20S;KG;297206W900;234.963;0;0;0;0
# ;A511;3001;A585030;PROMEX 20S;KG;301321W900;99.996;0;0;0;0
# """

content = """
H|100053300|44493|BTI SOY FERMENTATION|ZP01|A511|90000.000|LTR|F444930175|07/23/2023|07/24/2023
O|100053300|0020|07/23/2023|00:00:00|A5TS01|A511|Testing
O|100053300|0040|07/23/2023|00:00:00|A5BT01|A511|Storage/Media Mix Tank/
O|100053300|0050|07/23/2023|00:00:00|A5BT02|A511|Fermenters/ Harvest Tanks
O|100053300|0060|07/23/2023|00:00:00|A5CG01|A511|Chilled Glycol
O|100053300|0070|07/23/2023|00:00:00|A5PA01|A511|Process Air
C|100053300|0010|21892|Corn Syrup, D.E. 95 Blend|0.000|KGM||0020
C|100053300|0010|21892|Corn Syrup, D.E. 95 Blend|5940.000|KGM|282987W930|0020
C|100053300|0030|27740|MAGNESIUM SULFATE CR|0.000|KGM||0020
C|100053300|0030|27740|MAGNESIUM SULFATE CR|35.010|KGM|352345W900|0020
C|100053300|0040|44380|MANGANESE SULFATE|0.000|KGM||0020
C|100053300|0040|44380|MANGANESE SULFATE|5.500|KGM|346165W900|0020
C|100053300|0040|44380|MANGANESE SULFATE|0.530|KGM|350958W900|0020
C|100053300|0050|52142|OIL, SOY BEAN, EDIBLE|0.000|KGM||0020
C|100053300|0050|52142|OIL, SOY BEAN, EDIBLE|135.990|KGM|334370W900|0020
C|100053300|0060|58599|POTASSIUM PHOSPHATE|0.000|KGM||0020
C|100053300|0060|58599|POTASSIUM PHOSPHATE|77.380|KGM|348549W900|0020
C|100053300|0060|58599|POTASSIUM PHOSPHATE|22.610|KGM|350953W900|0020
C|100053300|0080|62632|SOYBEAN FLOUR, SPECI|0.000|KGM||0020
C|100053300|0080|62632|SOYBEAN FLOUR, SPECI|5940.000|KGM|355764W930|0020
C|100053300|0090|64051|Sulfuric Acid 66 Deg Bulk Tanker|0.000|KGM||0020
C|100053300|0090|64051|Sulfuric Acid 66 Deg Bulk Tanker|138.960|KGM|354733W930|0020
C|100053300|0100|23189|Sodium Hydroxide 30-32%|0.000|KGM||0020
C|100053300|0100|23189|Sodium Hydroxide 30-32%|1565.010|KGM|354743W930|0020
C|100053300|0120|A585024|Electricity|19928.700|KWH||0020
C|100053300|0130|A585026|Steam|87767.100|LBR||0020
C|100053300|0140|A585028|Waste Water|6827.400|GLL||0020
C|100053300|0150|23189|Sodium Hydroxide 30-32%|0.000|KGM||0020
C|100053300|0150|23189|Sodium Hydroxide 30-32%|888.840|KGM|354743W930|0020
C|100053300|0200|23265|DEFOAMER, INDSUTROL|0.000|KGM||0020
C|100053300|0200|23265|DEFOAMER, INDSUTROL|45.900|KGM|319367W900|0020
C|100053300|0220|A562011|SP3|146.700|KGM||0020
C|100053300|0240|39860|FISH PROTEIN CONCENT,SEAPRO|0.000|KGM||0020
C|100053300|0240|39860|FISH PROTEIN CONCENT,SEAPRO|1019.970|KGM|349838W900|0020
C|100053300|0250|A585004|Stabicip|0.000|KGM||0020
C|100053300|0250|A585004|Stabicip|42.300|KGM|353697W930|0020
C|100053300|0280|59613|AMMONIA WATER, 18%|0.000|KGM||0020
C|100053300|0280|59613|AMMONIA WATER, 18%|249.930|KGM|354737W930|0020
C|100053300|0290|A562016|Synergex (US)|0.000|KGM||0020
C|100053300|0290|A562016|Synergex (US)|4.950|KGM|336945W900|0020
C|100053300|0300|A552143|NON-GMO OIL, SOYBEAN, EDIBLE|135.990|KGM||0020
"""
def _extract_inventory_data(lines):
    # Split the content into lines and then by the pipe character
    materials = {}
    for line in lines:
        material_code = line[2]
        if material_code:
            material_name = line[3]
            lot = {
                'material_lot_number': line[5],
                'material_name': material_name,
                'unrestricted_quantity': line[6],
                'in_quality_inspection': line[8],
                'blocked': line[10],
                'stk_in_transit': line[12],
                'restricted_use': line[14],
                'uom': line[4],
            }

            if material_code not in materials:
                materials[material_code] = {'material_code': material_code,'material_name': material_name, 'lots': []}

            # Append the lot details to the material's 'lots' list
            materials[material_code]['lots'].append(lot)

    return materials

# process_inventory_files(None,None)
# process_po_files(None, None)
lambda_handler(None, None)
