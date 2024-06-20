import json
import logging
import os
from datetime import datetime
import time

import pandas as pd
import paramiko
import requests

DEBUG = True
# SFTP connection details
HOST = 'sftpval.valent.com'
USERNAME = 'SAPEBR'
PASSWORD = 'zuKABN34Bhjk'

PO_DIR = 'Outbound'
INVENTORY_DIR = 'Inventory'
PROCESSED_FILES_DIR = 'Processed_Files'
PROCESSED_PO_DIR = os.path.join(PROCESSED_FILES_DIR, PO_DIR)
PROCESSED_INVENTORY_DIR = os.path.join(PROCESSED_FILES_DIR, INVENTORY_DIR)

url = 'http://localhost:8081/v1/objects'
token = 'eyJhbGciOiJIUzUxMiJ9.eyJyb2xlcyI6W10sImVtcGxveWVlSWQiOiJCT1QwMSIsImhhc1NldENoYWxsZW5nZVF1ZXN0aW9uIjp0cnVlLCJmaXJzdE5hbWUiOiJMZXVjaW5lIEJvdCIsImlkIjoyLCJmYWNpbGl0eUlkcyI6W10sInNlcnZpY2VJZCI6IjE3MDM3NjI4NzEiLCJmYWNpbGl0aWVzIjpbXSwiY3VycmVudEZhY2lsaXR5SWQiOjE2OTU5ODQwMjMsInJvbGVOYW1lcyI6W10sImVtYWlsIjoiYm90QGxldWNpbmV0ZWNoLmNvbSIsImp0aSI6ImFkMjk4ZTdmMDZjYjRmM2RhMzZkMWYwNjdlMmI3N2M4IiwidXNlcm5hbWUiOiJib3QiLCJzdWIiOiJib3QiLCJpYXQiOjE3MDM3NjgwNzAsImV4cCI6MTczNTMwNDA3MH0.k4Tl3nD9nA-EmMyH7Vq9Q8XLaBUF41vySdjxA305IRmK5E0gWamJsSfQ8_BXgfBPhgdJcKhvAiGJe0iIwrYMNA'
headers = {
    'Authorization': f'Bearer {token}'
}

# Get the current date
now = datetime.now()
year = now.strftime("%Y")
month = now.strftime("%m")

base_dir = "/home/ubuntu/valent/etl"

# Create directory path for year and month
log_dir = f"{base_dir}/logs/{year}/{month}"

# Create directories if they don't exist
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Set log filename with daily date
log_filename = now.strftime(f"{log_dir}/sap-sftp-etl-logs-%Y-%m-%d.log")

# Set up logging
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s %(message)s')

# Key mappings for different object types and their properties
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
            'batch_number': '64ccc000a8a5a262c13c807e',
            'batch_status': '65ca096611631e060be2d4b0',
            'target_quantity': '65ca0c5711631e060be2d4b3',
            'batch_yield': '65ca094b11631e060be2d4af',
            'uom': '65ca15c211631e060be2d4b4',
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

# SFP Codes
sfp_codes = ['15538', '15539', '16024', '16874', '16876', '18571', '20010', '20011', '22915', '23472', '25461', '25932',
             '30445', '30446', '37388', '44493', '53422', '58392']


# Function to display messages
def _display(message: str, override=False):
    if message is not None and message != "":
        logging.info(message)
    if DEBUG or override:
        print(message)


# Function to build search filter
def _build_search_filter(key, value):
    return json.dumps({"op": "AND", "fields": [{"field": f"searchable.{key}", "op": "EQ", "values": [f"{value}"]}]})


# Main function
def lambda_handler(event, context):
    sftp = setup_sftp_connection()

    # Process Production Order Files
    po_files = get_all_files(sftp, PO_DIR)
    process_po_files(sftp, po_files)

    # Process Inventory Files
    inventory_files = get_all_files(sftp, INVENTORY_DIR)
    process_inventory_files(sftp, inventory_files)

    close_sftp_connection(sftp)


# Function to setup SFTP connection
def setup_sftp_connection():
    # Establish an SSH connection
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(HOST, username=USERNAME, password=PASSWORD)
    sftp = ssh_client.open_sftp()
    return sftp


# Function to close SFTP connection
def close_sftp_connection(sftp):
    sftp.close()
    # sftp.get_transport().close()


# Function to get all files in the directory
def get_all_files(sftp, directory: str):
    files = sftp.listdir_attr(directory)
    all_files_list = []
    for file in files:
        all_files_list.append(file.filename)
    return all_files_list


# Function to move Processed files to Processed PO's folder with up to 3 attempts and a 5-second pause between them
def _move_processed_files(sftp, source_dir, filename, target_dir):
    attempts = 0
    max_attempts = 3

    while attempts < max_attempts:
        try:
            source_path = os.path.join(source_dir, filename)
            target_path = os.path.join(target_dir, filename)
            sftp.rename(source_path, target_path)
            _display(f"Successfully moved {filename} to {target_dir}")
            break  # Exit the loop if successful
        except Exception as e:
            if 'RNTO-error' in str(e):
                new_filename = f"rename_{time.time()}_{filename}"
                sftp.rename(target_path, os.path.join(PROCESSED_FILES_DIR, source_dir, new_filename))
                _display(
                    f"Successfully rename {filename} to {new_filename} in {os.path.join(PROCESSED_FILES_DIR, source_dir, new_filename)}")
            attempts += 1
            _display(f"Attempt {attempts} failed to move {filename} to {target_dir}: {str(e)}")
            if attempts < max_attempts:
                time.sleep(5)  # Pause for 5 seconds before the next attempt

    if attempts == max_attempts:
        _display(f"Failed to move {filename} to {target_dir} after {max_attempts} attempts", True)


# Function to process inventory files
def process_inventory_files(sftp, filenames):
    _display(f"Starting to Process {len(filenames)} files: {filenames}", True)
    for filename in filenames:
        try:
            existing_material_lots = _get_all_existing_material_lots()
            to_be_processed_material_lots = dict()
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
                        material_lot_obj = _process_material_lot(material_lot_number, material_name,
                                                                 unrestricted_quantity,
                                                                 in_quality_inspection, blocked, stk_in_transit,
                                                                 restricted_use, uom, material_obj)
                        to_be_processed_material_lots[material_lot_number] = material_lot_obj
                    else:
                        _display(f"ERROR: Cannot create Lot for line item, Material: {material}, Lot: {lot}", True)
                    _display(material_lot_obj)
            to_be_archived_material_lot_ids = set(existing_material_lots.keys()) - set(
                to_be_processed_material_lots.keys())
            for key in to_be_archived_material_lot_ids:
                material_lot_id = existing_material_lots[key]
                _display(f"Archiving Lot [{material_lot_number}] for material: {material_obj['externalId']}", True)
                _archive_material_lot(material_lot_id)
            _move_processed_files(sftp, INVENTORY_DIR, filename, PROCESSED_INVENTORY_DIR)
        except Exception as e:
            _display(f"""Error processing file {filename}: {str(e)}""", True)


# Function to process PO files
def process_po_files(sftp, filenames):
    _display(f"Starting to Process {len(filenames)} files: {filenames}", True)
    for filename in filenames:
        try:
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
            batch_target_quantity = header['batch_target_quantity']
            batch_uom = header['uom']

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

            production_order_obj = _process_production_order(production_order_number, name, scheduled_start,
                                                             scheduled_end,
                                                             header_product_obj, header_semi_finished_product_obj)
            _display(f"Production Order: {production_order_number}")
            _display(production_order_obj)

            batch_obj = _process_batch(batch_number, name, batch_target_quantity, batch_uom, production_order_obj,
                                       header_product_obj, header_semi_finished_product_obj)
            _display(f"Batch: {batch_number}")
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
                bom_material_obj = _process_bom_material(name, bom_code, target_quantity, uom, production_order_obj,
                                                         material_obj, semi_finished_product_obj)
                _display(bom_material_obj)
            _move_processed_files(sftp, PO_DIR, filename, PROCESSED_PO_DIR)
        except Exception as e:
            _display(f"""Error processing file {filename}: {str(e)}""", True)


# Function to get epoch
def _get_epoch(date_str, format='%m/%d/%Y'):
    date_str = date_str.replace('\r', '')
    datetime_obj = datetime.strptime(date_str, format)
    epoch = int(datetime_obj.timestamp())
    return epoch


# Function to extract PO data
def _extract_po_data(lines):
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
            scheduled_start = str(_get_epoch(header_data[9].replace(",", "").replace("\r", "").strip())) if len(
                header_data) > 1 else None
            scheduled_end = str(_get_epoch(header_data[10].replace(",", "").replace("\r", "").strip())) if len(
                header_data) > 1 else None
            batch_number = header_data[8] if len(header_data) > 1 else None
            batch_target_quantity = header_data[6] if len(header_data) > 1 else None
            uom = header_data[7] if len(header_data) > 1 else None
            header = {
                'name': name,
                'code': code,
                'production_order_number': production_order_number,
                'scheduled_start': scheduled_start,
                'scheduled_end': scheduled_end,
                'batch_number': batch_number,
                'batch_target_quantity': batch_target_quantity,
                'uom': uom
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


# Function to process product
def _process_product(product_code, product_name):
    product_obj = _get_product(product_code)
    if product_obj is None:
        product_obj = _create_product(product_code, product_name)
    return product_obj


# Function to get product
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
        _display(f"Error processing url: {product_url}, response.status_code: {response.status_code}, response.text: {response.text}")
    return product_obj


# Function to create product
def _create_product(product_code, product_name):
    product_obj = None
    product_url = f"{url}"
    data = {
        'objectTypeId': key_mappings['product']['object_type_id'],
        'properties': {
            key_mappings['product']['property']['product_code']: product_code,
            key_mappings['product']['property']['product_name']: product_name
        },
        'reason': 'Leucine Bot created this object based on the most recent file received from SAP.'
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
        _display(f"Error processing url: {product_url}, data: {data}, response.status_code: {response.status_code}, response.text: {response.text}")
    return product_obj


# Function to process production order
def _process_production_order(production_order_number, name, scheduled_start, scheduled_end, product_obj,
                              semi_finished_product_obj):
    production_order_obj = _get_production_order(production_order_number)
    if production_order_obj is None:
        production_order_obj = _create_production_order(production_order_number, name, scheduled_start, scheduled_end,
                                                        product_obj, semi_finished_product_obj)
    return production_order_obj


# Function to get production order
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
        _display(f"Error processing url: {production_order_url}, response.status_code: {response.status_code}, response.text: {response.text}")
    return production_order_obj


# Function to create production order
def _create_production_order(production_order_number, name, scheduled_start, scheduled_end, product_obj,
                             semi_finished_product_obj):
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
            key_mappings['production_order']['relation']['semi_finished_product']: [
                semi_finished_product_obj] if semi_finished_product_obj is not None else None
        },
        'reason': 'Leucine Bot created this object based on the most recent file received from SAP.'
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
        _display(f"Error processing url: {production_order_url}, data: {data}, response.status_code: {response.status_code}, response.text: {response.text}")
    return production_order_obj


# Function to process batch
def _process_batch(batch_number, product_name, batch_target_quantity, batch_uom, production_order_obj, product_obj,
                   semi_finished_product_obj):
    batch_obj = _get_batch(batch_number)
    if batch_obj is None:
        batch_obj = _create_batch(batch_number, product_name, batch_target_quantity, batch_uom, production_order_obj,
                                  product_obj, semi_finished_product_obj)
    return batch_obj


# Function to get batch
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
        _display(f"Error processing url: {batch_url}, response.status_code: {response.status_code}, response.text: {response.text}")
    return batch_obj


# Function to create batch
def _create_batch(batch_number, product_name, batch_target_quantity, batch_uom, production_order_obj, product_obj,
                  semi_finished_product_obj):
    batch_obj = None
    batch_url = f"{url}"
    data = {
        'objectTypeId': key_mappings['batch']['object_type_id'],
        'properties': {
            key_mappings['batch']['property']['batch_number']: batch_number,
            key_mappings['batch']['property']['product_name']: product_name,
            key_mappings['batch']['property']['batch_status']: ['65ca096611631e060be2d4b2'],
            key_mappings['batch']['property']['batch_yield']: '0',
            key_mappings['batch']['property']['target_quantity']: batch_target_quantity,
            key_mappings['batch']['property']['uom']: batch_uom,
        },
        'relations': {
            key_mappings['batch']['relation']['production_order']: [production_order_obj],
            key_mappings['batch']['relation']['product']: [product_obj] if product_obj is not None else None,
            key_mappings['batch']['relation']['semi_finished_product']: [
                semi_finished_product_obj] if semi_finished_product_obj is not None else None
        },
        'reason': 'Leucine Bot created this object based on the most recent file received from SAP.'
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
        _display(f"Error processing url: {batch_url}, data: {data}, data: {data}, response.status_code: {response.status_code}, response.text: {response.text}")
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
        _display(f"Error processing url: {material_url}, response.status_code: {response.status_code}, response.text: {response.text}")
    return material_obj


def _create_material(material_code, material_name):
    material_obj = None
    material_url = f"{url}"
    data = {
        'objectTypeId': key_mappings['material']['object_type_id'],
        'properties': {
            key_mappings['material']['property']['material_code']: material_code,
            key_mappings['material']['property']['material_name']: material_name
        },
        'reason': 'Leucine Bot created this object based on the most recent file received from SAP.'
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
        _display(f"Error processing url: {material_url}, data: {data}, response.status_code: {response.status_code}, response.text: {response.text}")
    return material_obj


def _process_material_lot(material_lot_number, material_name, unrestricted_quantity, in_quality_inspection, blocked,
                          stk_in_transit, restricted_use, uom, material_obj):
    material_lot_obj = _get_material_lot(material_lot_number)
    if material_lot_obj is None:
        _display(f"Creating Lot [{material_lot_number}] for material: {material_obj['externalId']}", True)
        material_lot_obj = _create_material_lot(material_lot_number, material_name, unrestricted_quantity,
                                                in_quality_inspection, blocked, stk_in_transit, restricted_use, uom,
                                                material_obj)
    else:
        _display(f"Updating Lot [{material_lot_number}] for material: {material_obj['externalId']}", True)
        material_lot_obj = _update_material_lot(material_lot_obj, material_lot_number, material_name,
                                                unrestricted_quantity, in_quality_inspection, blocked, stk_in_transit,
                                                restricted_use, uom, material_obj)
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
        _display(f"Error processing url: {material_lot_url}, response.status_code: {response.status_code}, response.text: {response.text}")
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
        _display(f"Error processing url: {material_lot_url}, response.status_code: {response.status_code}, response.text: {response.text}")
    return material_lot_obj


def _create_material_lot(material_lot_number, material_name, unrestricted_quantity, in_quality_inspection, blocked,
                         stk_in_transit, restricted_use, uom, material_obj):
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
        },
        'reason': 'Leucine Bot created this object based on the most recent file received from SAP.'
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
        _display(f"Error processing url: {material_lot_url}, data: {data}, response.status_code: {response.status_code}, response.text: {response.text}")
    return material_lot_obj


def _update_material_lot(material_lot_obj, material_lot_number, material_name, unrestricted_quantity,
                         in_quality_inspection, blocked, stk_in_transit, restricted_use, uom, material_obj):
    material_lot_obj = _get_particular_material_lot(material_lot_obj['id'])
    material_lot_url = f"{url}/{material_lot_obj['id']}"
    relations = material_lot_obj['relations']
    properties = material_lot_obj['properties']
    temp = next((property for property in properties if
                 property["id"] == key_mappings['material_lot']['property']['material_lot_number']), None)
    material_lot_number = temp['value']
    temp = next((property for property in properties if
                 property["id"] == key_mappings['material_lot']['property']['material_name']), None)
    material_name = temp['value']
    temp = next(
        (relation for relation in relations if relation["id"] == key_mappings['material_lot']['relation']['material']),
        None)
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
        },
        'reason': 'Leucine Bot updated this object based on the most recent file received from SAP.'
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
        _display(f"Error processing url: {material_lot_url}, data: {data}, response.status_code: {response.status_code}, response.text: {response.text}")
    return material_lot_obj


def _archive_material_lot(material_lot_id):
    material_lot_obj = None
    material_lot_url = f"{url}/{material_lot_id}/archive"
    data = {
        'collectionName': key_mappings['material_lot']['collection'],
        'reason': 'Leucine Bot archived this object based on the most recent inventory file received from SAP.'
    }
    response = requests.patch(material_lot_url, json=data, headers=headers)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        if data:
            material_lot_obj = data
    else:
        _display(f"Error processing url: {material_lot_url}, data: {data}, response.status_code: {response.status_code}, response.text: {response.text}")
    return material_lot_obj


def _process_semi_finished_product(semi_finished_product_code, semi_finished_product_name):
    semi_finished_product_obj = _get_semi_finished_product(semi_finished_product_code)
    if semi_finished_product_obj is None:
        semi_finished_product_obj = _create_semi_finished_product(semi_finished_product_code,
                                                                  semi_finished_product_name)
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
        _display(f"Error processing url: {semi_finished_product_url}, response.status_code: {response.status_code}, response.text: {response.text}")
    return semi_finished_product_obj


def _create_semi_finished_product(semi_finished_product_code, semi_finished_product_name):
    semi_finished_product_obj = None
    semi_finished_product_url = f"{url}"
    data = {
        'objectTypeId': key_mappings['semi_finished_product']['object_type_id'],
        'properties': {
            key_mappings['semi_finished_product']['property']['semi_finished_product_code']: semi_finished_product_code,
            key_mappings['semi_finished_product']['property']['semi_finished_product_name']: semi_finished_product_name
        },
        'reason': 'Leucine Bot created this object based on the most recent file received from SAP.'
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
        _display(f"Error processing url: {semi_finished_product_url}, data: {data}, response.status_code: {response.status_code}, response.text: {response.text}")
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
        _display(f"Error processing url: {bom_material_url}, response.status_code: {response.status_code}, response.text: {response.text}")
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
        },
        'reason': 'Leucine Bot created this object based on the most recent file received from SAP.'
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
        _display(f"Error processing url: {bom_material_url}, data: {data}, response.status_code: {response.status_code}, response.text: {response.text}")
    return bom_material_obj


def _get_all_existing_material_lots():
    material_lots_mapping = dict()
    page = 0
    total_pages = 1
    material_lot_collection = key_mappings['material_lot']['collection']
    material_lot_url = f"{url}/partial"
    params = {
        'collection': {material_lot_collection},
    }
    while page < total_pages:
        # Update the page parameter for each request
        params['page'] = page
        response = requests.get(material_lot_url, params=params, headers=headers)

        if response.status_code == 200:
            data = response.json()

            # # Extract the external IDs from the current page and add to the set
            for item in data.get('data', []):
                material_lots_mapping[item.get('externalId')] = item.get('id')
                # material_lots_mapping.add(item)
            #     material_lot_ids.add(item.get('externalId'))

            # Update the total_pages and page counter
            total_pages = data.get('pageable', {}).get('totalPages', total_pages)
            page += 1
        else:
            # Handle errors (e.g., by breaking the loop or raising an exception)
            break
    return material_lots_mapping


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
                materials[material_code] = {'material_code': material_code, 'material_name': material_name, 'lots': []}

            # Append the lot details to the material's 'lots' list
            materials[material_code]['lots'].append(lot)

    return materials


lambda_handler(None, None)
