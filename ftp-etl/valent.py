import json
import os
import urllib.parse
from datetime import datetime

import boto3
import paramiko
import requests

DEBUG = False
# SFTP connection details
HOST = 'sftpval.valent.com'
USERNAME = 'SAPEBRQA'
PASSWORD = 'X3Wmbnde4we'
REMOTE_DIR = 'Outbound'

dynamodb = boto3.client('dynamodb')
url = 'https://api.valent.demo.platform.leucinetech.com/v1/objects'
token = 'eyJhbGciOiJIUzUxMiJ9.eyJsYXN0TmFtZSI6IiIsIm9yZ2FuaXNhdGlvbklkIjoxNjE3NjI1NDAxLCJyb2xlcyI6W3siaWQiOiIxIiwibmFtZSI6IkFDQ09VTlRfT1dORVIifV0sImVtcGxveWVlSWQiOiJFVEwwMDEiLCJoYXNTZXRDaGFsbGVuZ2VRdWVzdGlvbiI6dHJ1ZSwiZmlyc3ROYW1lIjoiRVRMIiwiaWQiOjQyNjc0ODYzNjI4ODY5NjMyMCwiZmFjaWxpdHlJZHMiOlstMSwxNjE2MzY3ODAzLDE2MTYzNjc4MDIsMTYxNjM2NzgwMV0sInNlcnZpY2VJZCI6ImM2ZDgyODViNzJhODRlZmI4ZmJkNjA4YzdjYWRhNDg0IiwiZmFjaWxpdGllcyI6W3siaWQiOiItMSIsIm5hbWUiOiJHbG9iYWwgUG9ydGFsIn0seyJpZCI6IjE2MTYzNjc4MDEiLCJuYW1lIjoiVU5JVC0xIn0seyJpZCI6IjE2MTYzNjc4MDIiLCJuYW1lIjoiVU5JVC0yIn0seyJpZCI6IjE2MTYzNjc4MDMiLCJuYW1lIjoiVU5JVC0zIn1dLCJjdXJyZW50RmFjaWxpdHlJZCI6MTYxNjM2NzgwMSwicm9sZU5hbWVzIjpbIkFDQ09VTlRfT1dORVIiXSwiZW1haWwiOiJldGxAbWFpbGluYXRvci5jb20iLCJqdGkiOiIzZDUyZmMxZDJhZWI0Y2UxODEwNjMxMWEyMWZlMWJiZiIsInVzZXJuYW1lIjoiZXRsIiwic3ViIjoiZXRsIiwiaWF0IjoxNzAwNjYzNTM5LCJleHAiOjE3MDkyMTcxMzl9.H38fVDmZFXz-DyxaxFyDCrgVNugd2Q3F4S9gDqx5mhMxqpvRM2cxxptmBESNoVc1lyeMkyw87lqv3WouM5DHhg'
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
            'product_name': '64ca635b7de0fe33130a99ad',
            'production_order_number': '64ca635b7de0fe33130a99ae',
            'scheduled_start': '64ccc848a8a5a262c13c8085',
            'scheduled_end': '64ccc884a8a5a262c13c8086'
        },
        'relation': {
            'product': '64ccbe15a8a5a262c13c807b'
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
            'production_order': '64ccc013a8a5a262c13c8084'
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

def _display(message: str,  override=False):
    if DEBUG or override:
        print(message)


def _build_search_filter(key, value):
    return json.dumps({"op": "AND", "fields": [{"field": f"searchable.{key}", "op": "EQ", "values": [f"{value}"]}]})


def lambda_handler(event, context):
    sftp = setup_sftp_connection()
    unsynced_files = get_unsynced_files(sftp)
    process_files(sftp, unsynced_files)
    close_sftp_connection(sftp)


def setup_sftp_connection():
    # Establish an SSH connection
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(HOST, username=USERNAME, password=PASSWORD)
    sftp = ssh_client.open_sftp()
    return sftp


def get_unsynced_files(sftp):
    files = sftp.listdir(REMOTE_DIR)
    # List to hold unsynced files
    unsynced_files = []

    # Check each file against DynamoDB to see if it's been synced
    for file in files:
        try:
            if not _file_exist(file):
                unsynced_files.append(file)
        except Exception as e:
            print(e)
            # Depending on your error policy, you might continue, return, or raise
    return unsynced_files


def _file_exist(file_name):
    try:
        # Check if the file exists in DynamoDB
        response = dynamodb.get_item(TableName='FileList', Key={'id': {'S': file_name}})
        return 'Item' in response
    except Exception as e:
        print(f"Error checking file existence: {str(e)}")
        return False


# def _store_file(sftp, file_name):


def process_files(sftp, unsynced_files):
    _display(f"Starting to Process {len(unsynced_files)} files: {unsynced_files}", True)
    for filename in unsynced_files:
        _display("")
        _display(f"------------------- Processing file: {filename} -------------------", True)
        is_valid_file_extension = os.path.splitext(filename)[1].lower() == '.csv'
        if not is_valid_file_extension:
            _display(f"Skipping file {filename} due to invalid file extension.")
            continue
        file_path = f'{REMOTE_DIR}/{filename}'
        with sftp.open(file_path) as f:
            content = f.read().decode('utf-8')

        # Extract data
        data = _extract_data(content)
        header = data['header']
        components = data['components']
        product_code = header['product_code']
        product_name = header['product_name']
        production_order_number = header['production_order_number']
        scheduled_start = header['scheduled_start']
        scheduled_end = header['scheduled_end']
        batch_number = header['batch_number']

        product_obj = _process_product(product_code, product_name)
        _display(product_obj)

        production_order_obj = _process_production_order(production_order_number, product_name, scheduled_start,
                                                         scheduled_end, product_obj)
        _display(production_order_obj)

        batch_obj = _process_batch(batch_number, product_name, production_order_obj)
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
            bom_material_obj = _process_bom_material(name, bom_code, target_quantity, uom, production_order_obj, material_obj,
                                  semi_finished_product_obj)
            _display(bom_material_obj, True)



def _get_epoch(date_str, format='%m/%d/%Y'):
    date_str = date_str.replace('\r', '')
    datetime_obj = datetime.strptime(date_str, format)
    epoch = int(datetime_obj.timestamp())
    return epoch


def close_sftp_connection(sftp):
    sftp.close()
    # sftp.get_transport().close()


# content = """
# H|100024840|85447|BACILLUS THURINGIENSIS SLURRY FOR 48B|ZP01|A511|23000.000|KGM|290141BJ06|09/18/2023|09/18/2023
# O|100024840|0010|09/18/2023|00:00:00|A5BO00|A511|Overhead
# O|100024840|0020|09/18/2023|00:00:00|A5TS01|A511|Testing
# O|100024840|0030|09/18/2023|00:00:00|A5FR01|A511|Freight From Osg to AZ
# O|100024840|0040|09/18/2023|00:00:00|A5BO03|A511|Rotary Separator/Vibrating
# O|100024840|0050|09/18/2023|00:00:00|A5BO04|A511|Centrifuge
# O|100024840|0060|09/18/2023|00:00:00|A5BO05|A511|Thin Film Evaporator
# O|100024840|0070|09/18/2023|00:00:00|A5BO06|A511|Formulation/  Load Out
# O|100024840|0080|09/18/2023|00:00:00|A5CG01|A511|Chilled Glycol
# O|100024840|0090|09/18/2023|00:00:00|A5CW01|A511|Chilled Water
# O|100024840|0100|09/18/2023|00:00:00|A5PA01|A511|Process Air
# C|100024840|0010|22082|Enzyme, Liquid (FoodPro BSL)|8.993|KGM||0010
# C|100024840|0020|31432|POTASSIUM SORBATE|59.961|KGM||0010
# C|100024840|0030|45675|METHYLPARABEN|17.940|KGM||0010
# C|100024840|0040|72719|ENZYME, LIQUID (VISC|4.301|KGM||0010
# C|100024840|0050|64051|Sulfuric Acid 66 Deg Bulk Tanker|186.277|KGM||0010
# C|100024840|0060|23189|Sodium Hydroxide 30-32%|1496.173|KGM||0010
# C|100024840|0070|39102|Enzyme, Liquid (FoodPro CBL)|6.003|KGM||0010
# C|100024840|0080|15538|DIPEL FERMENTATION|99986.980|LTR||0010
# C|100024840|0090|23189|Sodium Hydroxide 30-32%|228.206|KGM||0010
# C|100024840|0100|A562014|Risil Mat|334.995|KGM||0010
# C|100024840|0110|A562016|Synergex (US)|10.005|KGM||0010
# C|100024840|0120|A585024|Electricity|1248.992|KWH||0010
# C|100024840|0130|A585026|Steam|26128.023|LBR||0010
# C|100024840|0140|A585028|Waste Water|77550.066|GLL||0010
# """

def _extract_data(content):
    # Split the content into lines and then by the pipe character
    lines = [line.split("|") for line in content.strip().split("\n")]
    header = {}
    components = []

    for line in lines:
        if line[0] == "H":
            # Extracting the data based on the corrected mapping from the header
            # Assuming the first line is always the header
            header_data = line
            product_name = header_data[3] if len(header_data) > 1 else None
            product_code = header_data[2] if len(header_data) > 1 else None
            production_order_number = header_data[1] if len(header_data) > 1 else None
            scheduled_start = str(_get_epoch(header_data[9])) if len(header_data) > 1 else None
            scheduled_end = str(_get_epoch(header_data[10])) if len(header_data) > 1 else None
            batch_number = header_data[8] if len(header_data) > 1 else None
            header = {
                'product_name': product_name,
                'product_code': product_code,
                'production_order_number': production_order_number,
                'scheduled_start': scheduled_start,
                'scheduled_end': scheduled_end,
                'batch_number': batch_number
            }
        elif line[0] == "C":
            component_data = line
            bom_code = f"{component_data[1]}_{component_data[2]}"
            code = component_data[3] if len(component_data) > 1 else None
            name = component_data[4] if len(component_data) > 1 else None
            target_quantity = component_data[5] if len(component_data) > 1 else None
            uom = component_data[6] if len(component_data) > 1 else None

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


def _process_production_order(production_order_number, product_name, scheduled_start, scheduled_end, product_obj):
    production_order_obj = _get_production_order(production_order_number)
    if production_order_obj is None:
        production_order_obj = _create_production_order(production_order_number, product_name, scheduled_start,
                                                        scheduled_end, product_obj)
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


def _create_production_order(production_order_number, product_name, scheduled_start, scheduled_end, product_obj):
    production_order_obj = None
    production_order_url = f"{url}"
    data = {
        'objectTypeId': key_mappings['production_order']['object_type_id'],
        'properties': {
            key_mappings['production_order']['property']['production_order_number']: production_order_number,
            key_mappings['production_order']['property']['product_name']: product_name,
            key_mappings['production_order']['property']['scheduled_start']: scheduled_start,
            key_mappings['production_order']['property']['scheduled_end']: scheduled_end
        },
        'relations': {
            key_mappings['production_order']['relation']['product']: [product_obj]
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


def _process_batch(batch_number, product_name, production_order_obj):
    batch_obj = _get_batch(batch_number)
    if batch_obj is None:
        batch_obj = _create_batch(batch_number, product_name, production_order_obj)
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


def _create_batch(batch_number, product_name, production_order_obj):
    batch_obj = None
    batch_url = f"{url}"
    data = {
        'objectTypeId': key_mappings['batch']['object_type_id'],
        'properties': {
            key_mappings['batch']['property']['batch_number']: batch_number,
            key_mappings['batch']['property']['product_name']: product_name,
        },
        'relations': {
            key_mappings['batch']['relation']['production_order']: [production_order_obj]
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


# lambda_handler(None, None)
