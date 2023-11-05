import json
import urllib.parse
from datetime import datetime

import boto3
import paramiko
import requests

# SFTP connection details
HOST = 'sftpval.valent.com'
USERNAME = 'SAPEBRQA'
PASSWORD = 'X3Wmbnde4we'
REMOTE_DIR = 'Outbound'

dynamodb = boto3.client('dynamodb')
url = 'https://api.valent.uat.platform.leucinetech.com/v1/objects'
token = 'eyJhbGciOiJIUzUxMiJ9.eyJsYXN0TmFtZSI6IkwiLCJvcmdhbmlzYXRpb25JZCI6MTYxNzYyNTQwMSwicm9sZXMiOlt7ImlkIjoiMiIsIm5hbWUiOiJGQUNJTElUWV9BRE1JTiJ9XSwiZW1wbG95ZWVJZCI6IkwtZmFjaWxpdHkuYWRtaW4uMDEiLCJoYXNTZXRDaGFsbGVuZ2VRdWVzdGlvbiI6dHJ1ZSwiZmlyc3ROYW1lIjoiUHVzaHBhIiwiaWQiOjE2MTc2MjU0MDUsImZhY2lsaXR5SWRzIjpbMTYxNjM2NzgwNCwxNjE2MzY3ODAxXSwic2VydmljZUlkIjoiYzZkODI4NWI3MmE4NGVmYjhmYmQ2MDhjN2NhZGE0ODQiLCJmYWNpbGl0aWVzIjpbeyJpZCI6IjE2MTYzNjc4MDEiLCJuYW1lIjoiVW5pdCAxIn0seyJpZCI6IjE2MTYzNjc4MDQiLCJuYW1lIjoiVW5pdCA0In1dLCJjdXJyZW50RmFjaWxpdHlJZCI6MTYxNjM2NzgwMSwicm9sZU5hbWVzIjpbIkZBQ0lMSVRZX0FETUlOIl0sImVtYWlsIjoiZmEuMDFAbWFpbGluYXRvci5jb20iLCJqdGkiOiJjMjJlNDAwNTliNzc0OGZiYTZiMjU2ZmIyN2FhYzVkNCIsInVzZXJuYW1lIjoiZmFjaWxpdHkuYWRtaW4uMDEiLCJzdWIiOiJmYWNpbGl0eS5hZG1pbi4wMSIsImlhdCI6MTY5OTAyMTA0MiwiZXhwIjoxNzA3NTc0NjQyfQ.PN7HSmKagw6s5gybQhHTQ5GdMDfvIb3Dh0Ue2lXqT-cJPOwHxoEh0DyNfNa1prfqEAb3YbfOOiwodrIegpVKMw'
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
    }
}


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
    for file in unsynced_files:
        print("--------------------------------------")
        file_path = f'{REMOTE_DIR}/{file}'
        with sftp.open(file_path) as f:
            content = f.read().decode('utf-8')

        # Extract data
        data = extract_data(content)
        product_code = data['product_code']
        product_name = data['product_name']
        production_order_number = data['production_order_number']
        scheduled_start = data['scheduled_start']
        scheduled_end = data['scheduled_end']
        batch_number = data['batch_number']

        product_obj = _process_product(product_code, product_name)
        print(product_obj)
        production_order_obj = _process_production_order(production_order_number, product_name, scheduled_start,
                                                         scheduled_end, product_obj)
        print(production_order_obj)
        batch_obj = _process_batch(batch_number, product_name, production_order_obj)
        print(batch_obj)

        # Mark file as synced (e.g. store its name in S3 or DynamoDB)


def extract_data(content):
    # Split the content into lines and then by the pipe character
    lines = [line.split("|") for line in content.strip().split("\n")]

    # Extracting the data based on the corrected mapping from the header
    # Assuming the first line is always the header
    header_data = lines[0] if lines else []

    product_name = header_data[3] if len(header_data) > 1 else None
    product_code = header_data[2] if len(header_data) > 1 else None
    production_order_number = header_data[1] if len(header_data) > 1 else None
    scheduled_start = str(_get_epoch(header_data[9])) if len(header_data) > 1 else None
    scheduled_end = str(_get_epoch(header_data[10])) if len(header_data) > 1 else None
    batch_number = header_data[8] if len(header_data) > 1 else None

    # Mapping the data based on provided Excel-like cell positions
    extracted_data = {
        'product_name': product_name,
        'product_code': product_code,
        'production_order_number': production_order_number,
        'scheduled_start': scheduled_start,
        'scheduled_end': scheduled_end,
        'batch_number': batch_number
    }
    return extracted_data


def _get_epoch(date_str, format='%m/%d/%Y'):
    date_str = date_str.replace('\r', '')
    datetime_obj = datetime.strptime(date_str, format)
    epoch = int(datetime_obj.timestamp())
    return epoch


def close_sftp_connection(sftp):
    sftp.close()
    sftp.get_transport().close()


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
# data = extract_data(content)

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


def _build_search_filter(key, value):
    return json.dumps({"op": "AND", "fields": [{"field": f"searchable.{key}", "op": "EQ", "values": [f"{value}"]}]})


# product_code = "P003"
# product_name = "P003"
# production_order_number = "PO002"
# scheduled_start = "1698949800"
# scheduled_end = "1699122600"
# batch_number = "B002"
#
# product_obj = _process_product(product_code, product_name)
# print(product_obj)
# production_order_obj = _process_production_order(production_order_number, product_name, scheduled_start, scheduled_end, product_obj)
# print(production_order_obj)
# batch_obj = _process_batch(batch_number, product_name, production_order_obj)
# print(batch_obj)


# lambda_handler(None, None)

