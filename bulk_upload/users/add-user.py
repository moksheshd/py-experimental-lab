import json
import logging
import os
from datetime import datetime

import pandas as pd
import paramiko
import requests

DEBUG = True

# uat_token = 'eyJhbGciOiJIUzUxMiJ9.eyJsYXN0TmFtZSI6IiIsIm9yZ2FuaXNhdGlvbklkIjoxNjE3NjI1NDAxLCJyb2xlcyI6W3siaWQiOiIxIiwibmFtZSI6IkFDQ09VTlRfT1dORVIifV0sImVtcGxveWVlSWQiOiJCb3QwMSIsImhhc1NldENoYWxsZW5nZVF1ZXN0aW9uIjp0cnVlLCJmaXJzdE5hbWUiOiJMZXVjaW5lIEJvdCIsImlkIjo0Mjk5MjQzMTQ4NjM0NDM5NjgsImZhY2lsaXR5SWRzIjpbLTEsMTYxNjM2NzgwNCwxNjE2MzY3ODAzLDE2MTYzNjc4MDIsMTYxNjM2NzgwMV0sInNlcnZpY2VJZCI6ImM2ZDgyODViNzJhODRlZmI4ZmJkNjA4YzdjYWRhNDg0IiwiZmFjaWxpdGllcyI6W3siaWQiOiItMSIsIm5hbWUiOiJHbG9iYWwgUG9ydGFsIn0seyJpZCI6IjE2MTYzNjc4MDEiLCJuYW1lIjoiVW5pdCAxIn0seyJpZCI6IjE2MTYzNjc4MDIiLCJuYW1lIjoiVW5pdCAyIn0seyJpZCI6IjE2MTYzNjc4MDMiLCJuYW1lIjoiVW5pdCAzIn0seyJpZCI6IjE2MTYzNjc4MDQiLCJuYW1lIjoiVW5pdCA0In1dLCJjdXJyZW50RmFjaWxpdHlJZCI6MTYxNjM2NzgwMiwicm9sZU5hbWVzIjpbIkFDQ09VTlRfT1dORVIiXSwianRpIjoiMTFlOTk2Y2UyNzQ3NDJjZWFiZjI0ZmRjYThhNTczNWMiLCJ1c2VybmFtZSI6ImJvdDAxIiwic3ViIjoiYm90MDEiLCJpYXQiOjE3MDE0MjA1MDYsImV4cCI6MTcwOTk3NDEwNn0.ohOofJoIaSVNENpEmEGtCwGoTD9PwbKKvcuY3QcjMcaK0X5rE-vjyYnQJqzVRhcIYrizI8297DdtjaAwv17i0A'
# uat_url = 'https://api.valent.uat.platform.leucinetech.com/v1/objects'
# demo_token = 'eyJhbGciOiJIUzUxMiJ9.eyJsYXN0TmFtZSI6IiIsIm9yZ2FuaXNhdGlvbklkIjoxNjE3NjI1NDAxLCJyb2xlcyI6W3siaWQiOiIxIiwibmFtZSI6IkFDQ09VTlRfT1dORVIifV0sImVtcGxveWVlSWQiOiJCb3QwMSIsImhhc1NldENoYWxsZW5nZVF1ZXN0aW9uIjp0cnVlLCJmaXJzdE5hbWUiOiJMZXVjaW5lIEJvdCIsImlkIjo0Mjk5MjIxNzc1NTA5ODMxNjgsImZhY2lsaXR5SWRzIjpbLTEsMTY0MzEwMzUwMywxNjQzMTAzNTAyLDE2NDMxMDM1MDEsMTYxNjM2NzgwNCwxNjE2MzY3ODAzLDE2MTYzNjc4MDIsMTYxNjM2NzgwMV0sInNlcnZpY2VJZCI6ImM2ZDgyODViNzJhODRlZmI4ZmJkNjA4YzdjYWRhNDg0IiwiZmFjaWxpdGllcyI6W3siaWQiOiItMSIsIm5hbWUiOiJHbG9iYWwgUG9ydGFsIn0seyJpZCI6IjE2NDMxMDM1MDIiLCJuYW1lIjoiTG9uZG9uIn0seyJpZCI6IjE2NDMxMDM1MDEiLCJuYW1lIjoiTmV3IFlvcmsifSx7ImlkIjoiMTY0MzEwMzUwMyIsIm5hbWUiOiJTeWRuZXkifSx7ImlkIjoiMTYxNjM2NzgwMSIsIm5hbWUiOiJVbml0IDEifSx7ImlkIjoiMTYxNjM2NzgwMiIsIm5hbWUiOiJVbml0IDIifSx7ImlkIjoiMTYxNjM2NzgwMyIsIm5hbWUiOiJVbml0IDMifSx7ImlkIjoiMTYxNjM2NzgwNCIsIm5hbWUiOiJVbml0IDQifV0sImN1cnJlbnRGYWNpbGl0eUlkIjoxNjE2MzY3ODAyLCJyb2xlTmFtZXMiOlsiQUNDT1VOVF9PV05FUiJdLCJqdGkiOiI2MDk3Y2IyZTU3OGQ0MzIzYTUzYTA5YzE4OWQzZjYxNyIsInVzZXJuYW1lIjoiYm90MDEiLCJzdWIiOiJib3QwMSIsImlhdCI6MTcwMTQyMDA2MywiZXhwIjoxNzA5OTczNjYzfQ.LPT6Y1WFj_SDvi7aSIeP6ukRBgkuXI-ufW_TEztPdGCtBYPOLsb5cg9UtYC0aNLryIfzodW0joL-G-ExYJjxrw'
# uat_url = 'https://api.valent.demo.platform.leucinetech.com/v1/objects'
# url = uat_url
# token = demo_token
url = 'https://api.c.qa.platform.leucinetech.com/v1/users'
token = 'eyJhbGciOiJIUzUxMiJ9.eyJsYXN0TmFtZSI6IkwiLCJvcmdhbmlzYXRpb25JZCI6MTYxNzYyNTQwMSwicm9sZXMiOlt7ImlkIjoiMSIsIm5hbWUiOiJBQ0NPVU5UX09XTkVSIn1dLCJlbXBsb3llZUlkIjoiTC1hY2NvdW50Lm93bmVyLjAxIiwiaGFzU2V0Q2hhbGxlbmdlUXVlc3Rpb24iOnRydWUsImZpcnN0TmFtZSI6IlZpdmVrIiwiaWQiOjE2MTc2MjU0MDAsImZhY2lsaXR5SWRzIjpbLTEsMTcwOTE0MzUxOSwxNzA5MTQzNTE4LDE3MDkxNDM1MTcsMTcwOTE0MzUxNiwxNzA5MTQzNTE1LDE3MDkxNDM1MTQsMTcwOTE0MzUxMywxNzA5MTQzNTEyLDE3MDkxNDM1MTEsMTcwOTE0MzUxMCwxNzA5MTQzNTA5LDE3MDkxNDM1MDgsMTcwOTE0MzUwNywxNzA5MTQzNTA2LDE3MDkxNDM1MDUsMTcwOTE0MzUwNCwxNzA5MTQzNTAzLDE3MDkxNDM1MDIsMTcwOTE0MzUwMSwxNzA5MTQzNTAwLDE2NDMxMDM1MDMsMTY0MzEwMzUwMiwxNjQzMTAzNTAxLDE2MTYzNjc4MDMsMTYxNjM2NzgwMiwxNjE2MzY3ODAxLDE3MDkxNDM1MjUsMTcwOTE0MzUyNCwxNzA5MTQzNTIzLDE3MDkxNDM1MjIsMTcwOTE0MzUyMSwxNzA5MTQzNTIwXSwic2VydmljZUlkIjoiYzZkODI4NWI3MmE4NGVmYjhmYmQ2MDhjN2NhZGE0ODQiLCJmYWNpbGl0aWVzIjpbeyJpZCI6Ii0xIiwibmFtZSI6Ikdsb2JhbCBQb3J0YWwifSx7ImlkIjoiMTYxNjM2NzgwMSIsIm5hbWUiOiJCYW5nYWxvcmUifSx7ImlkIjoiMTYxNjM2NzgwMyIsIm5hbWUiOiJEZWxoaSJ9LHsiaWQiOiIxNzA5MTQzNTAwIiwibmFtZSI6Ikdyb3VwIEEifSx7ImlkIjoiMTcwOTE0MzUwMSIsIm5hbWUiOiJHcm91cCBCIn0seyJpZCI6IjE3MDkxNDM1MDIiLCJuYW1lIjoiR3JvdXAgQyJ9LHsiaWQiOiIxNzA5MTQzNTAzIiwibmFtZSI6Ikdyb3VwIEQifSx7ImlkIjoiMTcwOTE0MzUwNCIsIm5hbWUiOiJHcm91cCBFIn0seyJpZCI6IjE3MDkxNDM1MDUiLCJuYW1lIjoiR3JvdXAgRiJ9LHsiaWQiOiIxNzA5MTQzNTA2IiwibmFtZSI6Ikdyb3VwIEcifSx7ImlkIjoiMTcwOTE0MzUwNyIsIm5hbWUiOiJHcm91cCBIIn0seyJpZCI6IjE3MDkxNDM1MDgiLCJuYW1lIjoiR3JvdXAgSSJ9LHsiaWQiOiIxNzA5MTQzNTA5IiwibmFtZSI6Ikdyb3VwIEoifSx7ImlkIjoiMTcwOTE0MzUxMCIsIm5hbWUiOiJHcm91cCBLIn0seyJpZCI6IjE3MDkxNDM1MTEiLCJuYW1lIjoiR3JvdXAgTCJ9LHsiaWQiOiIxNzA5MTQzNTEyIiwibmFtZSI6Ikdyb3VwIE0ifSx7ImlkIjoiMTcwOTE0MzUxMyIsIm5hbWUiOiJHcm91cCBOIn0seyJpZCI6IjE3MDkxNDM1MTQiLCJuYW1lIjoiR3JvdXAgTyJ9LHsiaWQiOiIxNzA5MTQzNTE1IiwibmFtZSI6Ikdyb3VwIFAifSx7ImlkIjoiMTcwOTE0MzUxNiIsIm5hbWUiOiJHcm91cCBRIn0seyJpZCI6IjE3MDkxNDM1MTciLCJuYW1lIjoiR3JvdXAgUiJ9LHsiaWQiOiIxNzA5MTQzNTE4IiwibmFtZSI6Ikdyb3VwIFMifSx7ImlkIjoiMTcwOTE0MzUxOSIsIm5hbWUiOiJHcm91cCBUIn0seyJpZCI6IjE3MDkxNDM1MjAiLCJuYW1lIjoiR3JvdXAgVSJ9LHsiaWQiOiIxNzA5MTQzNTIxIiwibmFtZSI6Ikdyb3VwIFYifSx7ImlkIjoiMTcwOTE0MzUyMiIsIm5hbWUiOiJHcm91cCBXIn0seyJpZCI6IjE3MDkxNDM1MjMiLCJuYW1lIjoiR3JvdXAgWCJ9LHsiaWQiOiIxNzA5MTQzNTI0IiwibmFtZSI6Ikdyb3VwIFkifSx7ImlkIjoiMTcwOTE0MzUyNSIsIm5hbWUiOiJHcm91cCBaIn0seyJpZCI6IjE2NDMxMDM1MDIiLCJuYW1lIjoiTG9uZG9uIn0seyJpZCI6IjE2MTYzNjc4MDIiLCJuYW1lIjoiTXVtYmFpIn0seyJpZCI6IjE2NDMxMDM1MDEiLCJuYW1lIjoiTmV3IFlvcmsifSx7ImlkIjoiMTY0MzEwMzUwMyIsIm5hbWUiOiJTeWRuZXkifV0sImN1cnJlbnRGYWNpbGl0eUlkIjoxNzA5MTQzNTAwLCJyb2xlTmFtZXMiOlsiQUNDT1VOVF9PV05FUiJdLCJlbWFpbCI6ImFvLjAxQG1haWxpbmF0b3IuY29tIiwianRpIjoiN2ZmYTYxYzY1YTJmNDYyMTkwZjI3YzBlN2I2ZmEzNzIiLCJ1c2VybmFtZSI6ImFjY291bnQub3duZXIuMDEiLCJzdWIiOiJhY2NvdW50Lm93bmVyLjAxIiwiaWF0IjoxNzA5MTQ3MzYyLCJleHAiOjE3MDkxNTQ1NjJ9.9X-JqDFBVGoM2fDbbdNHarBv6e0T6DrDqWJ_Ms7NpWyQMrmJq14ABVAd_pIxzVzEFIoi2aYjaQGPQtEneAmZGw'

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

role_mapping = {
    "ACCOUNT_OWNER": "1",
    "FACILITY_ADMIN": "2",
    "SYSTEM_ADMIN": "3",
    "SUPERVISOR": "4",
    "OPERATOR": "5",
    "PROCESS_PUBLISHER": "6",
    "GLOBAL_ADMIN": "7",
}

def _display(message: str, override=False):
    logging.info(message)
    if DEBUG or override:
        print(message)


def _build_search_filter(key, value):
    return json.dumps({"op": "AND", "fields": [{"field": f"searchable.{key}", "op": "EQ", "values": [f"{value}"]}]})


def lambda_handler(event, context):
    process_user_files("/home/moksh/misc/temp/users.xlsx")

def _process_user(first_name, last_name, email, employee_id, facility_id, username, role_id):
    user_obj = _create_user(first_name, last_name, email, employee_id, facility_id, username, role_id)
    return user_obj


def _create_user(first_name, last_name, email, employee_id, facility_id, username, role_id):
    user_url = f"{url}"
    data = {
            "facilities": [{"id": facility_id}],
            "roles": [{"id": role_id}],
            "userType": "LOCAL",
            "firstName": first_name,
            "lastName": last_name,
            "employeeId": employee_id,
            "email": email,
            "department": "",
            "reason": "New User"
            }
    response = requests.post(user_url, json=data, headers=headers)
    if response.status_code == 200:
        response = response.json()
    user_obj = response['data']
    return user_obj



def process_user_files(file_path):
    _display(f"Starting to Process files: {file_path}", True)
    _display("")
    valid_extensions = {".xls", ".xlsx"}
    is_valid_file_extension = os.path.splitext(file_path)[1].lower() in valid_extensions
    if not is_valid_file_extension:
        _display(f"Skipping file {file_path} due to invalid file extension.")
    with open(file_path, 'rb') as f:
        content = pd.read_excel(f)
    for index, row in content.iterrows():
        first_name = str(row['first_name']).strip() if pd.notna(row['first_name']) else ''
        last_name = str(row['last_name']).strip() if pd.notna(row['last_name']) else ''
        email = str(row['email']).strip() if pd.notna(row['email']) else ''
        employee_id = str(row['employee_id']).strip() if pd.notna(row['employee_id']) else ''
        facility_id = str(row['facility_id']).strip() if pd.notna(row['facility_id']) else ''
        username = str(row['username']).strip() if pd.notna(row['username']) else ''
        role = str(row['role']).strip() if pd.notna(row['role']) else ''
        role_id = str(role_mapping.get(role))
        _process_user(first_name, last_name, email, employee_id, facility_id, username, role_id)
    print(content)
    # lines = [line.split("|") for line in content.strip().split("\n")]


lambda_handler(None, None)
