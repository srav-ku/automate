import os
import json
import gspread
from google.oauth2.service_account import Credentials

GSHEET_ID = os.environ["GSHEET_ID"]
GSHEETS_CREDENTIALS = os.environ["GSHEETS_CREDENTIALS"]

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def get_sheet(tab_name):
    creds_dict = json.loads(GSHEETS_CREDENTIALS)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(GSHEET_ID).worksheet(tab_name)

def read_pending_rows(tab_name):
    sheet = get_sheet(tab_name)
    values = sheet.get_all_values()

    headers = values[0]
    rows = []

    for idx, row in enumerate(values[1:], start=2):
        data = dict(zip(headers, row))
        if not data.get("Status"):
            data["row"] = idx
            rows.append(data)

    return rows

def get_max_assigned_number(tab_name):
    sheet = get_sheet(tab_name)
    values = sheet.get_all_values()[1:]

    nums = []
    for row in values:
        try:
            nums.append(int(row[4]))  # Assigned Number column (E)
        except:
            pass

    return max(nums) if nums else 0

def update_row(tab_name, row, status, assigned_number="", error=""):
    sheet = get_sheet(tab_name)

    sheet.update(f"D{row}", [[status]])
    sheet.update(f"E{row}", [[assigned_number]])
    sheet.update(f"F{row}", [[error]])
