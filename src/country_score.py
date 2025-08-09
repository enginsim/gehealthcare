from pymongo import MongoClient
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import pandas as pd
import math
import numpy as np

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = "country_scores"

SERVICE_ACCOUNT_FILE = 'tokyo-scholar-464119-b4-6a8fa808f85e.json' 
SPREADSHEET_NAME = 'country_scores' 

def write_to_drive():
    try:
        df = get_data_from_db()
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
        client_gs = gspread.authorize(creds)
        try:
            sheet = client_gs.open(SPREADSHEET_NAME)
        except gspread.SpreadsheetNotFound:
            sheet = client_gs.create(SPREADSHEET_NAME)

        worksheet = sheet.get_worksheet(0)
        worksheet.clear()
        
        worksheet.update([df.columns.values.tolist()] + df.fillna("").values.tolist())

    except Exception as e:
        print(f"Error: {e}")

def get_data_from_db():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    cursor = collection.find({}, {"_id": 0, "country": 1, "alpha3": 1, "overall_final_score": 1})
    df = pd.DataFrame(list(cursor), columns=["country", "alpha3", "overall_final_score"])
    df["overall_final_score"] = pd.to_numeric(df["overall_final_score"], errors="coerce")
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    return df