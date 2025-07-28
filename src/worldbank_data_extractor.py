import os
import zipfile
import requests
import pandas as pd
from pymongo import MongoClient
import gspread
from oauth2client.service_account import ServiceAccountCredentials


MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = "worldbank_data"

SERVICE_ACCOUNT_FILE = 'tokyo-scholar-464119-b4-6a8fa808f85e.json' 
SPREADSHEET_NAME = 'worldbank_data' 

def extract_file_to_mongodb(filepath, mongo_uri, db_name, collection_name="worldbank_data"):
    try:
        df = pd.read_excel(filepath)

        expected_columns = [
            "codeindyr", "code", "countryname", "year", "indicator",
            "estimate", "stddev", "nsource", "pctrank", "pctranklower", "pctrankupper"
        ]

        missing = set(expected_columns) - set(df.columns)
        if missing:
            raise ValueError(f"Missing expected columns: {missing}")

        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        df['estimate'] = pd.to_numeric(df['estimate'], errors='coerce')
        df['stddev'] = pd.to_numeric(df['stddev'], errors='coerce')
        df['nsource'] = pd.to_numeric(df['nsource'], errors='coerce')
        df['pctrank'] = pd.to_numeric(df['pctrank'], errors='coerce')
        df['pctranklower'] = pd.to_numeric(df['pctranklower'], errors='coerce')
        df['pctrankupper'] = pd.to_numeric(df['pctrankupper'], errors='coerce')

        df.dropna(subset=['code', 'countryname', 'year', 'indicator', 'estimate'], inplace=True)

        records = df.to_dict(orient="records")

        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        for record in records:
            collection.update_one(
                {
                    "codeindyr": record["codeindyr"],
                    "indicator": record["indicator"],
                    "year": record["year"]
                },
                {"$set": record},
                upsert=True
            )

        client.close()
        print("Insert completed")
    except Exception as e:
        print(f"Error loading dataset to MongoDB: {e}")

def download_and_extract_data(url, extract_to="data/raw"):
    
    zip_path = os.path.join(extract_to, "wgidata.zip")
    os.makedirs(extract_to, exist_ok=True)

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        for filename in os.listdir(extract_to):
            if filename.lower() == "wgidataset.xlsx":
                excel_path = os.path.join(extract_to, filename)
                extract_file_to_mongodb(excel_path, MONGO_URI, MONGO_DB, collection_name=MONGO_COLLECTION)
    
    except Exception as e:
        print(f"An error occured1: {e}")
        return None

def get_worldbank_data():
    url = "https://www.worldbank.org/content/dam/sites/govindicators/doc/wgidataset_excel.zip"
    download_and_extract_data(url)
    write_to_drive()

def write_to_drive():
    try:
        df = get_data_from_db()
            
        max_quarters = df.groupby("countryname")["year"].transform("max")
        df["most_recent"] = (df["year"] == max_quarters).astype(int)
       
        
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
        client_gs = gspread.authorize(creds)
        try:
            sheet = client_gs.open(SPREADSHEET_NAME)
        except gspread.SpreadsheetNotFound:
            sheet = client_gs.create(SPREADSHEET_NAME)

        worksheet = sheet.get_worksheet(0)
        worksheet.clear()
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())

    except Exception as e:
        print(f"Error: {e}")

def get_data_from_db():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    data = list(collection.find())
    df = pd.DataFrame(data)

    if "_id" in df.columns:
        df = df.drop(columns=["_id"])
    return df
