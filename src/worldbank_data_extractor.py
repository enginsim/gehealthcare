import os
import zipfile
import requests
import pandas as pd
from .db_config import db_config, db_name
import mysql.connector

def extract_file_to_mysql(filepath,db_name, table_name="worldbank_data"):
    
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
        


        # Convert to list of tuples
        data = [tuple(row) for row in df.itertuples(index=False, name=None)]

        conn = None
        cursor = None

        try:
            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor()
            cursor.execute(f"USE {db_name}")
            insert_query = f"""
                INSERT IGNORE INTO {table_name}
                (codeindyr, code, countryname, year, indicator,
                estimate, stddev, nsource, pctrank, pctranklower, pctrankupper)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(insert_query, data)
            conn.commit()

        except mysql.connector.Error as err:
            print(f"[Error] {err}")
        except Exception as e:
            print(f"[Error] {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        # Delete excel file
        if os.path.exists(filepath):
            os.remove(filepath)

        # Delete zip file
        zip_path = os.path.join(os.path.dirname(filepath), "wgidata.zip")
        if os.path.exists(zip_path):
            os.remove(zip_path)
        #Delete pdf file
        pdf_path = os.path.join(os.path.dirname(filepath), "readme.pdf")
        if os.path.exists(pdf_path):
            os.remove(pdf_path)

    except Exception as e:
        print(f"Error loading dataset: {e}")
        return None

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
                extract_file_to_mysql(excel_path, db_name, table_name="worldbank_data")
    
    except Exception as e:
        print(f"An error occured1: {e}")
        return None

def get_worldbank_data():
    url = "https://www.worldbank.org/content/dam/sites/govindicators/doc/wgidataset_excel.zip"
    download_and_extract_data(url)