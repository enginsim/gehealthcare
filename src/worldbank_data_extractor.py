import os
import zipfile
import requests
import pandas as pd

def extract_file_to_csv(filepath):
    
    try:
        df = pd.read_excel(filepath)

        expected_columns = [
            'codeindyr', 'code', 'countryname', 'year', 'indicator',
            'estimate', 'stddev', 'nsource',
            'pctrank', 'pctranklower', 'pctrankupper'
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
        
        # Save to csv
        csv_path = filepath.replace("data/raw","data/processed").replace(".xlsx", ".csv")
        df.to_csv(csv_path, index=False)

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
                extract_file_to_csv(excel_path)

        raise FileNotFoundError("File not found.")
    
    except Exception as e:
        print(f"An error occured: {e}")
        return None

def get_worldbank_data():
    url = "https://www.worldbank.org/content/dam/sites/govindicators/doc/wgidataset_excel.zip"
    download_and_extract_data(url)