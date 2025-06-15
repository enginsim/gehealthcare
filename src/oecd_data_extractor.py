import requests
import xml.etree.ElementTree as ET
import os
import pandas as pd
import io
from datetime import datetime
import time
from pymongo import MongoClient, UpdateOne


class OECDDataFetcher:
    def __init__(self):
        self.data_cache = {}
        print("OECDDataFetcher initialized")

    def fetch_data_from_api(self, api_url, description="data"):
        print(f"Fetching {description}...\nURL: {api_url}")

        if "format=csv" not in api_url:
            separator = "&" if "?" in api_url else "?"
            api_url = f"{api_url}{separator}format=csv"

        try:
            response = requests.get(api_url)
            response.raise_for_status()

            if 'csv' in response.headers.get('Content-Type', '').lower() or b',' in response.content[:100]:
                df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
                print(f"Fetched {len(df)} rows of {description}")
                return df
            else:
                root = ET.fromstring(response.content)
                ns = {'ns': root.tag.split('}')[0].strip('{')} if '}' in root.tag else {}

                data_points = []
                for series in root.findall('.//{*}Series'):
                    series_data = series.attrib.copy()
                    for obs in series.findall('.//{*}Obs'):
                        obs_data = series_data.copy()
                        obs_data.update(obs.attrib)
                        data_points.append(obs_data)

                if data_points:
                    df = pd.DataFrame(data_points)
                    print(f"Fetched {len(df)} rows of {description} from XML")
                    return df
                else:
                    print("No data in XML response.")
                    return None

        except Exception as e:
            print(f"Error fetching/parsing data: {e}")
            return None

    def simplify_dataframe(self, df):
        if df is None or df.empty:
            return None

        simplified_df = pd.DataFrame()
        column_mapping = {
            'REF_AREA': 'CountryCode', 'LOCATION': 'CountryCode', 'Country': 'CountryCode',
            'COUNTRY': 'CountryCode', 'OBS_VALUE': 'Measure', 'VALUE': 'Measure',
            'Value': 'Measure', 'OBSVALUE': 'Measure', 'value': 'Measure',
            'TIME_PERIOD': 'Time period', 'TIME': 'Time period', 'Date': 'Time period',
            'PERIOD': 'Time period', 'ObsTime': 'Time period'
        }

        for original_col, target_col in column_mapping.items():
            if original_col in df.columns and target_col not in simplified_df.columns:
                simplified_df[target_col] = df[original_col]

        if 'CountryCode' not in simplified_df.columns:
            for col in df.columns:
                if col.upper() in ['REF_AREA', 'LOCATION', 'COUNTRY'] or df[col].astype(str).str.len().eq(3).mean() > 0.5:
                    simplified_df['CountryCode'] = df[col]
                    break

        if 'Measure' not in simplified_df.columns:
            numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
            if numeric_cols.any():
                simplified_df['Measure'] = df[numeric_cols[0]]

        if 'Time period' not in simplified_df.columns:
            for col in df.columns:
                if 'time' in col.lower() or 'date' in col.lower() or 'period' in col.lower():
                    simplified_df['Time period'] = df[col]
                    break

        for col in ['CountryCode', 'Measure', 'Time period']:
            if col not in simplified_df.columns:
                simplified_df[col] = 'N/A'

        simplified_df = simplified_df[['CountryCode', 'Measure', 'Time period']]
        try:
            simplified_df['Measure'] = pd.to_numeric(simplified_df['Measure'], errors='coerce')
        except:
            pass

        return simplified_df


def insert_to_mongodb(data: pd.DataFrame, collection_name: str) -> bool:
    MONGO_URI = os.getenv("MONGO_URI")
    MONGO_DB = os.getenv("MONGO_DB")

    if not MONGO_URI or not MONGO_DB:
        print("MongoDB environment variables not set.")
        return False

    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[collection_name]
        collection.create_index([("CountryCode", 1), ("Time period", 1)], unique=True)

        operations = []
        for _, row in data.iterrows():
            doc = row.dropna().to_dict()
            if "CountryCode" in doc and "Time period" in doc:
                operations.append(
                    UpdateOne(
                        {"CountryCode": doc["CountryCode"], "Time period": doc["Time period"]},
                        {"$set": doc},
                        upsert=True
                    )
                )

        if operations:
            collection.bulk_write(operations, ordered=False)
            print(f"Inserted {len(operations)} records into {collection_name}")
            return True
        return False

    except Exception as e:
        print(f"MongoDB insert error: {e}")
        return False


def get_oecd_data():
    fetcher = OECDDataFetcher()

    datasets = {
        "oecd_employment_rate": "https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_LFS@DF_IALFS_EMP_WAP_Q,1.0/LTU+LVA+KOR+JPN+ISR+IRL+ISL+HUN+GRC+DEU+FRA+FIN+EST+DNK+CZE+CRI+COL+CHL+BEL+CAN+AUT+AUS+ITA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA.EMP_WAP.._Z.Y._T.Y15T64..Q?startPeriod=2015-Q1",
        "oecd_reserve_assets": "https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_BOP@DF_IIP,1.0/SAU+IND+CHN+USA+GBR+CHE+SWE+ESP+SVN+SVK+PRT+POL+NOR+NZL+NLD+LUX+LTU+LVA+JPN+ITA+IRL+ISL+HUN+GRC+DEU+FIN+EST+DNK+CZE+CAN+BEL+AUT+AUS+FRA..FA_R_F_S121...Q.XDC.?startPeriod=2015-Q1",
        "oecd_debt_securities": "https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_BOP@DF_IIP,1.0/SAU+IND+CHN+USA+GBR+CHE+SWE+ESP+SVN+SVK+PRT+POL+NOR+NZL+NLD+LUX+LTU+LVA+JPN+ITA+IRL+ISL+HUN+GRC+DEU+FIN+EST+DNK+CZE+CAN+BEL+AUT+AUS+FRA..FA_P_F3...Q.XDC.?startPeriod=2015-Q1"
    }

    for name, url in datasets.items():
        print(f"\n=== Processing {name} ===")
        df = fetcher.fetch_data_from_api(url, name)
        if df is not None:
            simplified_df = fetcher.simplify_dataframe(df)
            insert_to_mongodb(simplified_df, name)

