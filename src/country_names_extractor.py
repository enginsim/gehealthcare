import requests
import pandas as pd
from pymongo import MongoClient
from urllib.parse import quote_plus
import os

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = "countries"

def get_countries():

    url = "https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.csv"
    resp = requests.get(url)
    resp.raise_for_status()
    with open("all_countries.csv", "wb") as f:
        f.write(resp.content)

    df = pd.read_csv("all_countries.csv")
    
    records = df.to_dict(orient="records")

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    col = db[MONGO_COLLECTION]

    col.delete_many({})
    col.insert_many(records)
    print(f"Inserted {len(records)} documents.")

def get_country_names(output='countries.csv'):
    url = 'https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.csv'

    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error downloading the CSV: {e}")
        return

    lines = response.text.strip().splitlines()
    reader = csv.DictReader(lines)

    with open(output, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Country', 'Alpha-3'])

        for row in reader:
            writer.writerow([row['name'], row['alpha-3']])

    print(f"Countries saved to {output}")


def get_allianz_country():
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client["gehealthcare"]  # replace with your actual DB name
    collection = db["allianz_data"]  # replace with your collection name
    distinct_countries = collection.distinct("country")
    df = pd.DataFrame(distinct_countries, columns=["country"])
    df.to_csv("allianz_distinct_countries.csv", index=False)


def get_tradingeconomics_country():
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client["gehealthcare"]  # replace with your actual DB name
    collection = db["tradingeconomics_data"]  # replace with your collection name
    distinct_countries = collection.distinct("Country")
    df = pd.DataFrame(distinct_countries, columns=["Country"])
    df.to_csv("tradingeconomics_distinct_countries.csv", index=False)

def get_countryeconomy_country():
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client["gehealthcare"]  # replace with your actual DB name
    collection = db["countryeconomy_data"]  # replace with your collection name
    distinct_countries = collection.distinct("Country")
    df = pd.DataFrame(distinct_countries, columns=["Country"])
    df.to_csv("countryeconomy_distinct_countries.csv", index=False)

def get_worldbank_country():
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client["gehealthcare"]  # replace with your actual DB name
    collection = db["worldbank_data"]  # replace with your collection name
    distinct_countries = collection.distinct("code")
    df = pd.DataFrame(distinct_countries, columns=["code"])
    df.to_csv("worldbank_distinct_countries.csv", index=False)

def get_oecd_country():
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client["gehealthcare"]  # replace with your actual DB name
    collection = db["oecd_employment_rate"]  # replace with your collection name
    distinct_countries = collection.distinct("CountryCode")
    df = pd.DataFrame(distinct_countries, columns=["CountryCode"])
    df.to_csv("oecd_distinct_countries.csv", index=False)
