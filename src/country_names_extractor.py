import requests
import pandas as pd
from pymongo import MongoClient
import os
from io import StringIO

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = "countries"

def get_allianz_country_list():
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client["gehealthcare"] 
    collection = db["allianz_data"]
    distinct_countries = collection.distinct("country")
    df = pd.DataFrame(distinct_countries, columns=["country"])
    df.to_csv("allianz_distinct_countries.csv", index=False)

def get_tradingeconomics_country_list():
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client["gehealthcare"]
    collection = db["tradingeconomics_data"]
    distinct_countries = collection.distinct("Country")
    df = pd.DataFrame(distinct_countries, columns=["Country"])
    df.to_csv("tradingeconomics_distinct_countries.csv", index=False)

def get_countryeconomy_country_list():
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client["gehealthcare"] 
    collection = db["countryeconomy_data"] 
    distinct_countries = collection.distinct("Country")
    df = pd.DataFrame(distinct_countries, columns=["Country"])
    df.to_csv("countryeconomy_distinct_countries.csv", index=False)

def get_countries():
    url = "https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.csv"
    resp = requests.get(url)
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.text))

   
    records = df.to_dict(orient="records")


    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    col = db[MONGO_COLLECTION]

    # col.delete_many({})
    # col.insert_many(records)

    print(f"Processed {len(records)} records.")

def get_worldbank_country():
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client["gehealthcare"] 
    collection = db["worldbank_data"] 
    distinct_countries = collection.distinct("code")
    df = pd.DataFrame(distinct_countries, columns=["code"])
    df.to_csv("worldbank_distinct_countries.csv", index=False)

def get_oecd_country():
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client["gehealthcare"] 
    collection = db["oecd_employment_rate"] 
    distinct_countries = collection.distinct("CountryCode")
    df = pd.DataFrame(distinct_countries, columns=["CountryCode"])
    df.to_csv("oecd_distinct_countries.csv", index=False)
