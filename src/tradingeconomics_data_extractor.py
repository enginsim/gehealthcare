import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
from pymongo import MongoClient, UpdateOne

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = "tradingeconomics_data"

def get_countries(base_url="https://tradingeconomics.com/country-list/rating"):
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(base_url, headers=headers)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", class_="table")

    countries = []
    if table:
        for row in table.find_all("tr")[1:]:
            link = row.find("a")
            if link and "/country/" not in link["href"]:
                href = link["href"].strip("/")
                country = href.split("/")[0]
                countries.append(country)

    return list(set(countries))




def get_country_ratings(country_name):
    url = f"https://tradingeconomics.com/{country_name}/rating"
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return pd.DataFrame()

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")

    if not table:
        return pd.DataFrame()

    rows = table.find_all("tr")[1:]
    data = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) >= 4:
            agency = cols[0].get_text(strip=True)
            rating = cols[1].get_text(strip=True)
            outlook = cols[2].get_text(strip=True)
            date = cols[3].get_text(strip=True)
            data.append({
                "Country": country_name.capitalize(),
                "Agency": agency,
                "Rating": rating,
                "Outlook": outlook,
                "Date": date
            })

    return data



def get_tradingeconomics_data():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    collection.create_index(
        [("Country", 1), ("Agency", 1), ("Date", 1)],
        unique=True
    )
    names = get_countries()
    operations = []

    for i, name in enumerate(names):
        try:
            country_data = get_country_ratings(name)
            for doc in country_data:
                operations.append(
                    UpdateOne(
                        {"Country": doc["Country"], "Agency": doc["Agency"], "Date": doc["Date"]},
                        {"$set": doc},
                        upsert=True
                    )
                )
        except Exception as e:
            print(f"Error {name}: {e}")

        time.sleep(1)

    if operations:
        result = collection.bulk_write(operations, ordered=False)
        print("Operation completed")
    else:
        print("No operations to perform.")