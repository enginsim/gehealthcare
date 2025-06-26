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
    try:
        url = f"https://tradingeconomics.com/{country_name}/rating"
        headers = {"User-Agent": "Mozilla/5.0"}

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            return pd.DataFrame()

        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table")

        if not table:
            return pd.DataFrame()

        country_code = get_country_code(country_name.capitalize())
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
                    "Date": date,
                    "Alpha3":country_code
                })
        return data
    except Exception as e:
        print(f"Error {e}")
    return []



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

def get_country_code(name):
    country_code ={
  "Albania": "ALB",
  "Andorra": "AND",
  "Angola": "AGO",
  "Argentina": "ARG",
  "Armenia": "ARM",
  "Aruba": "ABW",
  "Australia": "AUS",
  "Austria": "AUT",
  "Azerbaijan": "AZE",
  "Bahamas": "BHS",
  "Bahrain": "BHR",
  "Bangladesh": "BGD",
  "Barbados": "BRB",
  "Belarus": "BLR",
  "Belgium": "BEL",
  "Belize": "BLZ",
  "Benin": "BEN",
  "Bermuda": "BMU",
  "Bolivia": "BOL",
  "Bosnia-and-herzegovina": "BIH",
  "Botswana": "BWA",
  "Brazil": "BRA",
  "Bulgaria": "BGR",
  "Burkina-faso": "BFA",
  "Cambodia": "KHM",
  "Cameroon": "CMR",
  "Canada": "CAN",
  "Cape-verde": "UNKNOWN",
  "Cayman-islands": "CYM",
  "Chad": "TCD",
  "Chile": "CHL",
  "China": "CHN",
  "Colombia": "COL",
  "Congo": "COG",
  "Costa-rica": "CRI",
  "Croatia": "HRV",
  "Cuba": "CUB",
  "Cyprus": "CYP",
  "Czech-republic": "CZE",
  "Denmark": "DNK",
  "Dominican-republic": "DOM",
  "Ecuador": "ECU",
  "Egypt": "EGY",
  "El-salvador": "SLV",
  "Estonia": "EST",
  "Ethiopia": "ETH",
  "European-union": "UNKNOWN",
  "Fiji": "FJI",
  "Finland": "FIN",
  "France": "FRA",
  "Gabon": "GAB",
  "Georgia": "GEO",
  "Germany": "DEU",
  "Ghana": "GHA",
  "Greece": "GRC",
  "Grenada": "GRD",
  "Guatemala": "GTM",
  "Honduras": "HND",
  "Hong-kong": "HKG",
  "Hungary": "HUN",
  "Iceland": "ISL",
  "India": "IND",
  "Indonesia": "IDN",
  "Iraq": "IRQ",
  "Ireland": "IRL",
  "Isle-of-man": "IMN",
  "Israel": "ISR",
  "Italy": "ITA",
  "Ivory-coast": "UNKNOWN",
  "Jamaica": "JAM",
  "Japan": "JPN",
  "Jordan": "JOR",
  "Kazakhstan": "KAZ",
  "Kenya": "KEN",
  "Kuwait": "KWT",
  "Kyrgyzstan": "KGZ",
  "Laos": "UNKNOWN",
  "Latvia": "LVA",
  "Lebanon": "LBN",
  "Liechtenstein": "LIE",
  "Lithuania": "LTU",
  "Luxembourg": "LUX",
  "Macau": "UNKNOWN",
  "Macedonia": "MKD",
  "Madagascar": "MDG",
  "Malaysia": "MYS",
  "Maldives": "MDV",
  "Mali": "MLI",
  "Malta": "MLT",
  "Mauritius": "MUS",
  "Mexico": "MEX",
  "Moldova": "MDA",
  "Mongolia": "MNG",
  "Montenegro": "MNE",
  "Morocco": "MAR",
  "Mozambique": "MOZ",
  "Namibia": "NAM",
  "Netherlands": "NLD",
  "New-zealand": "NZL",
  "Nicaragua": "NIC",
  "Niger": "NER",
  "Nigeria": "NGA",
  "Norway": "NOR",
  "Oman": "OMN",
  "Pakistan": "PAK",
  "Panama": "PAN",
  "Papua-new-guinea": "PNG",
  "Paraguay": "PRY",
  "Peru": "PER",
  "Philippines": "PHL",
  "Poland": "POL",
  "Portugal": "PRT",
  "Puerto-rico": "PRI",
  "Qatar": "QAT",
  "Republic-of-the-congo": "COG",
  "Romania": "ROU",
  "Russia": "RUS",
  "Rwanda": "RWA",
  "San-marino": "SMR",
  "Saudi-arabia": "SAU",
  "Senegal": "SEN",
  "Serbia": "SRB",
  "Singapore": "SGP",
  "Slovakia": "SVK",
  "Slovenia": "SVN",
  "Solomon-islands": "SLB",
  "South-africa": "ZAF",
  "South-korea": "KOR",
  "Spain": "ESP",
  "Sri-lanka": "LKA",
  "St-vincent-and-the-grenadines": "UNKNOWN",
  "Suriname": "SUR",
  "Swaziland": "UNKNOWN",
  "Sweden": "SWE",
  "Switzerland": "CHE",
  "Taiwan": "TWN",
  "Tajikistan": "TJK",
  "Tanzania": "TZA",
  "Thailand": "THA",
  "Togo": "TGO",
  "Trinidad-and-tobago": "TTO",
  "Tunisia": "TUN",
  "Turkey": "TUR",
  "Uganda": "UGA",
  "Ukraine": "UKR",
  "United-arab-emirates": "ARE",
  "United-kingdom": "GBR",
  "United-states": "USA",
  "Uruguay": "URY",
  "Uzbekistan": "UZB",
  "Venezuela": "VEN",
  "Vietnam": "VNM",
  "Zambia": "ZMB"
}

    return country_code[name]