import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
from pymongo import MongoClient, UpdateOne

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = "countryeconomy_data"

base_url = "https://countryeconomy.com"
main_url = f"{base_url}/ratings"

def get_country_links():
    res = requests.get(main_url)
    soup = BeautifulSoup(res.text, "html.parser")

    country_links = []
    for cell in soup.select("table tr td a"):
        href = cell.get("href", "")
        if href.startswith("/ratings/") and len(cell.text.strip()) > 0:
            country_links.append((cell.text.strip(), base_url + href))
    return country_links

def get_table_data(data, agency, country):
    results = []
    for table in data.find_all("table", class_="tabledat"):
        # Extract header rows
        header_rows = table.find_all("tr")
      
        if len(header_rows) < 2:
            continue

        type_headers = header_rows[0].find_all("th")
        currency_headers = header_rows[1].find_all("th")

        column_info = []
        for i in range(len(currency_headers)):
            type_text = type_headers[i // 2].get_text(strip=True)
            currency_text = currency_headers[i].get_text(strip=True)
            type_clean = type_text.replace("Rating", "").strip()
            currency_clean = currency_text.replace("currency", "").strip()
            column_info.append((type_clean, currency_clean))

        # Parse data rows
        for row in header_rows[2:]:
            cells = row.find_all("td")
            
            if not cells:
                continue

            for i in range(0, len(cells) - 1, 2):
                try:
                    date = cells[i].get_text(strip=True)
                    rating_text = cells[i + 1].get_text(strip=True)

                    if not date or not rating_text:
                        continue

                    match = re.match(r"([A-Za-z0-9+.-]+)(?: \((.*?)\))?", rating_text)
                    if not match:
                        continue

                    rating = match.group(1)
                    outlook = match.group(2) or ""
                    type_, currency = column_info[i // 2]

                    results.append({
                        "Country": country,
                        "Agency": agency,  # Modify here if agency name is available
                        "Date": date,
                        "Rating": rating,
                        "Outlook": outlook,
                        "Type": type_,
                        "Currency": currency,
                    })
                except Exception:
                    continue
    return results

def parse_rating_page(country, url):
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
    except Exception as e:
        print(f"[ERROR] Failed to get page for {country}: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    results = []
  
    ul = soup.find("ul", id="myTab")
    buttons = ul.find_all('button')

    for btn in buttons:
        target = btn.get('data-bs-target')
        text = btn.get_text(strip=True)
        div = soup.find("div",id=target[1:])
        results.append(get_table_data(div,text,country))
 
 
    return results


def get_country_economy_data():
    
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    # Ensure a unique index to prevent duplicates
    collection.create_index(
        [("Country", 1), ("Agency", 1), ("Date", 1)],
        unique=True
    )
   
    data = get_country_links()

    for country, url in data:
        try:
            country = country[:-4].rstrip()
            country_data = parse_rating_page(country, url)
            flat_data = [item for sublist in country_data for item in sublist if item]

            if flat_data:
               
                operations = [
                    UpdateOne(
                        {"Country": doc["Country"], "Agency": doc["Agency"], "Date": doc["Date"]},
                        {"$set": doc},
                        upsert=True
                    )
                    for doc in flat_data
                ]

                if operations:
                    result = collection.bulk_write(operations, ordered=False)
                    
        except Exception as e:
            print(f"Error {country}: {e}")

