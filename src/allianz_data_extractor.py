from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
import pdfplumber
import pandas as pd
import os
import time
from pymongo import MongoClient




MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = "allianz_data"

def get_previous_quarter(date=None):
    if date is None:
        date = datetime.today()
    
    current_quarter = (date.month - 1) // 3 + 1
    if current_quarter == 1:
        prev_quarter = 4
        year = date.year - 1
    else:
        prev_quarter = current_quarter - 1
        year = date.year

    return f"{year}Q{prev_quarter}"

def download_pdf_with_selenium(pdf_url, download_folder="data/raw"):
    os.makedirs(download_folder, exist_ok=True)

    options = Options()
    options.add_experimental_option("prefs", {
        "download.default_directory": os.path.abspath(download_folder),
        "download.prompt_for_download": False,
        "plugins.always_open_pdf_externally": True
    })
    #options.add_argument("--headless") # allianz reject the request when this option given
    options.add_argument("--disable-gpu")

    driver = webdriver.Chrome(options=options)
    driver.get(pdf_url)
    time.sleep(10) #we need this to give time to browser to download file

    driver.quit()


def extract_pdf_to_mongodb(pdf_path, mongo_uri, db_name, collection_name="allianz_data", year_quarter="2025Q2"):
    data = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    lines = text.split('\n')
                    for line in lines:
                        line_clean = line.strip().lower()
                        if ("country" in line_clean and "rating" in line_clean) or any(word in line_clean for word in ["short-term", "medium-term", "grade", "risk level", "allianz", "review"]):
                            continue 
                        parts = line.split()
                        if len(parts) >= 4:
                            country = ' '.join(parts[:-3])
                            mid_rating = parts[-3]
                            short_rating = parts[-2]
                            level = parts[-1].strip("()")
                            doc = {
                                "country": country,
                                "medium_term_rating": mid_rating,
                                "short_term_rating": short_rating,
                                "risk_level": level,
                                "year_quarter": year_quarter
                            }
                            data.append(doc)
    except Exception as e:
        print(f"Error: {e}")
        return

    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        if data:
            # Avoid duplicates: use country + year_quarter as a unique identifier
            for doc in data:
                collection.update_one(
                    {"country": doc["country"], "year_quarter": doc["year_quarter"]},
                    {"$set": doc},
                    upsert=True
                )
        print("Insert completed")
    except Exception as e:
        print(f"MongoDB Error: {e}")
    finally:
        client.close()

def get_allianz_last_available_info():
    options = Options()
    options.headless = True  # Set to False to debug with UI
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    base_url = "https://www.allianz.com"
    main_url = f"{base_url}/en/economic_research/country-and-sector-risk/country-risk.html"
    driver.get(main_url)

    result = []

    try:
        
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href$='.pdf']"))
        )

        pdf_link_element = driver.find_element(By.CSS_SELECTOR, "a[href$='.pdf']")
        pdf_href = pdf_link_element.get_attribute("href")

        if pdf_href:
            filename = pdf_href.split("/")[-1]
            result = [filename, pdf_href]
        

    except Exception as e:
        print(e)
    finally:
        driver.quit()

    return result

def get_allianz_data():

    prev_quarter = get_previous_quarter()
    d = {}
    d["2025Q1"] = ["Country_Risk_Ratings_March_2025_Q1_final.pdf","https://www.allianz.com/content/dam/onemarketing/azcom/Allianz_com/economic-research/country-risk/updateq1-2025/Country_Risk_Ratings_March_2025_Q1_final.pdf"]
    d["2024Q1"] = ["Q12024countryriskratings-EXT.pdf","https://www.allianz.com/content/dam/onemarketing/azcom/Allianz_com/economic-research/country-risk/updateq12024/Q12024countryriskratings-EXT.pdf"]
    d["2024Q2"] = ["Q22024countryriskratings-EXT.pdf","https://www.allianz.com/content/dam/onemarketing/azcom/Allianz_com/economic-research/country-risk/updateq22024/Q22024countryriskratings-EXT.pdf"]
    d["2024Q3"] = ["Q32024countryriskratings-EXT.pdf","https://www.allianz.com/content/dam/onemarketing/azcom/Allianz_com/economic-research/country-risk/updateq22024/Q22024countryriskratings-EXT.pdf"]
    d["2024Q4"] = ["Q42024countryriskratings-EXT.pdf","https://www.allianz.com/content/dam/onemarketing/azcom/Allianz_com/economic-research/country-risk/updateq22024/Q22024countryriskratings-EXT.pdf"]
    d["2023Q1"] = ["Q12023countryriskratings-EXT.pdf","https://www.allianz.com/content/dam/onemarketing/azcom/Allianz_com/economic-research/country-risk/updateq22024/Q22024countryriskratings-EXT.pdf"]
    d["2023Q2"] = ["Q22023countryriskratings-EXT.pdf","https://www.allianz.com/content/dam/onemarketing/azcom/Allianz_com/economic-research/country-risk/updateq22024/Q22024countryriskratings-EXT.pdf"]
    d["2023Q3"] = ["Q32023countryriskratings-EXT.pdf","https://www.allianz.com/content/dam/onemarketing/azcom/Allianz_com/economic-research/country-risk/updateq22024/Q22024countryriskratings-EXT.pdf"]
    d["2023Q4"] = ["Q42023countryriskratings-EXT.pdf","https://www.allianz.com/content/dam/onemarketing/azcom/Allianz_com/economic-research/country-risk/updateq22024/Q22024countryriskratings-EXT.pdf"]
    d["2022Q4"] = ["Q42022countryriskratings-EXT.pdf","https://www.allianz.com/content/dam/onemarketing/azcom/Allianz_com/economic-research/country-risk/updateq22024/Q22024countryriskratings-EXT.pdf"]
    d["2024Q2"] = ["Q22022countryriskratings-EXT.pdf","https://www.allianz.com/content/dam/onemarketing/azcom/Allianz_com/economic-research/country-risk/updateq22024/Q22024countryriskratings-EXT.pdf"]
    
    if prev_quarter not in d:
        d[prev_quarter] = get_allianz_last_available_info()
    
    for year_quarter, (pdf_name, pdf_url) in d.items():
        try:
            pdf_path = os.path.join("data/raw", pdf_name)
            if not os.path.exists(pdf_path):
                download_pdf_with_selenium(pdf_url, download_folder="data/raw")
            extract_pdf_to_mongodb(pdf_path, MONGO_URI, MONGO_DB, year_quarter=year_quarter)

            time.sleep(2)
        except Exception as e:
            print(f"Error {year_quarter}: {e}")