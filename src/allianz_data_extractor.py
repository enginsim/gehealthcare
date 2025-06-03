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
import mysql.connector

from .db_config import db_config, db_name

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

def extract_pdf_to_mysql(pdf_path, db_config, db_name, table_name="allianz_data", year_quarter="2025Q2"):
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
                            data.append((country, mid_rating, short_rating, level, year_quarter))
    except Exception as e:
        print(f"Error: {e}")
        return

    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(f"USE {db_name}")

        insert_query = f"""
            INSERT IGNORE INTO {table_name}
            (country, medium_term_rating, short_term_rating, risk_level, year_quarter)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, data)
        conn.commit()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()



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
    if prev_quarter not in d:
        d[prev_quarter] = get_allianz_last_available_info()
    
    for year_quarter, (pdf_name, pdf_url) in d.items():
        try:
            pdf_path = os.path.join("data/raw", pdf_name)
            if not os.path.exists(pdf_path):
                download_pdf_with_selenium(pdf_url, download_folder="data/raw")
            extract_pdf_to_mysql(pdf_path, db_config, db_name, year_quarter=year_quarter)
            time.sleep(2)
        except Exception as e:
            print(f"Error processing {year_quarter}: {e}")