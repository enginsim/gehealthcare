from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
import pdfplumber
import pandas as pd
import os
import time
from pymongo import MongoClient
import gspread
from oauth2client.service_account import ServiceAccountCredentials



MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = "allianz_data"

SERVICE_ACCOUNT_FILE = 'tokyo-scholar-464119-b4-6a8fa808f85e.json' 
SPREADSHEET_NAME = 'allianz_data' 


def get_current_quarter(date=None):
    if date is None:
        date = datetime.today()
    
    current_quarter = (date.month - 1) // 3 + 1
    year = date.year

    return f"{year}Q{current_quarter}"

def download_pdf_with_selenium(pdf_url, download_folder="data/raw"):
    try:
        os.makedirs(download_folder, exist_ok=True)

        options = Options()
        options.add_experimental_option("prefs", {
            "download.default_directory": os.path.abspath(download_folder),
            "download.prompt_for_download": False,
            "plugins.always_open_pdf_externally": True
        })
        #options.add_argument("--headless") # allianz reject the request when this option given
        options.add_argument("--disable-gpu")
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.get(pdf_url)
        time.sleep(10) #we need this to give time to browser to download file

        driver.quit()
    except Exception as e:
        print(f"Error: {e}")
        return

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
                            doc["alpha3"] = get_country_code(country)
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
    url = "https://www.allianz.com/en/economic_research/country-and-sector-risk/country-risk.html"
    driver = None
    try:
        options = Options()
        options.add_argument("--disable-gpu")
        options.add_argument("--headless") 

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.get(url)

        wait = WebDriverWait(driver, 20)
        target_box = wait.until(EC.presence_of_element_located(
            (By.XPATH, "//h3[contains(., 'Country Risk Rating')]/ancestor::div[contains(@class, 'c-teaser')]")
        ))
        pdf_link_element = target_box.find_element(By.XPATH, ".//a[contains(@href, '.pdf')]")
        return pdf_link_element.get_attribute("href")

    except TimeoutException:
        print("Timeout: Could not find the Country Risk Rating link.")
        return None
    except WebDriverException as e:
        print(f"WebDriver error: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None
    finally:
        if driver:
            driver.quit()

   
def get_allianz_data():

    current_quarter = get_current_quarter()
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
   
    d["2025Q2"] = ['Country_Risk_Ratings_June_2025_Q2.pdf', 'https://www.allianz.com/content/dam/onemarketing/azcom/Allianz_com/economic-research/country-risk/updateq2-2025/Country_Risk_Ratings_June_2025_Q2.pdf']
    if current_quarter not in d:
        pdf_url = get_allianz_last_available_info()
        if pdf_url:
            pdf_name = pdf_url.split("/")[-1] 
            d[current_quarter] = [pdf_name, pdf_url]

    
    for year_quarter, (pdf_name, pdf_url) in d.items():
        try:
            pdf_path = os.path.join("data/raw", pdf_name)
            if not os.path.exists(pdf_path):
                download_pdf_with_selenium(pdf_url, download_folder="data/raw")
            extract_pdf_to_mongodb(pdf_path, MONGO_URI, MONGO_DB, year_quarter=year_quarter)

            time.sleep(2)
        except Exception as e:
            print(f"Error {year_quarter}: {e}")

def write_to_drive():
    try:
        df = get_data_from_db()
        df["sort_key"] = df["year_quarter"].apply(quarter_to_sort_key)
            
        max_quarters = df.groupby("country")["sort_key"].transform("max")
        df["most_recent"] = (df["sort_key"] == max_quarters).astype(int)
        df.drop(columns=["sort_key"], inplace=True)
        
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
        client_gs = gspread.authorize(creds)
        try:
            sheet = client_gs.open(SPREADSHEET_NAME)
        except gspread.SpreadsheetNotFound:
            sheet = client_gs.create(SPREADSHEET_NAME)

        worksheet = sheet.get_worksheet(0)
        worksheet.clear()
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())

    except Exception as e:
        print(f"Error: {e}")

def get_data_from_db():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    data = list(collection.find())
    df = pd.DataFrame(data)

    if "_id" in df.columns:
        df = df.drop(columns=["_id"])
    return df

def quarter_to_sort_key(yq):
        year, quarter = yq.split("Q")
        return int(year) * 10 + int(quarter)  

def sync_allianz_data():
    get_allianz_data()
    write_to_drive()

def get_country_code(name):
    country_code = {
  "Afghanistan": "AFG",
  "Albania": "ALB",
  "Algeria": "DZA",
  "American Samoa": "ASM",
  "Andorra": "AND",
  "Angola": "AGO",
  "Anguilla": "AIA",
  "Antarctica": "ATA",
  "Antigua & Barbuda": "ATG",
  "Argentina": "ARG",
  "Armenia": "ARM",
  "Aruba": "ABW",
  "Australia": "AUS",
  "Austria": "AUT",
  "Azerbaijan": "AZE",
  "BES Islands (Bonaire, St Eustatius, Saba)": "BES",
  "Bahamas": "BHS",
  "Bahrain": "BHR",
  "Bangladesh": "BGD",
  "Barbados": "BRB",
  "Belarus": "BLR",
  "Belgium": "BEL",
  "Belize": "BLZ",
  "Benin": "BEN",
  "Bermuda": "BMU",
  "Bhutan": "BTN",
  "Bolivia": "BOL",
  "Bosnia and Herzegovina": "BIH",
  "Botswana": "BWA",
  "Bouvet Island": "BVT",
  "Brazil": "BRA",
  "British Indian Ocean Territory": "IOT",
  "British Virgin Islands": "VGB",
  "Brunei": "BRN",
  "Bulgaria": "BGR",
  "Burkina Faso": "BFA",
  "Burundi": "BDI",
  "Cambodia": "KHM",
  "Cameroon": "CMR",
  "Canada": "CAN",
  "Cape Verde Islands": "CPV",
  "Cayman Islands": "CYM",
  "Central African Republic": "CAF",
  "Chad": "TCD",
  "Chile": "CHL",
  "China": "CHN",
  "Christmas Island": "CXR",
  "Cocos (Keeling) Islands": "CCK",
  "Colombia": "COL",
  "Comoros": "COM",
  "Congo (Democratic Rep Of)": "COD",
  "Congo (People's Rep Of)": "COG",
  "Cook Islands": "COK",
  "Costa Rica": "CRI",
  "Croatia": "HRV",
  "Cuba": "CUB",
  "Curacao": "NLD",
  "Cyprus": "CYP",
  "Czech Republic": "CZE",
  "Côte d'Ivoire": "CIV",
  "Denmark": "DNK",
  "Djibouti": "DJI",
  "Dominica": "DMA",
  "Dominican Republic": "DOM",
  "Ecuador": "ECU",
  "Egypt": "EGY",
  "El Salvador": "SLV",
  "Equatorial Guinea": "GNQ",
  "Eritrea": "ERI",
  "Estonia": "EST",
  "Eswatini": "SWZ",
  "Ethiopia": "ETH",
  "Falkland Islands": "FLK",
  "Faroe Islands": "FRO",
  "Fiji": "FJI",
  "Finland": "FIN",
  "France": "FRA",
  "French Guiana": "GUF",
  "French Polynesia": "PYF",
  "French Southern Territory": "ATF",
  "Gabon": "GAB",
  "Gambia": "GMB",
  "Georgia": "GEO",
  "Germany": "DEU",
  "Ghana": "GHA",
  "Gibraltar": "GIB",
  "Greece": "GRC",
  "Greenland": "GRL",
  "Grenada": "GRD",
  "Guadeloupe": "FRA",
  "Guam": "GUM",
  "Guatemala": "GTM",
  "Guinea (Rep Of)": "GIN",
  "Guinea Bissau": "GNB",
  "Guyana": "GUY",
  "Haiti": "HTI",
  "Heard and McDonald Islands": "HMD",
  "Honduras": "HND",
  "Hong Kong": "HKG",
  "Hungary": "HUN",
  "Iceland": "ISL",
  "India": "IND",
  "Indonesia": "IDN",
  "Iran": "IRN",
  "Iraq": "IRQ",
  "Ireland": "IRL",
  "Israel": "ISR",
  "Italy": "ITA",
  "Jamaica": "JAM",
  "Japan": "JPN",
  "Jordan": "JOR",
  "Kazakhstan": "KAZ",
  "Kenya": "KEN",
  "Kiribati": "KIR",
  "Kuwait": "KWT",
  "Kyrgyzstan": "KGZ",
  "Laos": "LAO",
  "Latvia": "LVA",
  "Lebanon": "LBN",
  "Lesotho": "LSO",
  "Liberia": "LBR",
  "Libya": "LBY",
  "Liechtenstein": "LIE",
  "Lithuania": "LTU",
  "Luxembourg": "LUX",
  "Macao": "MAC",
  "Madagascar": "MDG",
  "Malawi": "MWI",
  "Malaysia": "MYS",
  "Maldives": "MDV",
  "Mali": "MLI",
  "Malta": "MLT",
  "Marshall Islands": "MHL",
  "Martinique": "MTQ",
  "Mauritania": "MRT",
  "Mauritius": "MUS",
  "Mayotte": "FRA",
  "Mexico": "MEX",
  "Micronesia": "FSM",
  "Moldova": "MDA",
  "Monaco": "MCO",
  "Mongolia": "MNG",
  "Montenegro": "MNE",
  "Montserrat": "MSR",
  "Morocco": "MAR",
  "Mozambique": "MOZ",
  "Myanmar (Burma)": "MMR",
  "Namibia": "NAM",
  "Nauru": "NRU",
  "Nepal": "NPL",
  "Netherlands": "NLD",
  "New Caledonia": "NCL",
  "New Zealand": "NZL",
  "Nicaragua": "NIC",
  "Niger": "NGA",
  "Nigeria": "NGA",
  "Niue": "NIU",
  "Norfolk Island": "NFK",
  "North Korea": "PRK",
  "North Macedonia": "MKD",
  "Northern Mariana Islands": "MNP",
  "Norway": "NOR",
  "Oman": "OMN",
  "Pakistan": "PAK",
  "Palau": "PLW",
  "Panama": "PAN",
  "Papua New Guinea": "PNG",
  "Paraguay": "PRY",
  "Peru": "PER",
  "Philippines": "PHL",
  "Pitcairn Islands": "PCN",
  "Poland": "POL",
  "Portugal": "PRT",
  "Puerto Rico": "PRI",
  "Qatar": "QAT",
  "Reunion": "REU",
  "Romania": "ROU",
  "Russia": "RUS",
  "Rwanda": "RWA",
  "Samoa": "WSM",
  "San Marino": "SMR",
  "Sao Tome & Principe": "STP",
  "Saudi Arabia": "SAU",
  "Senegal": "SEN",
  "Serbia": "SRB",
  "Seychelles": "SYC",
  "Sierra Leone": "SLE",
  "Singapore": "SGP",
  "Slovakia": "SVK",
  "Slovenia": "SVN",
  "Solomon Islands": "SLB",
  "Somalia": "SOM",
  "South Africa": "ZAF",
  "South Georgia/Sandwich Islands": "SGS",
  "South Korea": "KOR",
  "South Sudan": "SSD",
  "Spain": "ESP",
  "Sri Lanka": "LKA",
  "St Helena": "SHN",
  "St. Kitts & Nevis": "KNA",
  "St. Lucia": "LCA",
  "St. Maarten": "SXM",
  "St. Pierre Et Miquelon": "SPM",
  "St. Vincent & The Grenadines": "VCT",
  "Sudan": "SDN",
  "Suriname": "SUR",
  "Svalbard & Jan Mayen": "SJM",
  "Sweden": "SWE",
  "Switzerland": "CHE",
  "Syria": "SYR",
  "Taiwan": "TWN",
  "Tajikistan": "TJK",
  "Tanzania": "TZA",
  "Thailand": "THA",
  "Timor Leste": "TLS",
  "Togo": "TGO",
  "Tokelau": "TKL",
  "Tonga": "TON",
  "Trinidad & Tobago": "TTO",
  "Tunisia": "TUN",
  "Turkey": "TUR",
  "Turkmenistan": "TKM",
  "Turks & Caicos": "TCA",
  "Tuvalu": "TUV",
  "Türkiye": "TUR",
  "US Minor Outlying Islands": "UMI",
  "US Virgin Islands": "VIR",
  "Uganda": "UGA",
  "Ukraine": "UKR",
  "United Arab Emirates": "ARE",
  "United Kingdom": "GBR",
  "United States": "USA",
  "Uruguay": "URY",
  "Uzbekistan": "UZB",
  "Vanuatu": "VUT",
  "Vatican City": "VAT",
  "Venezuela": "VEN",
  "Vietnam": "VNM",
  "Wallis & Futuna": "WLF",
  "Yemen": "YEM",
  "Zambia": "ZMB",
  "Zimbabwe": "ZWE"
}
    return country_code[name]