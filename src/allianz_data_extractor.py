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