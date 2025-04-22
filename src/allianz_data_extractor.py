from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pdfplumber
import pandas as pd
import os
import time

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

def extract_pdf_to_csv(pdf_path, output_csv):
    all_tables = []
    #TODO: Clean the data before saving to csv file
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            tables = page.extract_tables()
            for table in tables:
                if table:
                    df = pd.DataFrame(table[1:], columns=table[0])
                    all_tables.append(df)
                    

    if all_tables:
        df = pd.concat(all_tables, ignore_index=True)
        os.makedirs(os.path.dirname(output_csv), exist_ok=True)
        df.to_csv(output_csv, index=False)
        


def get_allianz_last_available_info():
    print("getting info...")
    #TODO: Implement logic 
    #We need to get the last available file info from allianz web site 
    #This needs to return filename and url

def get_allianz_data():

    d = {}
    d["2025Q1"] = ["Country_Risk_Ratings_March_2025_Q1_final.pdf","https://www.allianz.com/content/dam/onemarketing/azcom/Allianz_com/economic-research/country-risk/updateq1-2025/Country_Risk_Ratings_March_2025_Q1_final.pdf"]
    d["2024Q1"] = ["Q12024countryriskratings-EXT.pdf","https://www.allianz.com/content/dam/onemarketing/azcom/Allianz_com/economic-research/country-risk/updateq12024/Q12024countryriskratings-EXT.pdf"]
    d["2024Q2"] = ["Q22024countryriskratings-EXT.pdf","https://www.allianz.com/content/dam/onemarketing/azcom/Allianz_com/economic-research/country-risk/updateq22024/Q22024countryriskratings-EXT.pdf"]
    #TODO: get_allianz_last_available_info function and add result to the dictionary 
    
    for i in d:
        try:
            pdf_url = d[i][1]
            pdf_path = os.path.join("data/raw", d[i][0])
            output_csv = os.path.join("data/processed", f"allianz_{i}.csv")
            #TODO:check if the pdf file exists in raw folder, if it does we do not need call the below lines
            download_pdf_with_selenium(pdf_url, download_folder="data/raw")
            extract_pdf_to_csv(pdf_path, output_csv)
            time.sleep(2)
        except:
            print('An error occured')
            continue