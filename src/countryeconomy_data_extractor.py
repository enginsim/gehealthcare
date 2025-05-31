import requests
from bs4 import BeautifulSoup
import time

def get_countries():
    url = 'https://countryeconomy.com/countries'
   
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching the page: {e}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')

    countries = []
    
    links = soup.select("a[href^='/countries/']")
    for link in links:
        country_name = link.text.strip()
        href = link.get('href')
        if href and country_name:
            full_url = 'https://countryeconomy.com' + href
            countries.append((country_name, full_url))

    return countries

def get_country_ratings(country_name):
    print(country_name)
    #e.g. https://countryeconomy.com/ratings/usa
    url = 'https://countryeconomy.com/ratings/'

    #get page and html 
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching the page: {e}")
        return []
    
    soup = BeautifulSoup(response.text, 'html.parser')

    #empty list to store countries and their rating data
    country_ratings_data = {}

    #get table rows
    rows = soup.select("tbody tr")

    #get cells from rows and links from cells
    for row in rows:
        cells = row.find_all("td")
        link = cells[0].find("a")

        #remove unwanted characters/spaces from country name
        if link:
            clean_name = link.text.strip().replace("[+]", "").strip()

            if clean_name == country_name:
                #get cells for each rating for the chosen country
                moody_cell = cells[1].find("span", class_="padleft")
                sp_cell = cells[2].find("span", class_="padleft")
                fitch_cell = cells[3].find("span", class_="padleft")

                #clean ratings and replace missing data
                rating_moody = moody_cell.text.strip() if moody_cell else "Unavailable"
                rating_sp = sp_cell.text.strip() if sp_cell else "Unavailable"
                rating_fitch = fitch_cell.text.strip() if fitch_cell else "Unavailable"

                print(f"""The ratings for {country_name} are:
            Moody's: {rating_moody}
            S&P: {rating_sp}
            Fitch: {rating_fitch}
            """)
    
    return

    
def get_countryeconomy_data():
    countries = get_countries()

    for i, (country_name, url) in enumerate(countries):
        print(f"{i+1}/{len(countries)} - Fetching ratings for {country_name}...")
        get_country_ratings(country_name)
        time.sleep(1.5) #Sleep between requests to avoid overloading the server 
