import requests
from bs4 import BeautifulSoup

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
    #e.g. https://countryeconomy.com/ratings/usa
    print(f'getting {country_name} ratings')
    
def get_countryeconomy_data():
    print('getting countryeconomy data...')