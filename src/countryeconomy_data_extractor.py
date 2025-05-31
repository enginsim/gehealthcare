import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
from datetime import datetime
import re

class CountryEconomyDetailedScraper:
    """
    Scrapes detailed rating history for all countries from countryeconomy.com
    Navigates through all country pages from Albania to Zambia
    """
    
    def __init__(self, output_dir=None):
        """
        Initialize the scraper
        
        Parameters:
        output_dir (str): Directory where data will be saved
        """
        if output_dir is None:
            # Get the current script directory (src folder)
            current_dir = os.path.dirname(os.path.abspath(__file__))
            # Go up one level from src to project root, then to data/processed
            project_root = os.path.dirname(current_dir)
            self.output_dir = os.path.join(project_root, "data", "processed")
        else:
            self.output_dir = output_dir
            
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        print(f"📁 Output directory: {os.path.abspath(self.output_dir)}")
        
        # Initialize session for consistent cookies
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
    def get_countries(self):
        """
        Get list of countries from countryeconomy.com/countries page
        
        Returns:
        list: List of tuples (country_name, country_url)
        """
        url = 'https://countryeconomy.com/countries'
       
        try:
            response = self.session.get(url)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching countries page: {e}")
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
        
        print(f"✅ Found {len(countries)} countries from countries list")
        return countries
        
    def get_country_rating_history(self, country_url, country_name=None):
        """
        Scrape rating history table from a country's rating page
        
        Parameters:
        country_url (str): URL of the country's rating page
        country_name (str): Known country name (from countries list)
        
        Returns:
        list: List of rating records for the country
        """
        try:
            print(f"  📊 Fetching rating history from: {country_url}")
            response = self.session.get(country_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Use provided country name or extract from page
            if not country_name:
                country_name = self.extract_country_name(soup, country_url)
            
            # Find the rating table
            rating_data = []
            
            # Look for the main rating table
            table = soup.find('table')
            if not table:
                print(f"    ⚠️  No rating table found for {country_name}")
                return []
            
            # Extract data rows (skip header row)
            rows = table.find_all('tr')[1:]
            
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:  # Ensure we have at least date and rating
                    
                    # Process each rating type from the table structure
                    records = []
                    
                    # Long term Foreign currency rating
                    if len(cells) >= 2 and cells[0].get_text(strip=True) and cells[1].get_text(strip=True):
                        date_text = cells[0].get_text(strip=True)
                        rating_text = cells[1].get_text(strip=True)
                        
                        if date_text and rating_text and date_text != 'Date':
                            # Clean rating text to extract just the rating (remove outlook)
                            rating_clean = self.clean_rating_text(rating_text)
                            if rating_clean:
                                records.append({
                                    'Reference area': country_name,
                                    'Rating agency': 'Moody\'s',
                                    'Rating': rating_clean,
                                    'Rating date': self.clean_date_text(date_text),
                                    'Term type': 'Long term'
                                })
                    
                    # Long term Local currency rating
                    if len(cells) >= 4 and cells[2].get_text(strip=True) and cells[3].get_text(strip=True):
                        date_text = cells[2].get_text(strip=True)
                        rating_text = cells[3].get_text(strip=True)
                        
                        if date_text and rating_text and date_text != 'Date':
                            rating_clean = self.clean_rating_text(rating_text)
                            if rating_clean:
                                records.append({
                                    'Reference area': country_name,
                                    'Rating agency': 'Moody\'s',
                                    'Rating': rating_clean,
                                    'Rating date': self.clean_date_text(date_text),
                                    'Term type': 'Long term'
                                })
                    
                    # Short term Foreign currency rating
                    if len(cells) >= 6 and cells[4].get_text(strip=True) and cells[5].get_text(strip=True):
                        date_text = cells[4].get_text(strip=True)
                        rating_text = cells[5].get_text(strip=True)
                        
                        if date_text and rating_text and date_text != 'Date':
                            rating_clean = self.clean_rating_text(rating_text)
                            if rating_clean:
                                records.append({
                                    'Reference area': country_name,
                                    'Rating agency': 'Moody\'s',
                                    'Rating': rating_clean,
                                    'Rating date': self.clean_date_text(date_text),
                                    'Term type': 'Short term'
                                })
                    
                    # Short term Local currency rating
                    if len(cells) >= 8 and cells[6].get_text(strip=True) and cells[7].get_text(strip=True):
                        date_text = cells[6].get_text(strip=True)
                        rating_text = cells[7].get_text(strip=True)
                        
                        if date_text and rating_text and date_text != 'Date':
                            rating_clean = self.clean_rating_text(rating_text)
                            if rating_clean:
                                records.append({
                                    'Reference area': country_name,
                                    'Rating agency': 'Moody\'s',
                                    'Rating': rating_clean,
                                    'Rating date': self.clean_date_text(date_text),
                                    'Term type': 'Short term'
                                })
                    
                    # Add all valid records from this row
                    rating_data.extend(records)
            
            print(f"    ✅ Extracted {len(rating_data)} rating records for {country_name}")
            return rating_data
            
        except requests.RequestException as e:
            print(f"    ❌ Error fetching {country_url}: {e}")
            return []
        except Exception as e:
            print(f"    ⚠️  Error parsing {country_url}: {e}")
            return []
    
    def clean_rating_text(self, rating_text):
        """
        Clean rating text to extract just the rating (remove outlook info)
        
        Parameters:
        rating_text (str): Raw rating text like "Ba3 (Stable)" or "B1 (Positive)"
        
        Returns:
        str: Clean rating like "Ba3" or "B1"
        """
        if not rating_text:
            return ""
        
        # Remove outlook information in parentheses
        rating_clean = re.sub(r'\s*\([^)]*\)', '', rating_text)
        
        # Remove extra whitespace
        rating_clean = rating_clean.strip()
        
        # Only return if it looks like a valid rating
        if rating_clean and len(rating_clean) >= 1:
            return rating_clean
        
        return ""
    
    def clean_date_text(self, date_text):
        """
        Clean and standardize date text
        
        Parameters:
        date_text (str): Raw date text
        
        Returns:
        str: Cleaned date text
        """
        if not date_text:
            return ""
        
        # Remove extra whitespace and return
        return date_text.strip()
    
    def extract_country_name(self, soup, url):
        """
        Extract country name from page title or URL with better accuracy
        """
        # First try to get from page title
        title = soup.find('title')
        if title:
            title_text = title.get_text()
            # Extract country name from title like "Rating Moody's Albania"
            if 'Rating' in title_text and 'Moody\'s' in title_text:
                parts = title_text.split()
                if len(parts) >= 3:
                    country_name = parts[-1]  # Last word should be country name
                    return country_name
        
        # Try to extract from URL
        if '/ratings/' in url:
            country_part = url.split('/ratings/')[-1].split('/')[0].split('?')[0]
            country_name = country_part.replace('-', ' ').title()
            return country_name
        
        return "Unknown"
    
    def create_ratings_url_from_country_url(self, country_url):
        """
        Convert country URL to ratings URL
        e.g., https://countryeconomy.com/countries/albania -> https://countryeconomy.com/ratings/albania
        """
        if '/countries/' in country_url:
            country_slug = country_url.split('/countries/')[-1].split('/')[0].split('?')[0]
            return f"https://countryeconomy.com/ratings/{country_slug}"
        return country_url
    
    def navigate_to_next_country(self, current_url):
        """
        Find and return the URL for the next country page
        
        Parameters:
        current_url (str): Current country page URL
        
        Returns:
        str: Next country page URL or None if not found
        """
        try:
            response = self.session.get(current_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for the next button with class "book-page-next"
            next_button = soup.find('a', class_='book-page-next')
            
            if next_button and next_button.get('href'):
                next_url = next_button.get('href')
                
                # Make sure it's a full URL
                if next_url.startswith('/'):
                    next_url = 'https://countryeconomy.com' + next_url
                
                return next_url
            
            return None
            
        except Exception as e:
            print(f"    ⚠️  Error finding next page from {current_url}: {e}")
            return None
    
    def scrape_all_countries_from_list(self, end_country="zambia"):
        """
        Scrape rating data for all countries using the countries list
        
        Parameters:
        end_country (str): Stop when reaching this country (default: Zambia)
        
        Returns:
        pandas.DataFrame: All rating data combined
        """
        print("=== Country Economy Detailed Rating History Scraper ===")
        print(f"🌍 Getting countries list from /countries page")
        print(f"🛑 Stopping at: {end_country.title()}")
        print(f"📂 Output: ../data/processed/")
        print()
        
        # Initialize data storage
        all_ratings_data = []
        
        # Get countries list
        countries = self.get_countries()
        if not countries:
            print("❌ Could not get countries list")
            return pd.DataFrame()
        
        # Create mapping for better country name extraction (initialize here)
        country_mapping = {}
        for country_name, country_url in countries:
            country_mapping[country_name] = country_url
        
        # Find start and end indices
        start_idx = 0  # Start from Albania (first in alphabetical order)
        end_idx = len(countries)
        
        # Find the end country index
        for i, (country_name, _) in enumerate(countries):
            if country_name.lower().startswith(end_country.lower()):
                end_idx = i + 1  # Include the end country
                break
        
        print(f"📋 Processing countries {start_idx + 1} to {end_idx} out of {len(countries)} total")
        print()
        
        # Process countries in the range
        for i in range(start_idx, end_idx):
            try:
                country_name, country_url = countries[i]
                
                print(f"🌍 [{i + 1}/{end_idx}] Processing: {country_name}")
                
                # Convert country URL to ratings URL
                ratings_url = self.create_ratings_url_from_country_url(country_url)
                
                # Scrape current country's rating data
                country_data = self.get_country_rating_history(ratings_url, country_name)
                all_ratings_data.extend(country_data)
                
                # Check if we've reached the end country
                if country_name.lower().startswith(end_country.lower()):
                    print(f"🛑 Reached {end_country.title()}! Stopping...")
                    break
                
                # Be respectful to the server
                time.sleep(0.5)
                
            except KeyboardInterrupt:
                print("\n⏹️  Scraping interrupted by user")
                break
            except Exception as e:
                print(f"    ❌ Unexpected error processing {country_name}: {e}")
                # Continue with next country
                time.sleep(1)
                continue
        
        print(f"\n✅ Scraping completed!")
        print(f"📊 Countries processed: {i + 1 - start_idx}")
        print(f"📈 Total rating records collected: {len(all_ratings_data)}")
        
        # Convert to DataFrame
        if all_ratings_data:
            df = pd.DataFrame(all_ratings_data)
            return df
        else:
            return pd.DataFrame()
    
    def extract_country_name_from_url(self, url):
        """Helper method to extract country name from URL"""
        if '/ratings/' in url:
            country_part = url.split('/ratings/')[-1].split('/')[0].split('?')[0]
            return country_part.replace('-', ' ').title()
        return "Unknown"
    
    def save_data(self, df, filename="detailed_country_ratings_history"):
        """
        Save the collected data to CSV with standardized columns
        
        Parameters:
        df (pandas.DataFrame): Data to save
        filename (str): Output filename
        
        Returns:
        bool: Success status
        """
        if df is not None and not df.empty:
            # Ensure we have the exact 5 columns requested
            required_columns = ['Reference area', 'Rating agency', 'Rating', 'Rating date', 'Term type']
            
            # Filter to only required columns
            df_filtered = df[required_columns].copy()
            
            # Remove rows with empty ratings or dates
            df_filtered = df_filtered.dropna(subset=['Rating', 'Rating date'])
            df_filtered = df_filtered[df_filtered['Rating'] != '']
            df_filtered = df_filtered[df_filtered['Rating date'] != '']
            
            # Sort by country and date
            df_filtered = df_filtered.sort_values(['Reference area', 'Rating date'])
            
            filepath = os.path.join(self.output_dir, f"{filename}.csv")
            df_filtered.to_csv(filepath, index=False)
            
            print(f"\n💾 Data saved to: {filepath}")
            print(f"📊 Data shape: {df_filtered.shape}")
            print(f"📋 Columns: {list(df_filtered.columns)}")
            
            # Show sample data
            print(f"\n📋 Sample data:")
            print(df_filtered.head(10))
            
            # Show statistics
            if not df_filtered.empty:
                print(f"\n📈 Data Statistics:")
                print(f"  • Total rating records: {len(df_filtered)}")
                print(f"  • Countries covered: {df_filtered['Reference area'].nunique()}")
                print(f"  • Rating agencies: {', '.join(df_filtered['Rating agency'].unique())}")
                print(f"  • Long term ratings: {len(df_filtered[df_filtered['Term type'] == 'Long term'])}")
                print(f"  • Short term ratings: {len(df_filtered[df_filtered['Term type'] == 'Short term'])}")
                
                # Show date range
                dates = pd.to_datetime(df_filtered['Rating date'], errors='coerce')
                valid_dates = dates.dropna()
                if not valid_dates.empty:
                    print(f"  • Date range: {valid_dates.min().strftime('%Y-%m-%d')} to {valid_dates.max().strftime('%Y-%m-%d')}")
                
                # Show countries covered
                countries = sorted(df_filtered['Reference area'].unique())
                print(f"\n🌍 Countries covered ({len(countries)}):")
                print(", ".join(countries))
            
            return True
        else:
            print(f"❌ No data to save")
            return False

# Main function for compatibility
def get_countryeconomy_detailed_data():
    """
    Main function to scrape detailed rating history for all countries
    Compatible with existing main.py structure
    """
    scraper = CountryEconomyDetailedScraper()
    
    # Scrape all countries from Albania to Zambia using countries list
    ratings_df = scraper.scrape_all_countries_from_list()
    
    # Save the data
    success = scraper.save_data(ratings_df, "detailed_country_ratings_history")
    
    if success:
        print(f"\n🎉 SUCCESS: Detailed rating history data extracted!")
        print(f"📁 File: ../data/processed/detailed_country_ratings_history.csv")
        print(f"🔧 Use relative path: ../data/processed/detailed_country_ratings_history.csv")
    else:
        print(f"\n❌ FAILED: Could not extract detailed rating data")
    
    return success

# Backward compatibility
def get_countryeconomy_data():
    """Legacy function name for compatibility"""
    return get_countryeconomy_detailed_data()

if __name__ == "__main__":
    get_countryeconomy_detailed_data()