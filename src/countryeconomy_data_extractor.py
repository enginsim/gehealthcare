import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import sys
import time
from datetime import datetime
import re
import mysql.connector

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from .db_config import db_config, db_name
except ImportError:
    from db_config import db_config, db_name

class CountryEconomyDetailedScraper:
    
    
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
        print(f"Output directory: {os.path.abspath(self.output_dir)}")
        
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
        
        print(f"Found {len(countries)} countries from countries list")
        return countries
        
    def get_country_rating_history(self, country_url, country_name=None):
        """
        Scrape rating history table from a country's rating page for all agencies (Moody's, S&P, Fitch)
        
        Parameters:
        country_url (str): URL of the country's rating page
        country_name (str): Known country name (from countries list)
        
        Returns:
        list: List of rating records for the country
        """
        try:
            print(f"Fetching rating history from: {country_url}")
            response = self.session.get(country_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Use provided country name or extract from page
            if not country_name:
                country_name = self.extract_country_name(soup, country_url)
            
            # Find all rating tables
            rating_data = []
            
            # Look for all tables on the page (typically 3 - one for each agency)
            tables = soup.find_all('table')
            
            if not tables:
                print(f"No rating tables found for {country_name}")
                return []
            
            # Process each table (each represents a different rating agency)
            # Stop after processing 3 tables (Moody's, S&P, Fitch)
            for table_idx, table in enumerate(tables):
                # Only process first 3 tables
                if table_idx >= 3:
                    break
                    
                # Determine rating agency based on table position or headers
                agency_name = self.determine_rating_agency(table, table_idx)
                
                if not agency_name:
                    # Skip this table if we can't determine the agency
                    continue
                
                print(f"Processing {agency_name} ratings for {country_name}")
                
                # Extract data rows (skip header row)
                rows = table.find_all('tr')[1:]
                
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:  # Ensure we have at least date and rating
                        
                        # Process ONLY FOREIGN CURRENCY ratings
                        records = []
                        
                        # Long term Foreign currency rating (columns 0-1)
                        if len(cells) >= 2 and cells[0].get_text(strip=True) and cells[1].get_text(strip=True):
                            date_text = cells[0].get_text(strip=True)
                            rating_text = cells[1].get_text(strip=True)
                            
                            if date_text and rating_text and date_text != 'Date':
                                rating_clean = self.clean_rating_text(rating_text)
                                if rating_clean:
                                    records.append({
                                        'Reference area': country_name,
                                        'Rating agency': agency_name,
                                        'Rating': rating_clean,
                                        'Rating date': self.clean_date_text(date_text),
                                        'Term type': 'Long term'
                                    })
                        
                        # Short term Foreign currency rating (columns 4-5)
                        if len(cells) >= 6 and cells[4].get_text(strip=True) and cells[5].get_text(strip=True):
                            date_text = cells[4].get_text(strip=True)
                            rating_text = cells[5].get_text(strip=True)
                            
                            if date_text and rating_text and date_text != 'Date':
                                rating_clean = self.clean_rating_text(rating_text)
                                if rating_clean:
                                    records.append({
                                        'Reference area': country_name,
                                        'Rating agency': agency_name,
                                        'Rating': rating_clean,
                                        'Rating date': self.clean_date_text(date_text),
                                        'Term type': 'Short term'
                                    })
                        
                        # Add all valid records from this row
                        rating_data.extend(records)
            
            print(f"Extracted {len(rating_data)} total rating records for {country_name}")
            return rating_data
            
        except requests.RequestException as e:
            print(f"Error fetching {country_url}: {e}")
            return []
        except Exception as e:
            print(f"Error parsing {country_url}: {e}")
            return []
    
    def determine_rating_agency(self, table, table_idx):
        """
        Determine which rating agency a table belongs to
        Returns None for unknown agencies to skip them completely
        
        Parameters:
        table: BeautifulSoup table element
        table_idx: Index of the table (0-based)
        
        Returns:
        str: Rating agency name or None to skip
        """
        # Check if table has any data rows (not just headers)
        rows = table.find_all('tr')
        if len(rows) <= 1:  # Only header or empty
            return None
            
        # Check if table has rating data by looking at cells
        first_data_row = rows[1] if len(rows) > 1 else None
        if first_data_row:
            cells = first_data_row.find_all(['td', 'th'])
            # Skip tables that don't look like rating tables
            if len(cells) < 2:
                return None
        
        # First, try to find agency name from nearby headers or elements
        # Look for h2, h3, or other headers before the table
        parent = table.parent
        if parent:
            # Look for headers within the parent element
            for header in parent.find_all(['h2', 'h3', 'h4']):
                header_text = header.get_text(strip=True).lower()
                if 'moody' in header_text:
                    return "Moody's"
                elif 's&p' in header_text or 'standard' in header_text:
                    return "S&P"
                elif 'fitch' in header_text:
                    return "Fitch"
        
        # Check the previous sibling elements
        prev_element = table.find_previous_sibling()
        checked_elements = 0
        while prev_element and checked_elements < 10:
            if prev_element.name in ['h2', 'h3', 'h4', 'p', 'div']:
                text = prev_element.get_text(strip=True).lower()
                if 'moody' in text:
                    return "Moody's"
                elif 's&p' in text or 'standard' in text:
                    return "S&P"
                elif 'fitch' in text:
                    return "Fitch"
            prev_element = prev_element.find_previous_sibling()
            checked_elements += 1
        
        # Look for agency name in table itself
        table_text = table.get_text().lower()
        if 'moody' in table_text:
            return "Moody's"
        elif 's&p' in table_text or 'standard' in table_text:
            return "S&P"
        elif 'fitch' in table_text:
            return "Fitch"
        
        # If we can't determine the agency, return None to skip this table
        # Only return known agencies (Moody's, S&P, Fitch)
        if table_idx < 3:
            # Try index-based mapping only for first 3 tables
            agency_map = {
                0: "Moody's",
                1: "S&P",
                2: "Fitch"
            }
            # Double-check that this looks like a rating table
            table_text = table.get_text().lower()
            if any(word in table_text for word in ['rating', 'date', 'outlook']):
                return agency_map.get(table_idx)
        
        # Return None for any unknown agency - this will skip the table
        return None
    
    def clean_rating_text(self, rating_text):
        """
        Clean rating text to extract just the rating (remove outlook info)
        Works for all rating agencies (Moody's, S&P, Fitch)
        
        Parameters:
        rating_text (str): Raw rating text like "Ba3 (Stable)", "BB- (Positive)", "BB+ (Negative)"
        
        Returns:
        str: Clean rating like "Ba3", "BB-", "BB+"
        """
        if not rating_text:
            return ""
        
        # Remove outlook information in parentheses
        rating_clean = re.sub(r'\s*\([^)]*\)', '', rating_text)
        
        # Remove extra whitespace
        rating_clean = rating_clean.strip()
        
        # Only return if it looks like a valid rating
        # Valid ratings include: Moody's (Aaa, Aa1, Ba3, etc.), S&P/Fitch (AAA, AA+, BB-, etc.)
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
            print(f"Error finding next page from {current_url}: {e}")
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
        print(f"Getting countries list from /countries page")
        print(f"Stopping at: {end_country.title()}")
        print(f"Output: ../data/processed/")
        print()
        
        # Initialize data storage
        all_ratings_data = []
        
        # Get countries list
        countries = self.get_countries()
        if not countries:
            print("Could not get countries list")
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
                
                print(f"[{i + 1}/{end_idx}] Processing: {country_name}")
                
                # Convert country URL to ratings URL
                ratings_url = self.create_ratings_url_from_country_url(country_url)
                
                # Scrape current country's rating data
                country_data = self.get_country_rating_history(ratings_url, country_name)
                all_ratings_data.extend(country_data)
                
                # Check if we've reached the end country
                if country_name.lower().startswith(end_country.lower()):
                    print(f"Reached {end_country.title()}! Stopping...")
                    break
                
                # Be respectful to the server
                time.sleep(0.5)
                
            except KeyboardInterrupt:
                print("\nScraping interrupted by user")
                break
            except Exception as e:
                print(f"Unexpected error processing {country_name}: {e}")
                # Continue with next country
                time.sleep(1)
                continue
        
        print(f"\nScraping completed!")
        print(f"Countries processed: {i + 1 - start_idx}")
        print(f"Total rating records collected: {len(all_ratings_data)}")
        
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
            
            print(f"\nData saved to: {filepath}")
            print(f"Data shape: {df_filtered.shape}")
            print(f"Columns: {list(df_filtered.columns)}")
            
            # Show sample data
            print(f"\nSample data:")
            print(df_filtered.head(10))
            
            # Show statistics
            if not df_filtered.empty:
                print(f"\nData Statistics:")
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
                print(f"\nCountries covered ({len(countries)}):")
                print(", ".join(countries))
            
            return True
        else:
            print(f"No data to save")
            return False
        
    def save_data_to_mysql(self, df, db_name, table_name="countryeconomy_data"):
        data = []

        try:
            if df is not None and not df.empty:
                # Ensure we have the exact 5 columns requested
                required_columns = ['Reference area', 'Rating agency', 'Rating', 'Rating date', 'Term type']
            
                # Filter to only required columns
                df_filtered = df[required_columns].copy()
            
                # Remove rows with empty ratings or dates
                df_filtered = df_filtered.dropna(subset=['Rating', 'Rating date'])
                df_filtered = df_filtered[df_filtered['Rating'] != '']
                df_filtered = df_filtered[df_filtered['Rating date'] != '']
                df_filtered = df_filtered.rename(columns={'Reference area': 'country'})
                df_filtered = df_filtered.rename(columns={'Rating agency': 'rating_agency'})
                df_filtered = df_filtered.rename(columns={'Rating date': 'rating_date'})
                df_filtered = df_filtered.rename(columns={'Term type': 'term_type'})


                # Sort by country and date
                df_filtered = df_filtered.sort_values(['country', 'rating_date'])
            
                conn = None
                cursor = None
                try:
                    conn = mysql.connector.connect(**db_config)
                    cursor = conn.cursor()
                    cursor.execute(f"USE {db_name}")

                    insert_query = f"""
                    INSERT IGNORE INTO {table_name}
                    (country, rating_agency, rating, rating_date, term_type)
                    VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor.executemany(insert_query, df_filtered.values.tolist())
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
                return True
            else:
                print(f"No data to save")
                return False
  
        except Exception as e:
            print(f"Error: {e}")
            return

   

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
    success_mysql = scraper.save_data_to_mysql(ratings_df, db_name, table_name="countryeconomy_data")
    
    if success and success_mysql:
        print(f"\nSUCCESS: Detailed rating history data extracted and saved to both CSV and MySQL!")
        print(f"CSV File: ../data/processed/detailed_country_ratings_history.csv")
        print(f"MySQL Table: {db_name}.countryeconomy_data")
    elif success:
        print(f"\nPARTIAL SUCCESS: Data saved to CSV but MySQL save failed")
        print(f"CSV File: ../data/processed/detailed_country_ratings_history.csv")
    elif success_mysql:
        print(f"\nPARTIAL SUCCESS: Data saved to MySQL but CSV save failed")
        print(f"MySQL Table: {db_name}.countryeconomy_data")
    else:
        print(f"\nFAILED: Could not extract detailed rating data")
    
    return success

# Backward compatibility
def get_countryeconomy_data():
    """Legacy function name for compatibility"""
    return get_countryeconomy_detailed_data()

if __name__ == "__main__":
    get_countryeconomy_detailed_data()