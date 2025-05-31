import requests
import xml.etree.ElementTree as ET
import json
import os
import pandas as pd
from datetime import datetime
import sys
import time
import io

class OECDDataFetcher:
    """
    OECD data fetcher using the current working API endpoints from data-explorer.oecd.org
    Automatically fetches Employment data using specific SDMX query
    """
    
    def __init__(self, output_dir=None):
        """
        Initialize the class
        
        Parameters:
        output_dir (str): Directory where data will be saved. If None, uses GitHub project structure.
        """
        if output_dir is None:
            # Get the current script directory (src folder)
            current_dir = os.path.dirname(os.path.abspath(__file__))
            
            # Go up one level from src to project root, then to data/processed
            project_root = os.path.dirname(current_dir)  # Go up from src to gehealthcare
            self.output_dir = os.path.join(project_root, "data", "processed")
        else:
            self.output_dir = output_dir
            
        self.data_cache = {}  # Data stored in memory
        
        # Create output directory and any necessary parent directories
        os.makedirs(self.output_dir, exist_ok=True)
        print(f"📁 Output directory set to: {os.path.abspath(self.output_dir)}")
        print(f"✅ Directory structure created/verified")
        
        # Show relative path from project root for clarity
        try:
            # Get relative path from project root
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            rel_path = os.path.relpath(self.output_dir, project_root)
            print(f"📂 Relative path from project root: {rel_path}")
        except:
            pass
    
    def fetch_data_from_api(self, api_url, description="data"):
        """
        Generic method to fetch data from OECD API URLs
        
        Parameters:
        api_url (str): Complete OECD API URL
        description (str): Description of the data being fetched
        
        Returns:
        pandas.DataFrame: Fetched data
        """
        print(f"Fetching {description}...")
        print(f"API URL: {api_url}")
        
        try:
            # Add CSV format parameter if not present
            if "format=csv" not in api_url:
                separator = "&" if "?" in api_url else "?"
                api_url = f"{api_url}{separator}format=csv"
            
            response = requests.get(api_url)
            response.raise_for_status()
            
            # Check if response is CSV
            if 'csv' in response.headers.get('Content-Type', '').lower() or \
               'csv' in api_url or \
               b',' in response.content[:100]:  # Basic CSV detection
                
                # Read as CSV
                try:
                    df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
                    print(f"✓ Successfully fetched {len(df)} rows of {description}")
                    return df
                except Exception as e:
                    print(f"Error parsing CSV: {e}")
                    print(f"Content sample: {response.content[:200]}")
                    return None
            else:
                # Try to parse as XML (SDMX-XML format)
                try:
                    # Parse XML response
                    root = ET.fromstring(response.content)
                    
                    # Find namespace
                    ns = {}
                    if '}' in root.tag:
                        ns_uri = root.tag.split('}')[0].strip('{')
                        ns['ns'] = ns_uri
                        print(f"XML Namespace found: {ns_uri}")
                    
                    # Extract data from SDMX XML
                    data_points = []
                    
                    # Look for Series elements in SDMX format
                    for series in root.findall('.//{*}Series'):
                        series_data = {}
                        
                        # Get series attributes (country, indicator, etc.)
                        for key, value in series.attrib.items():
                            series_data[key] = value
                        
                        # Get observations within this series
                        for obs in series.findall('.//{*}Obs'):
                            obs_data = series_data.copy()
                            
                            # Get observation attributes (time period, value)
                            for key, value in obs.attrib.items():
                                obs_data[key] = value
                            
                            data_points.append(obs_data)
                    
                    if data_points:
                        df = pd.DataFrame(data_points)
                        print(f"✓ Successfully fetched {len(df)} rows of {description} from XML")
                        return df
                    else:
                        print("No data points found in XML response.")
                        return None
                        
                except ET.ParseError as e:
                    print(f"XML parsing error: {e}")
                    print(f"Content sample: {response.content[:300]}")
                    return None
                except Exception as e:
                    print(f"Error processing XML: {e}")
                    print(f"Content sample: {response.content[:300]}")
                    return None
                
        except requests.exceptions.RequestException as e:
            print(f"API request error: {e}")
            if hasattr(e, 'response') and e.response:
                print(f"Status code: {e.response.status_code}")
                print(f"Response snippet: {e.response.text[:300]}")
            return None
        except Exception as e:
            print(f"Unexpected error: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def save_data(self, data, filename):
        """
        Save data to CSV file with simplified column structure
        """
        if data is not None and not data.empty:
            # Create simplified dataframe with only required columns
            simplified_data = self.simplify_dataframe(data)
            
            if simplified_data is not None and not simplified_data.empty:
                filepath = os.path.join(self.output_dir, f"{filename}.csv")
                simplified_data.to_csv(filepath, index=False)
                print(f"  ✓ Data saved to: {filepath}")
                
                # Display data info
                print(f"  📊 Data shape: {simplified_data.shape}")
                print(f"  📋 Columns: {list(simplified_data.columns)}")
                
                return True
            else:
                print(f"  ✗ No simplified data to save for {filename}")
                return False
        else:
            print(f"  ✗ No data to save for {filename}")
            return False

    def simplify_dataframe(self, df):
        """
        Simplify dataframe to only include Reference area, Measure, and Time period columns
        
        Parameters:
        df (pandas.DataFrame): Original dataframe from OECD API
        
        Returns:
        pandas.DataFrame: Simplified dataframe with 3 columns
        """
        if df is None or df.empty:
            return None
        
        simplified_df = pd.DataFrame()
        
        # Map common OECD column names to our desired names
        column_mapping = {
            # Reference area (country) columns
            'REF_AREA': 'Reference area',
            'LOCATION': 'Reference area',
            'Country': 'Reference area',
            'COUNTRY': 'Reference area',
            
            # Measure (value) columns  
            'OBS_VALUE': 'Measure',
            'VALUE': 'Measure',
            'Value': 'Measure',
            'OBSVALUE': 'Measure',
            'value': 'Measure',
            
            # Time period columns
            'TIME_PERIOD': 'Time period',
            'TIME': 'Time period',
            'Date': 'Time period',
            'PERIOD': 'Time period',
            'ObsTime': 'Time period'
        }
        
        # Find and map columns
        for original_col, target_col in column_mapping.items():
            if original_col in df.columns and target_col not in simplified_df.columns:
                simplified_df[target_col] = df[original_col]
        
        # If we couldn't find standard column names, try to identify by content
        if 'Reference area' not in simplified_df.columns:
            # Look for country-like columns (short codes)
            for col in df.columns:
                if col.upper() in ['REF_AREA', 'LOCATION', 'COUNTRY'] or \
                   any(df[col].astype(str).str.len().eq(3).sum() > len(df) * 0.5 for _ in [None]):  # Country codes are often 3 letters
                    simplified_df['Reference area'] = df[col]
                    break
        
        if 'Measure' not in simplified_df.columns:
            # Look for numeric value columns
            numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
            if len(numeric_cols) > 0:
                # Use the first numeric column as measure
                simplified_df['Measure'] = df[numeric_cols[0]]
        
        if 'Time period' not in simplified_df.columns:
            # Look for time-like columns
            for col in df.columns:
                if 'time' in col.lower() or 'date' in col.lower() or 'period' in col.lower():
                    simplified_df['Time period'] = df[col]
                    break
        
        # Ensure we have all three required columns
        required_columns = ['Reference area', 'Measure', 'Time period']
        for col in required_columns:
            if col not in simplified_df.columns:
                print(f"  ⚠️  Warning: Could not find '{col}' column. Available columns: {list(df.columns)}")
                # Add empty column as placeholder
                simplified_df[col] = 'N/A'
        
        # Reorder columns
        simplified_df = simplified_df[required_columns]
        
        # Clean data types
        if 'Measure' in simplified_df.columns:
            # Try to convert measure to numeric
            try:
                simplified_df['Measure'] = pd.to_numeric(simplified_df['Measure'], errors='coerce')
            except:
                pass
        
        print(f"  📋 Original columns: {list(df.columns)}")
        print(f"  📋 Simplified to: {list(simplified_df.columns)}")
        print(f"  📊 Simplified data preview:")
        print(simplified_df.head(3))
        
        return simplified_df

    def fetch_employment_working_age_population(self):
        """
        Fetch Employment to Working Age Population data using the specific SDMX query
        
        Returns:
        pandas.DataFrame: Employment data for all OECD countries (last 10 years)
        """
        # The specific SDMX query URL you provided - updated for last 10 years
        employment_url = "https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_LFS@DF_IALFS_EMP_WAP_Q,1.0/LTU+LVA+KOR+JPN+ISR+IRL+ISL+HUN+GRC+DEU+FRA+FIN+EST+DNK+CZE+CRI+COL+CHL+BEL+CAN+AUT+AUS+ITA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA.EMP_WAP.._Z.Y._T.Y15T64..Q?startPeriod=2015-Q1&dimensionAtObservation=AllDimensions"
        
        return self.fetch_data_from_api(employment_url, "Employment to Working Age Population (Quarterly)")

    def fetch_reserve_assets(self):
        """
        Fetch Reserve Assets data using the specific SDMX query
        
        Returns:
        pandas.DataFrame: Reserve Assets data for selected countries (last 10 years)
        """
        # The specific SDMX query URL for Reserve Assets - updated for last 10 years
        reserves_url = "https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_BOP@DF_IIP,1.0/SAU+IND+CHN+USA+GBR+CHE+SWE+ESP+SVN+SVK+PRT+POL+NOR+NZL+NLD+LUX+LTU+LVA+JPN+ITA+IRL+ISL+HUN+GRC+DEU+FIN+EST+DNK+CZE+CAN+BEL+AUT+AUS+FRA..FA_R_F_S121...Q.XDC.?startPeriod=2015-Q1&dimensionAtObservation=AllDimensions"
        
        return self.fetch_data_from_api(reserves_url, "Reserve Assets (Quarterly)")

    def fetch_debt_securities(self):
        """
        Fetch Debt Securities data using the specific SDMX query
        
        Returns:
        pandas.DataFrame: Debt Securities data for selected countries (last 10 years)
        """
        # The specific SDMX query URL for Debt Securities - updated for last 10 years
        debt_url = "https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_BOP@DF_IIP,1.0/SAU+IND+CHN+USA+GBR+CHE+SWE+ESP+SVN+SVK+PRT+POL+NOR+NZL+NLD+LUX+LTU+LVA+JPN+ITA+IRL+ISL+HUN+GRC+DEU+FIN+EST+DNK+CZE+CAN+BEL+AUT+AUS+FRA..FA_P_F3...Q.XDC.?startPeriod=2015-Q1&dimensionAtObservation=AllDimensions"
        
        return self.fetch_data_from_api(debt_url, "Debt Securities (Quarterly)")

# Main execution
if __name__ == "__main__":
    fetcher = OECDDataFetcher()
    
    print("=== OECD Economic Data Fetcher - GitHub Project Integration ===")
    print("Fetching Employment, Reserve Assets, and Debt Securities data")
    print("📅 Data Period: Last 10 years (2015-Q1 to present)")
    print(f"📁 Running from: src/ directory")
    print(f"📂 Output target: ../data/processed/")
    print()
    
    successful_fetches = 0
    total_datasets = 3
    
    # 1. Fetch Employment Data
    print("1. Fetching Employment to Working Age Population data...")
    print("   📅 Data period: 2015-Q1 to present (last 10 years)")
    print("   🌍 Countries: All OECD members")
    
    employment_data = fetcher.fetch_employment_working_age_population()
    
    if fetcher.save_data(employment_data, "oecd_employment_rate"):
        successful_fetches += 1
        if employment_data is not None:
            print(f"\n   📋 Sample employment data:")
            print(employment_data.head())
            
            # Show unique countries if REF_AREA column exists
            if 'Reference area' in employment_data.columns:
                countries = employment_data['Reference area'].unique()
                print(f"\n   🌍 Employment data countries ({len(countries)}): {', '.join(sorted(countries))}")
    
    print("\n" + "="*60)
    
    # 2. Fetch Reserve Assets Data
    print("\n2. Fetching Reserve Assets data...")
    print("   📅 Data period: 2015-Q1 to present (last 10 years)")
    print("   🌍 Countries: Major economies including SAU, IND, CHN, USA, etc.")
    
    reserves_data = fetcher.fetch_reserve_assets()
    
    if fetcher.save_data(reserves_data, "oecd_reserve_assets"):
        successful_fetches += 1
        if reserves_data is not None:
            print(f"\n   📋 Sample reserve assets data:")
            print(reserves_data.head())
            
            # Show unique countries if REF_AREA column exists
            if 'Reference area' in reserves_data.columns:
                countries = reserves_data['Reference area'].unique()
                print(f"\n   🌍 Reserve assets data countries ({len(countries)}): {', '.join(sorted(countries))}")
    
    print("\n" + "="*60)
    
    # 3. Fetch Debt Securities Data
    print("\n3. Fetching Debt Securities data...")
    print("   📅 Data period: 2015-Q1 to present (last 10 years)")
    print("   🌍 Countries: Major economies including SAU, IND, CHN, USA, etc.")
    
    debt_data = fetcher.fetch_debt_securities()
    
    if fetcher.save_data(debt_data, "oecd_debt_securities"):
        successful_fetches += 1
        if debt_data is not None:
            print(f"\n   📋 Sample debt securities data:")
            print(debt_data.head())
            
            # Show unique countries if REF_AREA column exists
            if 'Reference area' in debt_data.columns:
                countries = debt_data['Reference area'].unique()
                print(f"\n   🌍 Debt securities data countries ({len(countries)}): {', '.join(sorted(countries))}")
    
    # Final Summary
    print(f"\n" + "="*60)
    print(f"=== Final Summary ===")
    print(f"Successfully fetched: {successful_fetches}/{total_datasets} datasets")
    print(f"Data files saved in GitHub project: {fetcher.output_dir}/")
    print(f"📂 Full path: {os.path.abspath(fetcher.output_dir)}")
    
    if successful_fetches > 0:
        print(f"\n✅ SUCCESS: {successful_fetches} dataset(s) retrieved successfully!")
        print("📁 Files saved to GitHub project:")
        for file in os.listdir(fetcher.output_dir):
            if file.endswith('.csv'):
                filepath = os.path.join(fetcher.output_dir, file)
                try:
                    file_size = os.path.getsize(filepath)
                    # Show relative path for GitHub project
                    relative_path = os.path.join("gehealthcare", "data", "processed", file)
                    print(f"  - {relative_path} ({file_size} bytes)")
                except:
                    relative_path = os.path.join("gehealthcare", "data", "processed", file)
                    print(f"  - {relative_path}")
                    
        # Combined data analysis if all datasets were successful
        if successful_fetches == 3:
            print(f"\n📊 Combined Dataset Information:")
            if employment_data is not None:
                print(f"  • Employment data: {len(employment_data)} observations")
            if reserves_data is not None:
                print(f"  • Reserve assets data: {len(reserves_data)} observations")
            if debt_data is not None:
                print(f"  • Debt securities data: {len(debt_data)} observations")
        
        print(f"\n🔧 GitHub Integration Tips:")
        print(f"  • Files are ready for commit to your repository")
        print(f"  • Add to .gitignore if you don't want to track these files")
        print(f"  • Use these relative paths in your analysis scripts:")
        print(f"    - ../data/processed/oecd_employment_rate.csv")
        print(f"    - ../data/processed/oecd_reserve_assets.csv")
        print(f"    - ../data/processed/oecd_debt_securities.csv")
                    
    else:
        print("\n❌ FAILED: Could not fetch any data")
        print("This might be due to:")
        print("  - API endpoint temporary issues")
        print("  - Network connectivity problems") 
        print("  - SDMX query format changes")
        print("  - Server rate limiting")
    
    print(f"\n📊 Query Details:")
    print(f"1. Employment Dataset: DSD_LFS@DF_IALFS_EMP_WAP_Q")
    print(f"   • Indicator: EMP_WAP (Employment to Working Age Population)")
    print(f"   • Frequency: Quarterly")
    print(f"   • Period: 2015-Q1 to present (10 years)")
    print(f"2. Reserve Assets Dataset: DSD_BOP@DF_IIP")
    print(f"   • Indicator: FA_R_F_S121 (Reserve Assets)")
    print(f"   • Frequency: Quarterly")
    print(f"   • Period: 2015-Q1 to present (10 years)")
    print(f"3. Debt Securities Dataset: DSD_BOP@DF_IIP")
    print(f"   • Indicator: FA_P_F3 (Debt Securities)")
    print(f"   • Frequency: Quarterly")
    print(f"   • Period: 2015-Q1 to present (10 years)")