# OECD Economic Analysis Pipeline - Complete Version
# Includes all OECD collections: reserve_assets, debt_securities, employment_rate

import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class OECDAnalysisPipeline:
    def __init__(self):
        # MongoDB connection with correct database name
        self.client = MongoClient('mongodb+srv://bugra:bugraigp@cluster0.edri3gv.mongodb.net/')
        self.db = self.client['gehealthcare']
        
        # Target years for analysis
        self.target_years = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
        
        # Time-weighted scoring (recent years more important)
        self.score_weights = { 
            2024: 10, 2023: 9, 2022: 8, 2021: 7, 2020: 6, 
            2019: 5, 2018: 4, 2017: 3, 2016: 2, 2015: 1 
        }
        
        # 7-indicator weighting system (GDP excluded from scoring)
        self.indicator_weights = {
            'reserveAssets': 0.4,       # Financial security (Reserve Assets/GDP)
            'debtSecurities': 0.4,      # Fiscal sustainability (Debt Securities/GDP)
            'employment': 0.4,          # Labor market health
            'businessConfidence': 0.03, # Business sentiment
            'compositeLeading': 0.03,   # Economic outlook indicators
            'consumerConfidence': 0.03, # Consumer sentiment
            'consumerPriceIndex': 0.03  # Price stability (lower inflation better)
        }
        
        # Data storage
        self.raw_data = {}
        self.processed_data = {}
        self.combined_data = []
        self.country_scores = {}
        self.alpha3_mapping = {}

    def load_alpha3_mapping(self):
        """Load country code to alpha3 mapping"""
        self.alpha3_mapping = {
            'USA': 'USA', 'CHN': 'CHN', 'JPN': 'JPN', 'DEU': 'DEU', 'GBR': 'GBR',
            'FRA': 'FRA', 'ITA': 'ITA', 'BRA': 'BRA', 'CAN': 'CAN', 'RUS': 'RUS',
            'IND': 'IND', 'AUS': 'AUS', 'ESP': 'ESP', 'IDN': 'IDN', 'NLD': 'NLD',
            'TUR': 'TUR', 'CHE': 'CHE', 'SAU': 'SAU', 'ARG': 'ARG', 'SWE': 'SWE',
            'NOR': 'NOR', 'TWN': 'TWN', 'ZAF': 'ZAF', 'DNK': 'DNK', 'AUT': 'AUT',
            'BEL': 'BEL', 'FIN': 'FIN', 'IRL': 'IRL', 'PRT': 'PRT', 'GRC': 'GRC', 
            'POL': 'POL', 'CZE': 'CZE', 'HUN': 'HUN', 'KOR': 'KOR', 'MEX': 'MEX', 
            'CHL': 'CHL', 'ISR': 'ISR', 'NZL': 'NZL', 'LUX': 'LUX', 'EST': 'EST',
            'SVN': 'SVN', 'LTU': 'LTU', 'ISL': 'ISL'
        }

    def load_data_from_mongodb(self):
        """Load data from MongoDB collections"""
        print("\n📊 Loading data from MongoDB collections...")
        
        collections = {
            'gdp': 'oecd_gdp',
            'reserveAssets': 'oecd_reserve_assets',  # Now loading from real collection
            'debtSecurities': 'oecd_debt_securities',  # Now loading from real collection
            'employment': 'oecd_employment_rate',  # Now loading from real collection
            'businessConfidence': 'oecd_business_confidence',
            'compositeLeading': 'oecd_composite_leading',
            'consumerConfidence': 'oecd_consumer_confidence',
            'consumerPriceIndex': 'oecd_consumer_price_index'
        }
        
        total_records = 0
        
        for indicator, collection_name in collections.items():
            try:
                data = list(self.db[collection_name].find({}))
                self.raw_data[indicator] = data
                total_records += len(data)
                print(f"  ✅ {indicator}: {len(data)} records from {collection_name}")
                
                if data and len(data) > 0:
                    sample = data[0]
                    if indicator in ['reserveAssets', 'debtSecurities']:
                        # These collections have Measure field directly as number
                        print(f"      Sample: {sample.get('CountryCode')} | {sample.get('Time period')} | ${sample.get('Measure', 0):,.0f}")
                    else:
                        print(f"      Sample: {sample.get('CountryCode')} | {sample.get('Time period')} | {sample.get('Measure', 0)}")
                    
            except Exception as e:
                print(f"  ❌ Failed to load {collection_name}: {e}")
                self.raw_data[indicator] = []
        
        print(f"\n📊 Total records loaded: {total_records}")
        
        if total_records == 0:
            print("⚠️ No OECD data found! Analysis will exit.")
            return False
        
        return True

    def process_indicators(self):
        """Process and standardize all indicators"""
        print("\n🔄 Processing and standardizing indicators...")
        
        # Process GDP (annual data)
        self.processed_data['gdp'] = []
        if self.raw_data.get('gdp'):
            for item in self.raw_data['gdp']:
                try:
                    country_code = item.get('CountryCode')
                    time_period = item.get('Time period')
                    measure = item.get('Measure')
                    
                    if country_code and time_period and measure is not None:
                        year = int(time_period)
                        if year in self.target_years:
                            self.processed_data['gdp'].append({
                                'CountryCode': country_code,
                                'Year': year,
                                'Value': float(measure)
                            })
                except (ValueError, TypeError):
                    continue
        
        print(f"📊 GDP (reference): {len(self.processed_data['gdp'])} records")

        # Process quarterly indicators (reserve assets, debt securities, employment)
        quarterly_indicators = ['reserveAssets', 'debtSecurities', 'employment']
        
        for indicator in quarterly_indicators:
            self.processed_data[indicator] = []
            
            if self.raw_data.get(indicator):
                annual_data = {}
                
                for item in self.raw_data[indicator]:
                    try:
                        country_code = item.get('CountryCode')
                        time_period = item.get('Time period')
                        measure = item.get('Measure')
                        
                        if country_code and time_period and measure is not None:
                            # Extract year from quarterly format (e.g., "2023-Q1" -> 2023)
                            if '-Q' in str(time_period):
                                year = int(time_period.split('-')[0])
                            else:
                                year = int(time_period.split('-')[0])
                            
                            if year in self.target_years:
                                key = f"{country_code}_{year}"
                                if key not in annual_data:
                                    annual_data[key] = []
                                
                                # For reserve assets and debt securities, values are in millions USD
                                if indicator in ['reserveAssets', 'debtSecurities']:
                                    annual_data[key].append(float(measure) * 1e6)  # Convert to USD
                                else:
                                    annual_data[key].append(float(measure))
                                
                    except (ValueError, TypeError, IndexError):
                        continue
                
                # Calculate annual averages
                for key, values in annual_data.items():
                    country_code, year = key.split('_')
                    self.processed_data[indicator].append({
                        'CountryCode': country_code,
                        'Year': int(year),
                        'Value': np.mean(values)
                    })
            
            print(f"📊 {indicator}: {len(self.processed_data[indicator])} annual records")

        # Process monthly indicators
        monthly_indicators = ['businessConfidence', 'compositeLeading', 'consumerConfidence', 'consumerPriceIndex']
        
        for indicator in monthly_indicators:
            self.processed_data[indicator] = []
            
            if self.raw_data.get(indicator):
                annual_data = {}
                
                for item in self.raw_data[indicator]:
                    try:
                        country_code = item.get('CountryCode')
                        time_period = item.get('Time period')
                        measure = item.get('Measure')
                        
                        if country_code and time_period and measure is not None:
                            year = int(time_period.split('-')[0])
                            
                            if year in self.target_years:
                                key = f"{country_code}_{year}"
                                if key not in annual_data:
                                    annual_data[key] = []
                                annual_data[key].append(float(measure))
                                
                    except (ValueError, TypeError, IndexError):
                        continue
                
                for key, values in annual_data.items():
                    country_code, year = key.split('_')
                    self.processed_data[indicator].append({
                        'CountryCode': country_code,
                        'Year': int(year),
                        'Value': np.mean(values)
                    })
            
            print(f"📊 {indicator}: {len(self.processed_data[indicator])} annual records")
        
        return True

    def create_combined_dataset(self):
        """Create combined dataset with GDP ratios"""
        print("\n🔗 Creating combined dataset...")
        
        # Create GDP lookup
        gdp_lookup = {}
        for item in self.processed_data.get('gdp', []):
            key = f"{item['CountryCode']}_{item['Year']}"
            gdp_lookup[key] = item['Value']
        
        # Get all unique countries from OECD data
        all_countries = set()
        for indicator_data in self.processed_data.values():
            for item in indicator_data:
                all_countries.add(item['CountryCode'])
        
        print(f"📊 Countries with OECD data: {len(all_countries)}")
        print(f"🎯 Sample countries: {sorted(list(all_countries))[:15]}")
        
        # Build combined dataset
        self.combined_data = []
        
        for country in all_countries:
            for year in self.target_years:
                gdp_key = f"{country}_{year}"
                # Use realistic GDP estimates if missing
                gdp_value = gdp_lookup.get(gdp_key, self._estimate_gdp(country))
                
                record = {
                    'CountryCode': country,
                    'Year': year,
                    'gdp': gdp_value
                }
                
                # Add Reserve Assets to GDP ratio
                reserve_item = next((x for x in self.processed_data.get('reserveAssets', []) 
                                   if x['CountryCode'] == country and x['Year'] == year), None)
                if reserve_item:
                    record['reserveAssetsToGDP'] = (reserve_item['Value']**1.5 / gdp_value) * 100
                
                # Add Debt Securities to GDP ratio  
                debt_item = next((x for x in self.processed_data.get('debtSecurities', []) 
                                if x['CountryCode'] == country and x['Year'] == year), None)
                if debt_item:
                    record['debtSecuritiesToGDP'] = (debt_item['Value']**1.5 / gdp_value) * 100
                
                # Add other indicators
                for indicator in ['employment', 'businessConfidence', 'compositeLeading', 'consumerConfidence', 'consumerPriceIndex']:
                    item = next((x for x in self.processed_data.get(indicator, []) 
                               if x['CountryCode'] == country and x['Year'] == year), None)
                    if item:
                        record[indicator] = item['Value']
                
                # Only include records with at least 3 indicators
                indicator_count = sum(1 for key in record.keys() 
                                    if key not in ['CountryCode', 'Year', 'gdp'] and record.get(key) is not None)
                if indicator_count >= 3:
                    self.combined_data.append(record)

        print(f"✅ Combined dataset: {len(self.combined_data)} records")
        print(f"📊 Countries with sufficient data: {len(set(item['CountryCode'] for item in self.combined_data))}")
        
        return True

    def _estimate_gdp(self, country):
        """Estimate GDP for countries without data"""
        # Rough GDP estimates by country (in millions USD)
        gdp_estimates = {
            'USA': 20e12, 'CHN': 14e12, 'JPN': 5e12, 'DEU': 4e12, 'GBR': 3e12,
            'FRA': 2.7e12, 'ITA': 2e12, 'BRA': 1.8e12, 'CAN': 1.7e12, 'RUS': 1.5e12,
            'KOR': 1.6e12, 'ESP': 1.4e12, 'AUS': 1.3e12, 'MEX': 1.1e12, 'IDN': 1e12,
            'NLD': 900e9, 'SAU': 700e9, 'TUR': 750e9, 'CHE': 750e9, 'NOR': 360e9,
            'SWE': 540e9, 'DNK': 350e9, 'AUT': 430e9, 'BEL': 520e9, 'FIN': 270e9,
            'ISL': 25e9, 'LUX': 85e9, 'EST': 35e9, 'SVN': 55e9, 'LTU': 65e9
        }
        return gdp_estimates.get(country, 500e9)  # Default 500B

    def initialize_country_scores(self):
        """Initialize country scoring structure"""
        countries = set(item['CountryCode'] for item in self.combined_data)
        
        for country in countries:
            self.country_scores[country] = {
                'yearly_scores': {},
                'component_scores': {},
                'overall_score': 0,
                'overall_normalized_score': 0
            }
        
        print(f"📊 Initialized scoring for {len(countries)} countries")
        return True

    def calculate_percentile_scores(self, indicator, higher_is_better=True):
        """Calculate percentile scores for an indicator across years"""
        
        for year in self.target_years:
            year_weight = self.score_weights[year]
            
            valid_data = [item for item in self.combined_data 
                         if item['Year'] == year and indicator in item and item[indicator] is not None]
            
            if len(valid_data) < 2:
                continue
            
            sorted_data = sorted(valid_data, key=lambda x: x[indicator], reverse=higher_is_better)

            for index, country_data in enumerate(sorted_data):
                percentile_score = ((len(sorted_data) - index) / len(sorted_data)) * 100
                weighted_score = (percentile_score / 100) * year_weight

                country_code = country_data['CountryCode']
                year = country_data['Year']
                
                if year not in self.country_scores[country_code]['yearly_scores']:
                    self.country_scores[country_code]['yearly_scores'][year] = {}
                
                self.country_scores[country_code]['yearly_scores'][year][indicator] = {
                    'score': weighted_score,
                    'value': country_data[indicator],
                    'percentile': percentile_score
                }

    def calculate_final_scores(self):
        """Calculate final weighted scores"""
        
        for country in self.country_scores.keys():
            total_weighted_score = 0
            year_count = 0
            component_totals = {key: 0 for key in self.indicator_weights.keys()}

            for year in self.target_years:
                year_scores = self.country_scores[country]['yearly_scores'].get(year, {})
                if not year_scores:
                    continue

                year_score = 0
                has_data = False

                indicator_mapping = {
                    'reserveAssets': 'reserveAssetsToGDP',
                    'debtSecurities': 'debtSecuritiesToGDP',
                    'employment': 'employment',
                    'businessConfidence': 'businessConfidence',
                    'compositeLeading': 'compositeLeading',
                    'consumerConfidence': 'consumerConfidence',
                    'consumerPriceIndex': 'consumerPriceIndex'
                }

                for component, weight in self.indicator_weights.items():
                    mapped_indicator = indicator_mapping.get(component, component)
                    if mapped_indicator in year_scores:
                        component_score = year_scores[mapped_indicator]['score']
                        year_score += component_score * weight
                        component_totals[component] += component_score * weight
                        print(country,component_score," and ",weight)
                        has_data = True

                if has_data:
                    total_weighted_score += year_score
                    year_count += 1

            if year_count > 0:
                overall_score = total_weighted_score / year_count
                overall_normalized_score = overall_score / 100.0
                
                self.country_scores[country]['overall_score'] = overall_score
                self.country_scores[country]['overall_normalized_score'] = overall_normalized_score
                
                for component in self.indicator_weights.keys():
                    if year_count > 0:
                        component_score = component_totals[component] / year_count
                        self.country_scores[country]['component_scores'][component] = component_score / 100.0

        print("✅ Final scores calculated with 0-1 normalization")
        return True

    def update_country_scores_collection(self):
        """Update OECD normalized score in existing country_scores documents"""
        print("\n💾 Updating OECD scores in country_scores collection...")
        
        successful_updates = 0
        failed_updates = 0
        
        for country_code, country_data in self.country_scores.items():
            if country_data['overall_normalized_score'] <= 0:
                continue
                
            # Map to Alpha3
            alpha3_code = self.alpha3_mapping.get(country_code, country_code)
            oecd_normalized = country_data['overall_normalized_score']
            
            try:
                # Simple update - only add oecd_overall_normalized_score field
                update_doc = {
                    '$set': {
                        'oecd_overall_normalized_score': round(oecd_normalized, 4)
                    }
                }
                
                # Update document by alpha3 code
                result = self.db.country_scores.update_one(
                    {'alpha3': alpha3_code},
                    update_doc,
                    upsert=False  # Don't create new documents
                )
                
                if result.matched_count > 0:
                    if result.modified_count > 0:
                        print(f"✅ {alpha3_code}: OECD score {oecd_normalized:.4f} updated")
                    else:
                        print(f"⚠️ {alpha3_code}: Score unchanged (same value)")
                    successful_updates += 1
                else:
                    print(f"⚠️ {alpha3_code}: Document not found in country_scores collection")
                    failed_updates += 1
                
            except Exception as e:
                print(f"❌ {alpha3_code}: Failed to update - {e}")
                failed_updates += 1
        
        print(f"\n📊 OECD Update Summary:")
        print(f"✅ Successful updates: {successful_updates}")
        print(f"❌ Failed updates: {failed_updates}")
        
        return successful_updates > 0

    def run_analysis(self):
        """Run complete OECD analysis pipeline"""
        print("\n🚀 Starting OECD Analysis Pipeline...")
        print("=" * 50)
        
        try:
            # Load alpha3 mapping
            self.load_alpha3_mapping()
            
            # Load and process data
            if not self.load_data_from_mongodb():
                return False
                
            if not self.process_indicators():
                return False
                
            if not self.create_combined_dataset():
                return False
                
            if not self.initialize_country_scores():
                return False
            
            # Calculate scores
            print("\n📊 Calculating percentile scores for indicators...")
            self.calculate_percentile_scores('reserveAssetsToGDP', higher_is_better=True)
            self.calculate_percentile_scores('debtSecuritiesToGDP', higher_is_better=False)
            self.calculate_percentile_scores('employment', higher_is_better=True)
            self.calculate_percentile_scores('businessConfidence', higher_is_better=True)
            self.calculate_percentile_scores('compositeLeading', higher_is_better=True)
            self.calculate_percentile_scores('consumerConfidence', higher_is_better=True)
            self.calculate_percentile_scores('consumerPriceIndex', higher_is_better=False)
            
            if not self.calculate_final_scores():
                return False
            
            # Update country_scores collection
            success = self.update_country_scores_collection()
            
            if success:
                print("\n✅ OECD Analysis Pipeline completed successfully!")
                print(f"📊 Results normalized to 0-1 range")
                print(f"🗄️ OECD components updated in existing country_scores documents")
                
                # Display top results
                self._display_top_results()
                
                return True
            else:
                print("\n❌ Analysis completed but failed to update country_scores collection")
                return False
                
        except Exception as error:
            print(f"\n❌ Analysis pipeline failed: {error}")
            import traceback
            traceback.print_exc()
            return False
        
        finally:
            self.client.close()

    def _display_top_results(self):
        """Display top results"""
        print(f"\n🏆 Top 15 Countries by OECD Score:")
        print("=" * 55)
        
        sorted_countries = sorted(
            [(country, data['overall_score'], data['overall_normalized_score']) 
             for country, data in self.country_scores.items() 
             if data['overall_score'] > 0],
            key=lambda x: x[1], 
            reverse=True
        )
        
        for i, (country_code, score, normalized_score) in enumerate(sorted_countries[:15]):
            alpha3_code = self.alpha3_mapping.get(country_code, country_code)
            print(f"{i+1:2d}. {alpha3_code:3s} | Score: {score:.1f}/100 | Normalized: {normalized_score:.4f}")

# Main execution function
def main():
    """Main function to run the analysis"""
    try:
        pipeline = OECDAnalysisPipeline()
        pipeline.run_analysis()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

# Run the analysis
if __name__ == "__main__":
    main()