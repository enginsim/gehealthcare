"""
Country Economics Scoring System - MongoDB Implementation
Calculates 10-year weighted scores from credit ratings and saves to MongoDB
"""

import pymongo
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union
import json

# MongoDB Connection
try:
    client = pymongo.MongoClient('mongodb+srv://bugra:bugraigp@cluster0.edri3gv.mongodb.net/')
    client.admin.command('ismaster')
    print("✅ MongoDB connection successful")
except Exception as e:
    print(f"❌ MongoDB connection failed: {e}")
    exit()

class CountryEconomicsScoring:
    """Country Economics scoring system with MongoDB integration"""
    
    def __init__(self, database_name='gehealthcare'):
        self.client = client
        self.db = client[database_name]
        self.current_year = datetime.now().year
        
        # Rating scoring system for all agencies
        self.rating_scores = {
            # Moody's Long-term ratings
            'Aaa': 100, 'Aa1': 95, 'Aa2': 90, 'Aa3': 85,
            'A1': 80, 'A2': 75, 'A3': 70,
            'Baa1': 65, 'Baa2': 60, 'Baa3': 55,
            'Ba1': 50, 'Ba2': 45, 'Ba3': 40,
            'B1': 35, 'B2': 30, 'B3': 25,
            'Caa1': 20, 'Caa2': 15, 'Caa3': 10,
            'Ca': 5, 'C': 1,
            
            # S&P and Fitch Long-term ratings
            'AAA': 100, 'AA+': 95, 'AA': 90, 'AA-': 85,
            'A+': 80, 'A': 75, 'A-': 70,
            'BBB+': 65, 'BBB': 60, 'BBB-': 55,
            'BB+': 50, 'BB': 45, 'BB-': 40,
            'B+': 35, 'B': 30, 'B-': 25,
            'CCC+': 20, 'CCC': 15, 'CCC-': 10,
            'CC': 5, 'C': 1, 'D': 0
        }
        
        print(f"🏦 Initialized Country Economics Scoring for database: {database_name}")
    
    def get_rating_score(self, agency: str, rating: str, rating_type: str) -> Optional[float]:
        """Get numerical score for a credit rating"""
        if rating_type != 'Long term':
            return None
        
        return self.rating_scores.get(rating, None)
    
    def load_country_economics_data(self) -> List[Dict]:
        """Load country economics data from MongoDB"""
        print("📊 Loading country economics data from MongoDB...")
        
        try:
            collection_name = 'countryeconomy_data'
            
            # Check if collection exists
            if collection_name not in self.db.list_collection_names():
                print(f"❌ Collection '{collection_name}' not found")
                return []
            
            print(f"✅ Using collection: {collection_name}")
            
            # Load all data
            data = list(self.db[collection_name].find())
            print(f"📈 Loaded {len(data)} economics records")
            
            return data
            
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return []
    
    def calculate_weighted_score(self, country_data: List[Dict]) -> Optional[float]:
        """Calculate 10-year weighted score for a country"""
        
        # Years for analysis (2015-2024)
        years = list(range(self.current_year - 9, self.current_year + 1))
        agencies = ["Moody's", "S&P", "Fitch"]
        
        yearly_scores = {}
        
        # For each year, find the latest rating from each agency
        for year in years:
            yearly_scores[year] = {}
            
            for agency in agencies:
                # Filter ratings for this agency up to this year
                relevant_ratings = []
                
                for item in country_data:
                    if (item.get('Agency') == agency and 
                        item.get('Type') == 'Long term' and
                        item.get('Date')):
                        
                        try:
                            item_date = datetime.strptime(item['Date'], '%Y-%m-%d')
                            item_year = item_date.year
                            
                            if item_year <= year:
                                relevant_ratings.append({
                                    'date': item_date,
                                    'rating': item['Rating'],
                                    'year': item_year
                                })
                        except (ValueError, KeyError):
                            continue
                
                # Get the most recent rating for this agency
                if relevant_ratings:
                    latest_rating = max(relevant_ratings, key=lambda x: x['date'])
                    score = self.get_rating_score(agency, latest_rating['rating'], 'Long term')
                    
                    if score is not None:
                        yearly_scores[year][agency] = score
        
        # Calculate weighted average
        total_weighted_score = 0
        total_weight = 0
        
        for i, year in enumerate(years):
            weight = 10 - i  # 2024=10, 2023=9, ..., 2015=1
            agency_scores = yearly_scores[year]
            
            # Calculate average of available agencies for this year (equal weight)
            valid_scores = [score for score in agency_scores.values() if score is not None]
            
            if valid_scores:
                year_average = sum(valid_scores) / len(valid_scores)
                total_weighted_score += year_average * weight
                total_weight += weight
        
        return total_weighted_score / total_weight if total_weight > 0 else None
    
    def normalize_score(self, raw_score: float, min_score: float = 0, max_score: float = 100) -> float:
        """Normalize score to 0-100 scale"""
        if max_score == min_score:
            return 50.0  # Default middle value if no variance
        
        normalized = ((raw_score - min_score) / (max_score - min_score)) * 100
        return max(0, min(100, normalized))
    
    def calculate_all_country_scores(self) -> Dict:
        """Calculate scores for all countries"""
        print("🌍 Calculating Country Economics scores for all countries...")
        print("=" * 60)
        
        # Load data
        economics_data = self.load_country_economics_data()
        if not economics_data:
            return {}
        
        # Group by country
        country_groups = {}
        for item in economics_data:
            country = item.get('Country')
            if country:
                if country not in country_groups:
                    country_groups[country] = []
                country_groups[country].append(item)
        
        print(f"📊 Processing {len(country_groups)} countries...")
        
        # Calculate raw scores
        country_scores = {}
        raw_scores = []
        
        for country, data in country_groups.items():
            weighted_score = self.calculate_weighted_score(data)
            
            if weighted_score is not None:
                # Get Alpha3 code
                alpha3 = data[0].get('Alpha3', country[:3].upper())
                
                country_scores[country] = {
                    'country': country,
                    'alpha3': alpha3,
                    'raw_score': weighted_score,
                    'data_count': len(data)
                }
                raw_scores.append(weighted_score)
        
        # Normalize scores
        if raw_scores:
            min_raw = min(raw_scores)
            max_raw = max(raw_scores)
            
            print(f"📈 Raw score range: {min_raw:.2f} - {max_raw:.2f}")
            
            # Add normalized scores
            for country_data in country_scores.values():
                country_data['normalized_score'] = self.normalize_score(
                    country_data['raw_score'], min_raw, max_raw
                )
        
        # Sort by normalized score
        sorted_countries = sorted(
            country_scores.values(),
            key=lambda x: x['normalized_score'],
            reverse=True
        )
        
        return {
            'countries': sorted_countries,
            'total_countries': len(sorted_countries),
            'score_range': {'min': min_raw, 'max': max_raw} if raw_scores else None
        }
    
    def save_scores_to_mongodb(self, results: Dict) -> bool:
        """Save calculated scores to MongoDB"""
        print("💾 Saving Country Economics scores to MongoDB...")
        
        try:
            # Create or update country_scores collection
            collection = self.db.country_scores
            
            # Don't clear existing data - just update/insert Country Economics scores
            print("🔄 Updating existing country_scores collection...")
            
            # Process each country
            updated_count = 0
            inserted_count = 0
            
            for country_data in results['countries']:
                # Prepare update document
                update_doc = {
                    '$set': {
                        'countryeconomics_overall_normalized_score': round(country_data['normalized_score']/100, 4),
                        'countryeconomics_calculation_date': datetime.now(),
                        'countryeconomics_analysis_period': f"{self.current_year - 9}-{self.current_year}",
                    },
                    '$setOnInsert': {
                        'country': country_data['country'],
                        'alpha3': country_data['alpha3'],
                        'created_date': datetime.now()
                    }
                }
                
                # Try to find by alpha3 first, then by country name
                query = {'alpha3': country_data['alpha3']}
                existing = collection.find_one(query)
                
                if not existing:
                    query = {'country': country_data['country']}
                    existing = collection.find_one(query)
                
                # Upsert (update if exists, insert if not)
                result = collection.update_one(
                    query,
                    update_doc,
                    upsert=True
                )
                
                if result.matched_count > 0:
                    updated_count += 1
                else:
                    inserted_count += 1
            
            print(f"✅ Updated {updated_count} existing records")
            print(f"✅ Inserted {inserted_count} new records")
            print(f"📊 Total Country Economics records processed: {updated_count + inserted_count}")
            
            # Create indexes for better performance (only if they don't exist)
            try:
                collection.create_index("alpha3", background=True)
                collection.create_index("countryeconomics_overall_normalized_score", background=True)
                collection.create_index("country", background=True)
            except:
                pass  # Indexes might already exist
            
            return True
                
        except Exception as e:
            print(f"❌ Error saving to MongoDB: {e}")
            return False
    
    def print_top_countries(self, results: Dict, limit: int = 20):
        """Print top countries by score"""
        print(f"\n🏆 TOP {limit} COUNTRIES - COUNTRY ECONOMICS SCORES")
        print("=" * 70)
        print(f"{'Rank':<4} {'Country':<25} {'Alpha3':<6} {'Score':<8} {'Raw Score':<10}")
        print("-" * 70)
        
        for i, country in enumerate(results['countries'][:limit], 1):
            print(f"{i:<4} {country['country']:<25} {country['alpha3']:<6} "
                  f"{country['normalized_score']:<8.1f} {country['raw_score']:<10.2f}")
    
    def generate_summary_stats(self, results: Dict):
        """Generate summary statistics"""
        countries = results['countries']
        
        if not countries:
            return
        
        scores = [c['normalized_score'] for c in countries]
        raw_scores = [c['raw_score'] for c in countries]
        
        print(f"\n📊 COUNTRY ECONOMICS SCORING SUMMARY")
        print("=" * 50)
        print(f"📈 Total countries analyzed: {len(countries)}")
        print(f"📈 Analysis period: {self.current_year - 9}-{self.current_year}")
        print(f"📈 Methodology: 10-year weighted credit rating analysis")
        
        print(f"\n📊 Normalized Score Statistics (0-100):")
        print(f"  • Mean: {np.mean(scores):.1f}")
        print(f"  • Median: {np.median(scores):.1f}")
        print(f"  • Std Dev: {np.std(scores):.1f}")
        print(f"  • Min: {min(scores):.1f} ({countries[-1]['country']})")
        print(f"  • Max: {max(scores):.1f} ({countries[0]['country']})")
        
        print(f"\n📊 Raw Score Statistics:")
        print(f"  • Mean: {np.mean(raw_scores):.2f}")
        print(f"  • Median: {np.median(raw_scores):.2f}")
        print(f"  • Range: {min(raw_scores):.2f} - {max(raw_scores):.2f}")
        
        # Score distribution
        score_ranges = {
            'Excellent (90-100)': len([s for s in scores if s >= 90]),
            'Very Good (80-89)': len([s for s in scores if 80 <= s < 90]),
            'Good (70-79)': len([s for s in scores if 70 <= s < 80]),
            'Fair (60-69)': len([s for s in scores if 60 <= s < 70]),
            'Poor (50-59)': len([s for s in scores if 50 <= s < 60]),
            'Very Poor (<50)': len([s for s in scores if s < 50])
        }
        
        print(f"\n📊 Score Distribution:")
        for range_name, count in score_ranges.items():
            percentage = (count / len(scores)) * 100
            print(f"  • {range_name}: {count} countries ({percentage:.1f}%)")

def main():
    """Main execution function"""
    print("🏦 COUNTRY ECONOMICS SCORING SYSTEM")
    print("=" * 60)
    print("📊 10-Year Weighted Credit Rating Analysis")
    print("🔄 MongoDB Integration with Normalization")
    print("=" * 60)
    
    try:
        # Initialize scorer
        scorer = CountryEconomicsScoring()
        
        # Calculate all scores
        results = scorer.calculate_all_country_scores()
        
        if not results['countries']:
            print("❌ No countries processed. Check data availability.")
            return
        
        # Generate statistics
        scorer.generate_summary_stats(results)
        
        # Print top countries
        scorer.print_top_countries(results, 25)
        
        # Save to MongoDB
        success = scorer.save_scores_to_mongodb(results)
        
        if success:
            print(f"\n✅ SUCCESS! Country Economics scores calculated and saved.")
            print(f"📊 Collection: country_scores")
            print(f"📈 Field: countryeconomics_overall_normalized_score")
            
        else:
            print(f"\n❌ Failed to save scores to MongoDB")
        
        print(f"\n🎯 Analysis completed successfully!")
        
    except Exception as e:
        print(f"❌ Critical error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()