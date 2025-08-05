#!/usr/bin/env python3
"""
World Bank Governance Indicators Analysis
MongoDB Integration Script

This script performs time-weighted analysis of World Bank governance indicators,
calculates normalized scores (0-1), and stores results in MongoDB.

Author: Data Analysis Team
Date: July 2025
"""

import pymongo
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from typing import Dict, List, Optional
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GovernanceAnalyzer:
    """
    Analyzes World Bank Governance Indicators with time-weighted scoring
    """
    
    def __init__(self, connection_string: str, database_name: str):
        """
        Initialize MongoDB connection and analysis parameters
        
        Args:
            connection_string: MongoDB connection string
            database_name: Database name
        """
        self.client = pymongo.MongoClient(connection_string)
        self.db = self.client[database_name]
        self.indicators = ['cc', 'ge', 'pv', 'rl', 'rq', 'va']
        self.latest_year = 2023
        self.analysis_years = 10  # Last 10 years
        
    def get_weight(self, year: int) -> int:
        """
        Calculate time weight for given year
        
        Args:
            year: Year to calculate weight for
            
        Returns:
            Weight value (10 for latest year, decreasing to 1)
        """
        years_from_latest = self.latest_year - year
        if years_from_latest >= self.analysis_years:
            return 0
        return self.analysis_years - years_from_latest
    
    def load_worldbank_data(self) -> pd.DataFrame:
        """
        Load World Bank governance data from MongoDB
        
        Returns:
            DataFrame with governance indicators data
        """
        logger.info("Loading World Bank governance data from MongoDB...")
        
        try:
            # Query World Bank collection
            collection = self.db['worldbank_data']
            
            # Create aggregation pipeline to get data from last 10 years
            pipeline = [
                {
                    '$match': {
                        'year': {'$gte': self.latest_year - self.analysis_years + 1},
                        'indicator': {'$in': self.indicators},
                        'estimate': {'$ne': None}
                    }
                },
                {
                    '$project': {
                        '_id': 1,
                        'indicator': 1,
                        'year': 1,
                        'code': 1,
                        'countryname': 1,
                        'estimate': 1,
                        'nsource': 1,
                        'pctrank': 1,
                        'stddev': 1
                    }
                }
            ]
            
            # Execute query
            cursor = collection.aggregate(pipeline)
            data = list(cursor)
            
            if not data:
                raise ValueError("No data found in worldbank_data collection")
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            logger.info(f"Loaded {len(df)} records from MongoDB")
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading data from MongoDB: {e}")
            raise
    
    def calculate_weighted_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate time-weighted scores for each country and indicator
        
        Args:
            df: Raw governance data
            
        Returns:
            DataFrame with weighted scores and metadata
        """
        logger.info("Calculating time-weighted scores...")
        
        results = []
        countries = df['code'].unique()
        
        for i, country_code in enumerate(countries):
            if i % 50 == 0:
                logger.info(f"Processing country {i+1}/{len(countries)}")
            
            # Get country name
            country_name = df[df['code'] == country_code]['countryname'].iloc[0]
            
            country_result = {
                'country_code': country_code,
                'country_name': country_name,
                'analysis_date': datetime.now(),
                'method': 'time_weighted_last_10_years'
            }
            
            # Calculate weighted score for each indicator
            indicator_scores = []
            total_weights = []
            total_weighted_sums = []
            
            for indicator in self.indicators:
                result = self._calculate_indicator_weighted_score(df, country_code, indicator)
                
                # Store individual indicator results
                country_result[f'{indicator}_weighted_score'] = result['weighted_score']
                country_result[f'{indicator}_total_weight'] = result['total_weight']
                country_result[f'{indicator}_weighted_sum'] = result['weighted_sum']
                country_result[f'{indicator}_data_points'] = result['data_points']
                
                if result['weighted_score'] is not None:
                    indicator_scores.append(result['weighted_score'])
                    total_weights.append(result['total_weight'])
                    total_weighted_sums.append(result['weighted_sum'])
            
            # Calculate overall weighted score and totals
            if indicator_scores:
                country_result['overall_weighted_score'] = np.mean(indicator_scores)
                country_result['overall_total_weight'] = sum(total_weights)
                country_result['overall_weighted_sum'] = sum(total_weighted_sums)
                country_result['indicators_with_data'] = len(indicator_scores)
            else:
                country_result['overall_weighted_score'] = None
                country_result['overall_total_weight'] = 0
                country_result['overall_weighted_sum'] = 0
                country_result['indicators_with_data'] = 0
            
            results.append(country_result)
        
        return pd.DataFrame(results)
    
    def _calculate_indicator_weighted_score(self, df: pd.DataFrame, 
                                          country_code: str, indicator: str) -> Dict:
        """
        Calculate weighted score for specific country and indicator
        
        Args:
            df: Raw data
            country_code: Country code
            indicator: Indicator code
            
        Returns:
            Dictionary with weighted score, total weight, and metadata
        """
        # Filter data for this country and indicator
        country_data = df[
            (df['code'] == country_code) & 
            (df['indicator'] == indicator) &
            (df['estimate'].notna())
        ]
        
        if country_data.empty:
            return {
                'weighted_score': None,
                'total_weight': 0,
                'data_points': 0,
                'weighted_sum': 0
            }
        
        weighted_sum = 0
        total_weight = 0
        data_points = 0
        
        for _, row in country_data.iterrows():
            weight = self.get_weight(row['year'])
            if weight > 0:
                weighted_sum += row['estimate'] * weight
                total_weight += weight
                data_points += 1
        
        if total_weight > 0:
            return {
                'weighted_score': weighted_sum / total_weight,
                'total_weight': total_weight,
                'weighted_sum': weighted_sum,
                'data_points': data_points
            }
        else:
            return {
                'weighted_score': None,
                'total_weight': 0,
                'weighted_sum': 0,
                'data_points': 0
            }
    
    def normalize_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize scores to 0-1 range using Min-Max scaling
        
        Args:
            df: DataFrame with weighted scores
            
        Returns:
            DataFrame with normalized scores
        """
        logger.info("Normalizing scores to 0-1 range...")
        
        df_normalized = df.copy()
        
        # Get valid overall scores for min-max calculation
        valid_scores = df['overall_weighted_score'].dropna()
        
        if valid_scores.empty:
            logger.warning("No valid scores found for normalization")
            return df_normalized
        
        min_score = valid_scores.min()
        max_score = valid_scores.max()
        score_range = max_score - min_score
        
        logger.info(f"Score range: {min_score:.6f} to {max_score:.6f}")
        
        # Normalize overall scores
        df_normalized['overall_normalized_score'] = df_normalized['overall_weighted_score'].apply(
            lambda x: (x - min_score) / score_range if pd.notna(x) else None
        )
        
        # Normalize individual indicator scores
        for indicator in self.indicators:
            col_name = f'{indicator}_weighted_score'
            normalized_col = f'{indicator}_normalized'
            
            df_normalized[normalized_col] = df_normalized[col_name].apply(
                lambda x: (x - min_score) / score_range if pd.notna(x) else None
            )
        
        # Add normalization metadata
        df_normalized['normalization_min'] = min_score
        df_normalized['normalization_max'] = max_score
        df_normalized['normalization_range'] = score_range
        
        return df_normalized
    
    def save_results_to_mongodb(self, df: pd.DataFrame) -> None:
        """
        Update existing country records in country_scores collection with World Bank data
        
        Args:
            df: DataFrame with analysis results
        """
        logger.info("Updating country_scores collection with World Bank governance data...")
        
        try:
            collection = self.db['country_scores']
            
            # Convert DataFrame to records
            records = df.to_dict('records')
            
            updated_count = 0
            not_found_count = 0
            
            for record in records:
                # Convert numpy types to Python types
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif isinstance(value, (np.integer, np.floating)):
                        record[key] = float(value)
                
                # World Bank'daki 'code' zaten alpha3 kodu (AFG, TUR, USA vs.)
                alpha3_code = record['country_code']  # Bu zaten alpha3
                country_name = record['country_name']
                
                # Manuel alpha3 kod düzeltmeleri
                alpha3_mapping = {
                    'ADO': 'AND',  # Andorra
                    'ZAR': 'COD',  # Congo, Dem. Rep.
                    'JEY': 'JEY',  # Jersey (aynı kalabilir)
                    'KSV': 'XKX',  # Kosovo (geçici ISO kodu)
                    'NER': 'NER',  # Niger (aynı)
                    'PSE': 'PSE',  # Palestine
                    'WBG': 'PSE',  # West Bank and Gaza → Palestine
                    'ROM': 'ROU',  # Romania
                    'TMP': 'TLS',  # Timor-Leste
                    'ZAF': 'ZAF',  # South Africa (aynı)
                    'GBR': 'GBR',  # United Kingdom (aynı)
                }
                
                # Eğer mapping'de varsa düzelt, yoksa orijinal kodu kullan
                corrected_alpha3 = alpha3_mapping.get(alpha3_code, alpha3_code)
                
                logger.debug(f"Looking for alpha3: {alpha3_code} → {corrected_alpha3} ({country_name})")
                
                # Prepare World Bank fields
                worldbank_fields = {
                    'worldbank_overall_normalized_score': record.get('overall_normalized_score'),
                    'worldbank_last_updated': datetime.now(),
                }
                
                # Find by corrected alpha3 field
                existing_record = collection.find_one({'alpha3': corrected_alpha3})
                
                if existing_record:
                    # Update existing record
                    result = collection.update_one(
                        {'_id': existing_record['_id']},
                        {'$set': worldbank_fields}
                    )
                    if result.modified_count > 0:
                        updated_count += 1
                        logger.debug(f"✅ Updated: {country_name} ({alpha3_code} → {corrected_alpha3})")
                else:
                    logger.warning(f"❌ No record found for alpha3: {corrected_alpha3} ({country_name})")
                    not_found_count += 1
            
            logger.info(f"World Bank data update completed:")
            logger.info(f"- Updated existing records: {updated_count}")
            logger.info(f"- Countries not found (skipped): {not_found_count}")
            
            # Show some examples of countries not found
            if not_found_count > 0:
                logger.info("Some alpha3 codes not found in country_scores collection.")
                logger.info("Check if these countries exist in your country_scores:")
                
                not_found_examples = []
                for record in records:
                    alpha3 = record['country_code']
                    
                    # Manuel mapping kontrolü
                    alpha3_mapping = {
                        'ADO': 'AND', 'ZAR': 'COD', 'JEY': 'JEY', 'KSV': 'XKX', 'NER': 'NER',
                        'PSE': 'PSE', 'WBG': 'PSE', 'ROM': 'ROU', 'TMP': 'TLS', 'ZAF': 'ZAF', 'GBR': 'GBR'
                    }
                    corrected_alpha3 = alpha3_mapping.get(alpha3, alpha3)
                    
                    if not collection.find_one({'alpha3': corrected_alpha3}):
                        not_found_examples.append(f"{alpha3} → {corrected_alpha3} ({record['country_name']})")
                        if len(not_found_examples) >= 5:
                            break
                
                for example in not_found_examples:
                    logger.info(f"  Missing: {example}")
            
        except Exception as e:
            logger.error(f"Error updating MongoDB with World Bank data: {e}")
            raise
    
    def save_summary_statistics(self, df: pd.DataFrame) -> None:
        """
        Save summary statistics to MongoDB
        
        Args:
            df: DataFrame with analysis results
        """
        logger.info("Calculating and saving summary statistics...")
        
        try:
            # Calculate statistics
            valid_scores = df['overall_normalized_score'].dropna()
            
            summary_stats = {
                'analysis_date': datetime.now(),
                'total_countries': len(df),
                'countries_with_data': len(valid_scores),
                'score_statistics': {
                    'mean': float(valid_scores.mean()),
                    'median': float(valid_scores.median()),
                    'std': float(valid_scores.std()),
                    'min': float(valid_scores.min()),
                    'max': float(valid_scores.max()),
                    'q25': float(valid_scores.quantile(0.25)),
                    'q75': float(valid_scores.quantile(0.75))
                },
                'top_10_countries': self._get_top_countries(df, 10),
                'bottom_10_countries': self._get_bottom_countries(df, 10),
                'indicator_coverage': self._get_indicator_coverage(df),
                'methodology': {
                    'time_weights': {str(year): self.get_weight(year) 
                                   for year in range(2014, 2024)},
                    'normalization': 'min_max_scaling',
                    'analysis_period': f'{self.latest_year - self.analysis_years + 1}-{self.latest_year}'
                }
            }
            
            # Save to MongoDB
            collection = self.db['governance_summary_statistics']
            collection.delete_many({})  # Clear previous summaries
            collection.insert_one(summary_stats)
            
            logger.info("Summary statistics saved successfully")
            
        except Exception as e:
            logger.error(f"Error saving summary statistics: {e}")
            raise
    
    def _get_top_countries(self, df: pd.DataFrame, n: int) -> List[Dict]:
        """Get top N countries by normalized score"""
        top_countries = df.nlargest(n, 'overall_normalized_score')
        return [
            {
                'rank': i + 1,
                'country_code': row['country_code'],
                'country_name': row['country_name'],
                'score': float(row['overall_normalized_score'])
            }
            for i, (_, row) in enumerate(top_countries.iterrows())
            if pd.notna(row['overall_normalized_score'])
        ]
    
    def _get_bottom_countries(self, df: pd.DataFrame, n: int) -> List[Dict]:
        """Get bottom N countries by normalized score"""
        bottom_countries = df.nsmallest(n, 'overall_normalized_score')
        return [
            {
                'rank': len(df) - i,
                'country_code': row['country_code'],
                'country_name': row['country_name'],
                'score': float(row['overall_normalized_score'])
            }
            for i, (_, row) in enumerate(bottom_countries.iterrows())
            if pd.notna(row['overall_normalized_score'])
        ]
    
    def _get_indicator_coverage(self, df: pd.DataFrame) -> Dict:
        """Calculate indicator coverage statistics"""
        coverage = {}
        for indicator in self.indicators:
            col_name = f'{indicator}_weighted_score'
            valid_count = df[col_name].notna().sum()
            coverage[indicator] = {
                'countries_with_data': int(valid_count),
                'coverage_percentage': float(valid_count / len(df) * 100)
            }
        return coverage
    
    def get_country_ranking(self, country_code: str) -> Dict:
        """
        Get specific country's ranking and details from country_scores collection
        
        Args:
            country_code: Alpha3 country code (e.g., 'TUR', 'USA')
            
        Returns:
            Dictionary with country details
        """
        try:
            collection = self.db['country_scores']
            
            # Simple: Find by alpha3 directly
            country_data = collection.find_one({'alpha3': country_code})
            
            if not country_data:
                return {'error': f'Country with alpha3 code {country_code} not found in country_scores collection'}
            
            if 'worldbank_overall_normalized_score' not in country_data:
                return {'error': f'World Bank data not found for {country_code}'}
            
            # Get ranking among countries with World Bank data
            total_countries = collection.count_documents({
                'worldbank_overall_normalized_score': {'$ne': None}
            })
            better_countries = collection.count_documents({
                'worldbank_overall_normalized_score': {'$gt': country_data['worldbank_overall_normalized_score']}
            })
            
            ranking = better_countries + 1
            
            return {
                'country_code': country_code,
                'country_name': country_data.get('country'),
                'alpha3': country_data.get('alpha3'),
                'worldbank_overall_score': country_data.get('worldbank_overall_normalized_score'),
                'worldbank_total_weight': country_data.get('worldbank_total_weight'),
                'ranking': ranking,
                'total_countries': total_countries,
                'percentile': round((total_countries - ranking + 1) / total_countries * 100, 1),
                'last_updated': country_data.get('worldbank_last_updated')
            }
            
        except Exception as e:
            logger.error(f"Error getting country ranking: {e}")
            return {'error': str(e)}
    
    def run_full_analysis(self) -> Dict:
        """
        Run complete governance analysis pipeline
        
        Returns:
            Dictionary with analysis results summary
        """
        try:
            logger.info("Starting World Bank Governance Analysis...")
            
            # Step 1: Load data
            df = self.load_worldbank_data()
            
            # Step 2: Calculate weighted scores
            df_weighted = self.calculate_weighted_scores(df)
            
            # Step 3: Normalize scores
            df_normalized = self.normalize_scores(df_weighted)
            
            # Step 4: Save results with upsert
            self.save_results_to_mongodb(df_normalized)
            
            # Step 5: Save summary statistics with upsert
            self.save_summary_statistics(df_normalized)
            
            logger.info("Analysis completed successfully!")
            
            return {
                'status': 'success',
                'total_countries_analyzed': len(df_normalized),
                'countries_with_complete_data': len(df_normalized[df_normalized['overall_normalized_score'].notna()]),
                'top_3_countries': self._get_top_countries(df_normalized, 3),
                'bottom_3_countries': self._get_bottom_countries(df_normalized, 3),
                'analysis_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            return {
                'status': 'error',
                'error_message': str(e),
                'analysis_timestamp': datetime.now().isoformat()
            }
    
    def close_connection(self):
        """Close MongoDB connection"""
        self.client.close()
        logger.info("MongoDB connection closed")

def main():
    """
    Main execution function
    """
    # MongoDB connection settings - Updated connection string
    CONNECTION_STRING = "mongodb+srv://bugra:bugraigp@cluster0.edri3gv.mongodb.net/"
    DATABASE_NAME = "gehealthcare"
    
    # Initialize analyzer
    analyzer = GovernanceAnalyzer(CONNECTION_STRING, DATABASE_NAME)
    
    try:
        # Run full analysis
        results = analyzer.run_full_analysis()
        
        # Print results
        print("\n" + "="*60)
        print("WORLD BANK GOVERNANCE ANALYSIS RESULTS")
        print("="*60)
        print(json.dumps(results, indent=2, default=str))
        
        if results['status'] == 'success':
            print(f"\n✅ Analysis completed successfully!")
            print(f"📊 Analyzed {results['total_countries_analyzed']} countries")
            print(f"🏆 Top 3 countries: {[c['country_name'] for c in results['top_3_countries']]}")
            print(f"🔴 Bottom 3 countries: {[c['country_name'] for c in results['bottom_3_countries']]}")
            print(f"💾 Data saved to 'country_scores' collection")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        
    finally:
        # Clean up
        analyzer.close_connection()

if __name__ == "__main__":
    main()