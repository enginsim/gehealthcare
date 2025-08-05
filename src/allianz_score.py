import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AllianzScoreCalculator:
    def __init__(self, connection_string: str, database_name: str = "gehealthcare"):
        """
        Initialize the Allianz score calculator
        
        Args:
            connection_string (str): MongoDB connection string
            database_name (str): Database name (default: "gehealthcare")
        """
        self.connection_string = connection_string
        self.database_name = database_name
        self.client = None
        self.db = None
        self.source_collection = "allianz_data"
        self.target_collection = "country_scores"
        
    def connect_to_database(self):
        """Establish connection to MongoDB database"""
        try:
            self.client = MongoClient(self.connection_string)
            self.db = self.client[self.database_name]
            logger.info(f"Successfully connected to database: {self.database_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            return False
    
    def load_allianz_data_from_mongo(self):
        """
        Load Allianz data from MongoDB collection
        
        Returns:
            list: Raw Allianz data from MongoDB
        """
        try:
            collection = self.db[self.source_collection]
            data = list(collection.find({}))
            logger.info(f"Successfully loaded {len(data)} records from {self.source_collection} collection")
            return data
        except Exception as e:
            logger.error(f"Failed to load data from {self.source_collection}: {str(e)}")
            return []
    
    def rating_to_numeric(self, rating: str) -> int:
        """
        Convert rating to numeric score
        
        Args:
            rating (str): Credit rating (AA, A, BB1, BB, B, C, D)
            
        Returns:
            int: Numeric score
        """
        rating_map = {
            'AA': 100,
            'A': 85,
            'BB1': 70,
            'BB': 65,
            'B': 50,
            'C': 30,  
            'D': 10
        }
        return rating_map.get(rating, 0)
    
    def calculate_overall_scores(self, data: list) -> dict:
        """
        Calculate overall normalized scores for all countries
        
        Args:
            data (list): Raw Allianz data from MongoDB
            
        Returns:
            dict: Country-wise calculated scores
        """
        # Define weighting system
        weights = {
            '2025Q1': 10,  # Current year - highest weight
            '2024Q4': 8,   # Previous year
            '2024Q3': 8,
            '2024Q2': 8,
            '2024Q1': 8,
            '2023Q4': 6,   # Two years ago
            '2023Q3': 6,
            '2023Q2': 6,
            '2023Q1': 6,
            '2022Q4': 4    # Baseline period
        }
        
        # Group data by country
        country_scores = {}
        
        for record in data:
            country = record.get('country', '')
            alpha3 = record.get('alpha3', '')
            period = record.get('year_quarter', '')
            rating = record.get('medium_term_rating', '')
            risk_level = record.get('risk_level', '')
            short_term_rating = record.get('short_term_rating', '')
            
            numeric_score = self.rating_to_numeric(rating)
            weight = weights.get(period, 0)
            weighted_score = numeric_score * weight
            
            if country not in country_scores:
                country_scores[country] = {
                    'alpha3': alpha3,
                    'country': country,
                    'total_weighted_score': 0,
                    'total_weight': 0,
                    'current_risk_level': risk_level,
                    'current_short_term_rating': short_term_rating,
                    'ratings_history': {}
                }
            
            country_scores[country]['total_weighted_score'] += weighted_score
            country_scores[country]['total_weight'] += weight
            country_scores[country]['ratings_history'][period] = {
                'medium_term_rating': rating,
                'short_term_rating': short_term_rating,
                'risk_level': risk_level,
                'numeric_score': numeric_score,
                'weight': weight,
                'weighted_score': weighted_score
            }
        
        # Calculate final scores
        final_scores = {}
        
        for country, country_data in country_scores.items():
            if country_data['total_weight'] > 0:
                overall_score = round(
                    (country_data['total_weighted_score'] / country_data['total_weight']) * 100
                ) / 100
            else:
                overall_score = 0
            
            # Determine risk category based on score
            if overall_score >= 90:
                category = 'AA'
                risk_description = 'Minimal Risk'
            elif overall_score >= 80:
                category = 'A'
                risk_description = 'Low Risk'
            elif overall_score >= 70:
                category = 'BBB'
                risk_description = 'Moderate Risk'
            elif overall_score >= 60:
                category = 'BB'
                risk_description = 'Acceptable Risk'
            elif overall_score >= 50:
                category = 'B'
                risk_description = 'Moderate-High Risk'
            elif overall_score >= 30:
                category = 'C'
                risk_description = 'High Risk'
            else:
                category = 'D'
                risk_description = 'Very High Risk'
            
            # Get most recent rating (2025Q1 if available, otherwise latest)
            latest_period = max(country_data['ratings_history'].keys()) if country_data['ratings_history'] else None
            latest_rating = country_data['ratings_history'].get(latest_period, {}).get('medium_term_rating', '') if latest_period else ''
            
            final_scores[country] = {
                'alpha3': country_data['alpha3'],
                'allianz_overall_normalized_score': overall_score/100
            }
        
        # Calculate rankings (optional - can be removed if not needed)
        sorted_countries = sorted(final_scores.items(), key=lambda x: x[1]['allianz_overall_normalized_score'], reverse=True)
        
        logger.info(f"Calculated overall scores for {len(final_scores)} countries")
        return final_scores
    
    def update_country_scores_collection(self, allianz_scores: dict) -> bool:
        """
        Update country_scores collection with Allianz data
        
        Args:
            allianz_scores (dict): Calculated Allianz scores by country
            
        Returns:
            bool: Success status
        """
        if self.db is None:
            logger.error("Database connection not established")
            return False
        
        try:
            collection = self.db[self.target_collection]
            
            # Create indexes for efficient queries
            collection.create_index([("country", 1)])
            collection.create_index([("alpha3", 1)])
            collection.create_index([("allianz_overall_normalized_score", -1)])
            
            updated_count = 0
            new_count = 0
            failed_count = 0
            
            for country, allianz_data in allianz_scores.items():
                try:
                    # Find existing document by country name or alpha3
                    existing_doc = collection.find_one({
                        "$or": [
                            {"country": country},
                            {"alpha3": allianz_data['alpha3']}
                        ]
                    })
                    
                    if existing_doc:
                        # Update existing document with Allianz data
                        update_result = collection.update_one(
                            {"_id": existing_doc["_id"]},
                            {"$set": allianz_data}
                        )
                        
                        if update_result.modified_count > 0:
                            updated_count += 1
                            logger.debug(f"Updated Allianz data for: {country}")
                        
                    else:
                        # Create new document if country doesn't exist
                        new_document = {
                            "country": country,
                            **allianz_data
                        }
                        
                        collection.insert_one(new_document)
                        new_count += 1
                        logger.debug(f"Created new country record with Allianz data: {country}")
                        
                except Exception as e:
                    logger.error(f"Failed to process {country}: {str(e)}")
                    failed_count += 1
            
            logger.info(f"Country scores collection update completed:")
            logger.info(f"  - Existing records updated: {updated_count}")
            logger.info(f"  - New records created: {new_count}")
            logger.info(f"  - Failed operations: {failed_count}")
            logger.info(f"  - Total processed: {len(allianz_scores)}")
            logger.info(f"  - Target collection: {self.target_collection}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update country_scores collection: {str(e)}")
            return False
    
    def get_allianz_summary_statistics(self) -> dict:
        """
        Get summary statistics for Allianz data in country_scores collection
        
        Returns:
            dict: Summary statistics
        """
        try:
            collection = self.db[self.target_collection]
            
            # Basic statistics
            pipeline = [
                {
                    "$match": {
                        "allianz_overall_normalized_score": {"$exists": True}
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "total_countries": {"$sum": 1},
                        "average_score": {"$avg": "$allianz_overall_normalized_score"},
                        "max_score": {"$max": "$allianz_overall_normalized_score"},
                        "min_score": {"$min": "$allianz_overall_normalized_score"},
                        "total_weight_sum": {"$sum": "$allianz_total_weight"},
                        "total_weighted_score_sum": {"$sum": "$allianz_total_weighted_score"}
                    }
                }
            ]
            
            stats_result = list(collection.aggregate(pipeline))
            stats = stats_result[0] if stats_result else {}
            
            # Top and bottom performers
            top_performers = list(collection.find(
                {"allianz_overall_normalized_score": {"$exists": True}},
                {"country": 1, "alpha3": 1, "allianz_overall_normalized_score": 1, "allianz_total_weight": 1}
            ).sort("allianz_overall_normalized_score", -1).limit(5))
            
            bottom_performers = list(collection.find(
                {"allianz_overall_normalized_score": {"$exists": True}},
                {"country": 1, "alpha3": 1, "allianz_overall_normalized_score": 1, "allianz_total_weight": 1}
            ).sort("allianz_overall_normalized_score", 1).limit(5))
            
            return {
                "total_countries": stats.get("total_countries", 0),
                "average_score": round(stats.get("average_score", 0), 2),
                "max_score": stats.get("max_score", 0),
                "min_score": stats.get("min_score", 0),
                "total_weight_sum": stats.get("total_weight_sum", 0),
                "total_weighted_score_sum": stats.get("total_weighted_score_sum", 0),
                "top_performers": [
                    {
                        "country": country["country"],
                        "alpha3": country["alpha3"],
                        "score": country["allianz_overall_normalized_score"],
                        "weight": country["allianz_total_weight"]
                    } for country in top_performers
                ],
                "bottom_performers": [
                    {
                        "country": country["country"],
                        "alpha3": country["alpha3"],
                        "score": country["allianz_overall_normalized_score"],
                        "weight": country["allianz_total_weight"]
                    } for country in bottom_performers
                ]
            }
            
        except Exception as e:
            logger.error(f"Failed to get summary statistics: {str(e)}")
            return {}
    
    def close_connection(self):
        """Close database connection"""
        if self.client is not None:
            self.client.close()
            logger.info("Database connection closed")

def main():
    """
    Main function to execute the Allianz score calculation and country_scores update
    """
    # Configuration
    CONNECTION_STRING = "mongodb+srv://bugra:bugraigp@cluster0.edri3gv.mongodb.net/"
    DATABASE_NAME = "gehealthcare"
    
    # Initialize calculator
    calculator = AllianzScoreCalculator(CONNECTION_STRING, DATABASE_NAME)
    
    try:
        # Connect to database
        if not calculator.connect_to_database():
            logger.error("Failed to establish database connection. Exiting.")
            return
        
        # Load data from allianz_data collection
        logger.info("Loading Allianz data from MongoDB...")
        raw_data = calculator.load_allianz_data_from_mongo()
        
        if not raw_data:
            logger.error("No data loaded from allianz_data collection. Exiting.")
            return
        
        # Calculate overall scores
        logger.info("Calculating Allianz overall normalized scores...")
        allianz_scores = calculator.calculate_overall_scores(raw_data)
        
        # Update country_scores collection
        logger.info("Updating country_scores collection with Allianz data...")
        success = calculator.update_country_scores_collection(allianz_scores)
        
        if success:
            # Get and display summary statistics
            logger.info("Getting Allianz summary statistics...")
            stats = calculator.get_allianz_summary_statistics()
            
            logger.info("=" * 80)
            logger.info("ALLIANZ SCORE CALCULATION AND COUNTRY_SCORES UPDATE COMPLETED")
            logger.info("=" * 80)
            logger.info(f"Total Countries Processed: {stats.get('total_countries', 'N/A')}")
            logger.info(f"Average Allianz Score: {stats.get('average_score', 'N/A')}")
            logger.info(f"Highest Score: {stats.get('max_score', 'N/A')}")
            logger.info(f"Lowest Score: {stats.get('min_score', 'N/A')}")
            logger.info(f"Total Weight Sum: {stats.get('total_weight_sum', 'N/A')}")
            logger.info(f"Total Weighted Score Sum: {stats.get('total_weighted_score_sum', 'N/A')}")
            
            logger.info("\nTop 5 Performers:")
            for performer in stats.get('top_performers', []):
                logger.info(f"  {performer['country']} ({performer['alpha3']}): Score={performer['score']}, Weight={performer['weight']}")
            
            logger.info("\nBottom 5 Performers:")
            for performer in stats.get('bottom_performers', []):
                logger.info(f"  {performer['country']} ({performer['alpha3']}): Score={performer['score']}, Weight={performer['weight']}")
            
            logger.info("=" * 80)
            
        else:
            logger.error("Failed to update country_scores collection")
    
    except Exception as e:
        logger.error(f"Unexpected error in main execution: {str(e)}")
    
    finally:
        # Close database connection
        calculator.close_connection()

if __name__ == "__main__":
    main()

# Example usage for specific operations:
"""
# Initialize calculator
calculator = AllianzScoreCalculator("your_connection_string")

# Connect and process
calculator.connect_to_database()
data = calculator.load_allianz_data_from_mongo()
scores = calculator.calculate_overall_scores(data)
calculator.update_country_scores_collection(scores)

# Get statistics
stats = calculator.get_allianz_summary_statistics()
print(f"Updated {stats['total_countries']} countries with Allianz scores")

# Close connection
calculator.close_connection()
"""