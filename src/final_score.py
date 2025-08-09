#!/usr/bin/env python3
"""
MongoDB Aggregate Pipeline ile Overall Final Score Hesaplama
GE Healthcare - Country Risk Assessment System

Bu script MongoDB aggregate pipeline kullanarak daha verimli bir şekilde
overall_final_score hesaplar ve detaylı istatistikler sunar.
"""

import pymongo
from datetime import datetime
import json
from typing import List, Dict

class MongoAggregateScorer:
    
    def __init__(self, mongo_uri: str, db_name: str = "gehealthcare"):
        """MongoDB bağlantısı kur"""
        self.client = pymongo.MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db.country_scores
        
        # Ağırlık tanımları
        self.weights = {
            'allianz': 0.35,
            'worldbank': 0.20,
            'countryeconomics': 0.35,
            'oecd': 0.10
        }
        
        print("✅ MongoDB Aggregate Scorer initialized")
    
    def create_aggregate_pipeline(self) -> List[Dict]:
        """Overall final score hesaplama için aggregate pipeline oluştur"""
        
        pipeline = [
            # Stage 1: Skorları ve mevcut veri sayısını hesapla
            {
                '$addFields': {
                    'available_scores': {
                        '$let': {
                            'vars': {
                                'allianz_valid': {
                                    '$cond': [
                                        {'$and': [
                                            {'$ne': ['$allianz_overall_normalized_score', None]},
                                            {'$gte': ['$allianz_overall_normalized_score', 0]}
                                        ]},
                                        '$allianz_overall_normalized_score',
                                        0
                                    ]
                                },
                                'worldbank_valid': {
                                    '$cond': [
                                        {'$and': [
                                            {'$ne': ['$worldbank_overall_normalized_score', None]},
                                            {'$gte': ['$worldbank_overall_normalized_score', 0]}
                                        ]},
                                        '$worldbank_overall_normalized_score',
                                        0
                                    ]
                                },
                                'countryeconomics_valid': {
                                    '$cond': [
                                        {'$and': [
                                            {'$ne': ['$countryeconomics_overall_normalized_score', None]},
                                            {'$gte': ['$countryeconomics_overall_normalized_score', 0]}
                                        ]},
                                        '$countryeconomics_overall_normalized_score',
                                        0
                                    ]
                                },
                                'oecd_valid': {
                                    '$cond': [
                                        {'$and': [
                                            {'$ne': ['$oecd_overall_normalized_score', None]},
                                            {'$gte': ['$oecd_overall_normalized_score', 0]}
                                        ]},
                                        '$oecd_overall_normalized_score',
                                        0
                                    ]
                                }
                            },
                            'in': {
                                'allianz': '$$allianz_valid',
                                'worldbank': '$$worldbank_valid', 
                                'countryeconomics': '$$countryeconomics_valid',
                                'oecd': '$$oecd_valid'
                            }
                        }
                    }
                }
            },
            
            # Stage 2: Geçerli skor sayısı ve ağırlıklı toplamı hesapla
            {
                '$addFields': {
                    'valid_score_count': {
                        '$add': [
                            {'$cond': [{'$gt': ['$available_scores.allianz', 0]}, 1, 0]},
                            {'$cond': [{'$gt': ['$available_scores.worldbank', 0]}, 1, 0]},
                            {'$cond': [{'$gt': ['$available_scores.countryeconomics', 0]}, 1, 0]},
                            {'$cond': [{'$gt': ['$available_scores.oecd', 0]}, 1, 0]}
                        ]
                    },
                    'weighted_sum': {
                        '$add': [
                            {'$multiply': ['$available_scores.allianz', self.weights['allianz']]},
                            {'$multiply': ['$available_scores.worldbank', self.weights['worldbank']]},
                            {'$multiply': ['$available_scores.countryeconomics', self.weights['countryeconomics']]},
                            {'$multiply': ['$available_scores.oecd', self.weights['oecd']]}
                        ]
                    },
                    'total_weight': {
                        '$add': [
                            {'$cond': [{'$gt': ['$available_scores.allianz', 0]}, self.weights['allianz'], 0]},
                            {'$cond': [{'$gt': ['$available_scores.worldbank', 0]}, self.weights['worldbank'], 0]},
                            {'$cond': [{'$gt': ['$available_scores.countryeconomics', 0]}, self.weights['countryeconomics'], 0]},
                            {'$cond': [{'$gt': ['$available_scores.oecd', 0]}, self.weights['oecd'], 0]}
                        ]
                    }
                }
            },
            
            # Stage 3: Overall final score hesapla
            {
                '$addFields': {
                    'overall_final_score': {
                        '$cond': [
                            {'$gt': ['$total_weight', 0]},
                            {'$divide': ['$weighted_sum', '$total_weight']},
                            None
                        ]
                    },
                    'data_availability_percentage': {
                        '$multiply': [
                            {'$divide': ['$valid_score_count', 4]},
                            100
                        ]
                    }
                }
            },
            
            # Stage 4: Sonuçları formatla
            {
                '$addFields': {
                    'overall_final_score_rounded': {
                        '$cond': [
                            {'$ne': ['$overall_final_score', None]},
                            {'$round': ['$overall_final_score', 4]},
                            None
                        ]
                    },
                    'calculation_metadata': {
                        'calculation_date': datetime.now(),
                        'weights_used': self.weights,
                        'data_sources_available': {
                            'allianz': {'$gt': ['$available_scores.allianz', 0]},
                            'worldbank': {'$gt': ['$available_scores.worldbank', 0]},
                            'countryeconomics': {'$gt': ['$available_scores.countryeconomics', 0]},
                            'oecd': {'$gt': ['$available_scores.oecd', 0]}
                        }
                    }
                }
            }
        ]
        
        return pipeline
    
    def calculate_and_update_scores(self) -> Dict:
        """Calculate and update the scores using the aggregate pipeline"""
        
        print("\n🔄 Running aggregate pipeline to calculate overall final scores...")
        
        pipeline = self.create_aggregate_pipeline()
        
        # Aggregate pipelineı çalıştır
        results = list(self.collection.aggregate(pipeline))
        
        print(f"📊 Processed {len(results)} countries")
        
        # Sonuçları güncelle
        updated_count = 0
        no_data_count = 0
        error_count = 0
        
        for result in results:
            try:
                country = result.get('country', 'Unknown')
                alpha3 = result.get('alpha3', 'UNK')
                overall_score = result.get('overall_final_score_rounded')
                data_availability = result.get('data_availability_percentage', 0)
                
                if overall_score is not None:
                    # Update document
                    update_doc = {
                        '$set': {
                            'overall_final_score': overall_score,
                            'overall_final_score_calculation_date': datetime.now(),
                            'overall_final_score_data_availability': round(data_availability, 1),
                            'overall_final_score_metadata': {
                                'calculation_method': 'mongodb_aggregate',
                                'weights_used': self.weights,
                                'valid_score_count': result.get('valid_score_count', 0),
                                'total_possible_sources': 4,
                                'component_scores': result.get('available_scores', {}),
                                'weighted_sum': result.get('weighted_sum', 0),
                                'total_weight': result.get('total_weight', 0)
                            }
                        }
                    }
                    
                    self.collection.update_one(
                        {'_id': result['_id']},
                        update_doc
                    )
                    
                    print(f"✅ {alpha3:3s}: {overall_score:.4f} ({data_availability:4.1f}% data)")
                    updated_count += 1
                else:
                    print(f"⚠️ {alpha3:3s}: No valid data")
                    no_data_count += 1
                    
            except Exception as e:
                print(f"❌ Error updating {result.get('alpha3', 'Unknown')}: {e}")
                error_count += 1
        
        return {
            'total_processed': len(results),
            'updated_count': updated_count,
            'no_data_count': no_data_count,
            'error_count': error_count
        }
    
    def generate_statistics_report(self) -> Dict:
        """Detaylı istatistik raporu oluştur"""
        
        print("\n📊 Generating detailed statistics report...")
        
        # Aggregate pipeline for statistics
        stats_pipeline = [
            {
                '$match': {
                    'overall_final_score': {'$ne': None, '$exists': True}
                }
            },
            {
                '$group': {
                    '_id': None,
                    'total_countries': {'$sum': 1},
                    'avg_score': {'$avg': '$overall_final_score'},
                    'min_score': {'$min': '$overall_final_score'},
                    'max_score': {'$max': '$overall_final_score'},
                    'avg_data_availability': {'$avg': '$overall_final_score_data_availability'},
                    'complete_data_count': {
                        '$sum': {
                            '$cond': [
                                {'$eq': ['$overall_final_score_data_availability', 100]},
                                1, 0
                            ]
                        }
                    }
                }
            }
        ]
        
        stats = list(self.collection.aggregate(stats_pipeline))
        
        if stats:
            stat = stats[0]
            print(f"\n📈 OVERALL STATISTICS:")
            print(f"Total countries with scores: {stat['total_countries']}")
            print(f"Average final score: {stat['avg_score']:.4f}")
            print(f"Score range: {stat['min_score']:.4f} - {stat['max_score']:.4f}")
            print(f"Average data availability: {stat['avg_data_availability']:.1f}%")
            print(f"Countries with 100% data: {stat['complete_data_count']}")
            
            return stat
        
        return {}
    
    def get_top_countries(self, limit: int = 10) -> List[Dict]:
        """En yüksek skorlu ülkeleri getir"""
        
        pipeline = [
            {
                '$match': {
                    'overall_final_score': {'$ne': None, '$exists': True}
                }
            },
            {
                '$sort': {'overall_final_score': -1}
            },
            {
                '$limit': limit
            },
            {
                '$project': {
                    'country': 1,
                    'alpha3': 1,
                    'overall_final_score': 1,
                    'overall_final_score_data_availability': 1,
                    'allianz_overall_normalized_score': 1,
                    'worldbank_overall_normalized_score': 1,
                    'countryeconomics_overall_normalized_score': 1,
                    'oecd_overall_normalized_score': 1
                }
            }
        ]
        
        return list(self.collection.aggregate(pipeline))
    
    def analyze_data_completeness(self) -> Dict:
        """Veri eksiksizliği analizi"""
        
        print("\n🔍 Analyzing data completeness...")
        
        pipeline = [
            {
                '$project': {
                    'country': 1,
                    'alpha3': 1,
                    'has_allianz': {
                        '$cond': [
                            {'$and': [
                                {'$ne': ['$allianz_overall_normalized_score', None]},
                                {'$gte': ['$allianz_overall_normalized_score', 0]}
                            ]},
                            1, 0
                        ]
                    },
                    'has_worldbank': {
                        '$cond': [
                            {'$and': [
                                {'$ne': ['$worldbank_overall_normalized_score', None]},
                                {'$gte': ['$worldbank_overall_normalized_score', 0]}
                            ]},
                            1, 0
                        ]
                    },
                    'has_countryeconomics': {
                        '$cond': [
                            {'$and': [
                                {'$ne': ['$countryeconomics_overall_normalized_score', None]},
                                {'$gte': ['$countryeconomics_overall_normalized_score', 0]}
                            ]},
                            1, 0
                        ]
                    },
                    'has_oecd': {
                        '$cond': [
                            {'$and': [
                                {'$ne': ['$oecd_overall_normalized_score', None]},
                                {'$gte': ['$oecd_overall_normalized_score', 0]}
                            ]},
                            1, 0
                        ]
                    }
                }
            },
            {
                '$group': {
                    '_id': None,
                    'total_countries': {'$sum': 1},
                    'allianz_count': {'$sum': '$has_allianz'},
                    'worldbank_count': {'$sum': '$has_worldbank'},
                    'countryeconomics_count': {'$sum': '$has_countryeconomics'},
                    'oecd_count': {'$sum': '$has_oecd'}
                }
            }
        ]
        
        result = list(self.collection.aggregate(pipeline))
        
        if result:
            data = result[0]
            total = data['total_countries']
            
            print(f"📊 DATA SOURCE AVAILABILITY:")
            print(f"Allianz: {data['allianz_count']}/{total} ({data['allianz_count']/total*100:.1f}%)")
            print(f"WorldBank: {data['worldbank_count']}/{total} ({data['worldbank_count']/total*100:.1f}%)")
            print(f"CountryEconomics: {data['countryeconomics_count']}/{total} ({data['countryeconomics_count']/total*100:.1f}%)")
            print(f"OECD: {data['oecd_count']}/{total} ({data['oecd_count']/total*100:.1f}%)")
            
            return data
        
        return {}

def main():
    """Ana fonksiyon"""
    
    MONGO_URI = "mongodb+srv://bugra:bugraigp@cluster0.edri3gv.mongodb.net/"
    
    try:
        print("🚀 MongoDB Aggregate Score Calculator Starting...")
        
        # Scorerı başlat
        scorer = MongoAggregateScorer(MONGO_URI)
        
        # Skorları hesapla ve güncelle
        update_results = scorer.calculate_and_update_scores()
        
        print("\n" + "="*60)
        print("📈 UPDATE SUMMARY")
        print("="*60)
        print(f"✅ Successfully updated: {update_results['updated_count']}")
        print(f"⚠️ No valid data: {update_results['no_data_count']}")
        print(f"❌ Errors: {update_results['error_count']}")
        print(f"📊 Total processed: {update_results['total_processed']}")
        
        # İstatistikleri oluştur
        scorer.generate_statistics_report()
        
        # Veri eksiksizliği analizi
        scorer.analyze_data_completeness()
        
        # Top 10 ülkeler
        print(f"\n🏆 TOP 10 COUNTRIES:")
        print("-" * 70)
        top_countries = scorer.get_top_countries(10)
        
        for i, country in enumerate(top_countries, 1):
            score = country.get('overall_final_score', 0)
            data_avail = country.get('overall_final_score_data_availability', 0)
            alpha3 = country.get('alpha3', 'UNK')
            name = country.get('country', 'Unknown')
            
            print(f"{i:2d}. {alpha3:3s}: {score:.4f} ({data_avail:4.1f}% data) - {name}")
        
        print(f"\n✅ MongoDB Aggregate Score calculation completed!")
        
    except Exception as e:
        print(f"❌ Critical error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        print("🔌 Closing MongoDB connection...")

if __name__ == "__main__":
    main()
