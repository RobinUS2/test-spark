#!/usr/bin/env python3
"""
Test script for fuzzy artist name matching
"""

import logging
import sys
sys.path.append('.')

from repositories.database_repository import db_repo
from services.artist_normalization_service import artist_service
from utils import setup_logging

def test_fuzzy_normalization():
    """Test the fuzzy normalization on existing database"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Testing fuzzy artist name normalization")
    
    # Test with different thresholds
    thresholds = [95.0, 90.0, 85.0]
    
    for threshold in thresholds:
        logger.info(f"Testing with threshold: {threshold}")
        
        try:
            normalizations = artist_service.normalize_artist_names_in_db(fuzzy_threshold=threshold)
            logger.info("Normalization results", extra={
                'threshold': threshold,
                'normalizations_found': normalizations
            })
            
            # Show some examples by querying the database
            engine = db_repo.get_database_engine()
            with engine.connect() as conn:
                from sqlalchemy import text
                result = conn.execute(text("""
                    SELECT artist_clean, COUNT(*) as count 
                    FROM music_records 
                    GROUP BY artist_clean 
                    ORDER BY COUNT(*) DESC 
                    LIMIT 10
                """))
                
                logger.info("Top artists after normalization:")
                for artist, count in result.fetchall():
                    logger.info(f"  {artist}: {count} plays")
                    
        except Exception as e:
            logger.error(f"Error with threshold {threshold}: {e}")
        
        print("-" * 50)

if __name__ == "__main__":
    test_fuzzy_normalization()