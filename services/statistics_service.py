#!/usr/bin/env python3
"""
Statistics and reporting service for music data analysis

This module provides comprehensive statistics and reporting capabilities
for the music database.
"""

import logging
from typing import List, Tuple, Dict, Any
from sqlalchemy import text

from repositories.database_repository import db_repo
from models.data_classes import DatabaseStats

logger = logging.getLogger(__name__)


class StatisticsService:
    """Service for generating database statistics and reports"""
    
    def print_database_statistics(self) -> None:
        """Print comprehensive database statistics with detailed breakdowns"""
        logger.info("Database Statistics")
        
        # Get basic stats using repository
        stats = db_repo.get_database_statistics()
        
        # Log basic statistics
        logger.info("Music records in database", extra={'count': stats.music_records_count})
        logger.info("Records with parsed timestamps", extra={'count': stats.records_with_timestamps})
        logger.info("Cached artists", extra={'count': stats.cached_artists})
        logger.info("Successful image fetches", extra={'count': stats.successful_images})
        logger.info("Cached tracks", extra={'count': stats.cached_tracks})
        
        # Get detailed statistics
        self._print_detailed_statistics()
    
    def _print_detailed_statistics(self) -> None:
        """Print detailed statistics with advanced queries"""
        engine = db_repo.get_database_engine()
        
        try:
            with engine.connect() as conn:
                # Count tracks with duration
                duration_count = conn.execute(text(
                    "SELECT COUNT(*) FROM track_info WHERE duration IS NOT NULL"
                )).scalar()
                logger.info("Tracks with duration", extra={'count': duration_count})
                
                # Count records with track duration
                records_duration_count = conn.execute(text(
                    "SELECT COUNT(*) FROM music_records WHERE track_duration IS NOT NULL"
                )).scalar()
                logger.info("Records with track duration", extra={'count': records_duration_count})
                
                # Show sample cached artists
                self._show_cached_artists_sample(conn)
                
                # Show sample tracks with duration
                track_cache_count = conn.execute(text("SELECT COUNT(*) FROM track_info")).scalar()
                if track_cache_count > 0:
                    self._show_tracks_sample(conn)
                
                # Show top artists statistics
                self._show_top_artists_by_listening_time(conn)
                self._show_bottom_artists_by_plays(conn)
                
        except Exception as e:
            logger.error("Error generating detailed statistics", extra={'error': str(e)})
    
    def _show_cached_artists_sample(self, conn) -> None:
        """Show sample of cached artists"""
        cached_artists = conn.execute(text("""
            SELECT artist_name, 
                   CASE WHEN fetch_success = 1 THEN 'Success' ELSE 'Failed' END as status,
                   SUBSTR(image_url, 1, 50) || '...' as image_preview
            FROM artist_images 
            LIMIT 5
        """)).fetchall()
        
        for artist in cached_artists:
            logger.debug("Cached artist sample", extra={
                'artist': artist[0], 'status': artist[1], 'image_preview': artist[2]
            })
    
    def _show_tracks_sample(self, conn) -> None:
        """Show sample tracks with duration"""
        logger.debug("Sample tracks with duration")
        cached_tracks = conn.execute(text("""
            SELECT artist_clean, song_clean, duration,
                   SUBSTR(track_mbid, 1, 8) || '...' as mbid_preview
            FROM track_info 
            WHERE duration > 0
            ORDER BY RANDOM()
            LIMIT 5
        """)).fetchall()
        
        for track in cached_tracks:
            artist_clean, song_clean, duration, mbid_preview = track
            duration_str = f"{int(duration // 60)}:{int(duration % 60):02d}" if duration else "N/A"
            logger.debug("Track with duration", extra={
                'artist': artist_clean, 'song': song_clean,
                'duration': duration_str, 'mbid_preview': mbid_preview
            })
    
    def _show_top_artists_by_listening_time(self, conn) -> None:
        """Show top 10 artists by cumulative listening time"""
        logger.info("Top 10 Artists by Cumulative Listening Time")
        top_artists = conn.execute(text("""
            SELECT 
                COALESCE(mr.artist_lastfm, mr.artist_clean) as display_name,
                MIN(mr.artist_clean) as artist_clean,
                MIN(mr.artist_lastfm) as artist_lastfm,
                COUNT(*) as play_count,
                AVG(mr.track_duration) as avg_duration_seconds,
                SUM(mr.track_duration) as total_listening_seconds,
                CASE 
                    WHEN SUM(mr.track_duration) >= 3600 THEN 
                        CAST(SUM(mr.track_duration) / 3600 AS TEXT) || 'h ' || 
                        CAST((SUM(mr.track_duration) % 3600) / 60 AS TEXT) || 'm'
                    WHEN SUM(mr.track_duration) >= 60 THEN 
                        CAST(SUM(mr.track_duration) / 60 AS TEXT) || 'm ' ||
                        CAST(SUM(mr.track_duration) % 60 AS TEXT) || 's'
                    ELSE 
                        CAST(SUM(mr.track_duration) AS TEXT) || 's'
                END as total_listening_time
            FROM music_records mr
            WHERE mr.track_duration IS NOT NULL 
              AND mr.track_duration > 0
              AND mr.artist_clean IS NOT NULL
            GROUP BY COALESCE(mr.artist_lastfm, mr.artist_clean)
            ORDER BY SUM(mr.track_duration) DESC
            LIMIT 10
        """)).fetchall()
        
        if top_artists:
            for i, (display_name, artist_clean, artist_lastfm, plays, avg_dur, total_sec, total_time) in enumerate(top_artists, 1):
                avg_duration_str = f"{int(avg_dur // 60)}:{int(avg_dur % 60):02d}" if avg_dur else "N/A"
                logger.info("Top artist by listening time", extra={
                    'rank': i, 'artist_display': display_name, 'artist_clean': artist_clean,
                    'artist_lastfm': artist_lastfm, 'plays': plays,
                    'avg_duration': avg_duration_str, 'total_time': total_time
                })
        else:
            logger.warning("No artists found with duration data for listening time analysis")
    
    def _show_bottom_artists_by_plays(self, conn) -> None:
        """Show bottom 10 artists by play count"""
        logger.info("Bottom 10 Artists by Play Count")
        bottom_artists = conn.execute(text("""
            SELECT 
                COALESCE(mr.artist_lastfm, mr.artist_clean) as display_name,
                MIN(mr.artist_clean) as artist_clean,
                MIN(mr.artist_lastfm) as artist_lastfm,
                COUNT(*) as play_count
            FROM music_records mr
            WHERE mr.artist_clean IS NOT NULL
            GROUP BY COALESCE(mr.artist_lastfm, mr.artist_clean)
            ORDER BY COUNT(*) ASC
            LIMIT 10
        """)).fetchall()
        
        if bottom_artists:
            for i, (display_name, artist_clean, artist_lastfm, plays) in enumerate(bottom_artists, 1):
                logger.info("Bottom artist by plays", extra={
                    'rank': i, 'artist_display': display_name, 'artist_clean': artist_clean,
                    'artist_lastfm': artist_lastfm, 'plays': plays
                })
        else:
            logger.warning("No artists found for bottom plays analysis")
    
    def demonstrate_pandas_integration(self) -> None:
        """Demonstrate pandas integration with database (keeping as demo function)"""
        logger.info("Demonstrating pandas integration")
        
        try:
            import pandas as pd
            engine = db_repo.get_database_engine()
            
            # Read data into pandas DataFrame
            df = pd.read_sql_query("""
                SELECT artist_clean, COUNT(*) as play_count
                FROM music_records 
                WHERE artist_clean IS NOT NULL
                GROUP BY artist_clean
                ORDER BY play_count DESC
                LIMIT 10
            """, engine)
            
            logger.info("Pandas DataFrame shape", extra={'shape': str(df.shape)})
            logger.info("Top artists from pandas", extra={'artists': df.to_dict('records')})
            
        except ImportError:
            logger.warning("Pandas not available for integration demo")
        except Exception as e:
            logger.error("Error in pandas integration demo", extra={'error': str(e)})


# Global service instance
stats_service = StatisticsService()