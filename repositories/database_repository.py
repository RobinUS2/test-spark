#!/usr/bin/env python3
"""Database repository for music data operations"""

import logging
import pandas as pd
from typing import Optional, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from config.settings import settings
from models.database_models import Base, MusicRecord, ArtistImage, TrackInfo
from models.data_classes import ArtistCacheEntry, TrackCacheEntry, DatabaseStats

logger = logging.getLogger(__name__)


class DatabaseRepository:
    
    def __init__(self):
        self.engine = None
        self.Session = None
    
    def get_database_engine(self):
        """Get SQLAlchemy database engine with optimizations for large transactions"""
        if self.engine is None:
            if settings.database.type == 'sqlite':
                db_url = f"sqlite:///{settings.database.sqlite_path}"
                # SQLite optimizations for large datasets
                self.engine = create_engine(
                    db_url, 
                    echo=False,
                    connect_args={
                        'timeout': 30,  # Prevent timeout during batch operations
                        'check_same_thread': False
                    },
                    pool_pre_ping=True,
                    pool_recycle=3600
                )
            else:
                db_url = (f"postgresql://{settings.database.username}:{settings.database.password}"
                         f"@{settings.database.host}:{settings.database.port}/{settings.database.database}")
                self.engine = create_engine(
                    db_url, 
                    echo=False,
                    pool_pre_ping=True,
                    pool_recycle=3600,
                )
            
            self.Session = sessionmaker(bind=self.engine)
        
        return self.engine
    
    def init_database(self):
        try:
            engine = self.get_database_engine()
            logger.info("Creating database tables if they don't exist")
            Base.metadata.create_all(engine)
            
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                logger.info("Database initialization and connection test successful")
                
            return engine
        except Exception as e:
            logger.error("Database initialization failed", extra={'error': str(e)})
            raise
    
    def save_dataframe_to_db(self, df_pandas: pd.DataFrame, table_name: str = 'music_records') -> bool:
        if df_pandas.empty:
            logger.warning("Empty DataFrame provided, nothing to save")
            return False
            
        total_records = len(df_pandas)
        logger.info("Starting database save operation", extra={
            'total_records': total_records,
            'table_name': table_name,
            'batch_size': settings.processing.batch_size
        })
        
        try:
            engine = self.get_database_engine()
            
            # Calculate number of batches using configured batch size
            batch_size = settings.processing.batch_size
            num_batches = (total_records + batch_size - 1) // batch_size
            
            logger.info("Batch processing plan", extra={
                'num_batches': num_batches,
                'batch_size': batch_size,
                'estimated_time': f"{num_batches * 2} seconds"  # Rough estimate
            })
            
            saved_count = 0
            
            # Process in batches
            for i in range(0, total_records, batch_size):
                batch_end = min(i + batch_size, total_records)
                batch_df = df_pandas.iloc[i:batch_end]
                batch_num = (i // batch_size) + 1
                
                logger.info("Processing batch", extra={
                    'batch': f"{batch_num}/{num_batches}",
                    'records': f"{len(batch_df)}",
                    'range': f"{i+1}-{batch_end}"
                })
                
                # Save batch to database
                try:
                    batch_df.to_sql(
                        table_name, 
                        engine, 
                        if_exists='append', 
                        index=False,
                        method='multi'  # Use multi-insert for better performance
                    )
                    saved_count += len(batch_df)
                    
                    logger.debug("Batch saved successfully", extra={
                        'batch': batch_num,
                        'saved': len(batch_df),
                        'total_saved': saved_count
                    })
                    
                except Exception as batch_error:
                    logger.error("Batch save failed", extra={
                        'batch': batch_num,
                        'error': str(batch_error),
                        'batch_size': len(batch_df)
                    })
                    # Continue with next batch rather than failing completely
                    continue
            
            success_rate = (saved_count / total_records) * 100
            logger.info("Database save operation completed", extra={
                'total_records': total_records,
                'saved_records': saved_count,
                'success_rate': f"{success_rate:.1f}%"
            })
            
            return saved_count > 0
            
        except Exception as e:
            logger.error("Database save operation failed", extra={
                'error': str(e),
                'table_name': table_name,
                'records_attempted': total_records
            })
            return False
    
    def get_artist_image_from_cache(self, artist_clean_name: str) -> Optional[str]:
        """Get cached artist image URL from database"""
        try:
            session = self.Session()
            artist_image = session.query(ArtistImage).filter_by(
                artist_name=artist_clean_name
            ).first()
            
            if artist_image and artist_image.fetch_success:
                logger.debug("Artist image found in cache", extra={
                    'artist': artist_clean_name,
                    'url': artist_image.image_url[:50] if artist_image.image_url else None
                })
                return artist_image.image_url
            
            return None
            
        except Exception as e:
            logger.warning("Error accessing artist image cache", extra={
                'artist': artist_clean_name,
                'error': str(e)
            })
            return None
        finally:
            if 'session' in locals():
                session.close()
    
    def get_track_info_from_cache(self, artist_clean: str, song_clean: str) -> Optional[Dict[str, Any]]:
        """Get cached track information from database"""
        try:
            session = self.Session()
            track_info = session.query(TrackInfo).filter_by(
                artist_clean=artist_clean,
                song_clean=song_clean
            ).first()
            
            if track_info and track_info.fetch_success:
                logger.debug("Track info found in cache", extra={
                    'artist': artist_clean,
                    'song': song_clean,
                    'duration': track_info.duration
                })
                
                return {
                    'artist_lastfm': track_info.artist_lastfm,
                    'song_lastfm': track_info.song_lastfm,
                    'duration': track_info.duration,
                    'track_mbid': track_info.track_mbid
                }
            
            return None
            
        except Exception as e:
            logger.warning("Error accessing track info cache", extra={
                'artist': artist_clean,
                'song': song_clean,
                'error': str(e)
            })
            return None
        finally:
            if 'session' in locals():
                session.close()
    
    def cache_artist_image(self, entry: ArtistCacheEntry) -> None:
        """Cache artist image result in database using dataclass"""
        try:
            session = self.Session()
            
            # Check if entry already exists
            existing = session.query(ArtistImage).filter_by(
                artist_name=entry.artist_name
            ).first()
            
            if existing:
                # Update existing entry
                existing.image_url = entry.image_url
                existing.mbid = entry.mbid
                existing.artist_lastfm = entry.artist_lastfm
                existing.fetch_success = entry.success
                existing.error_message = entry.error_message
                existing.updated_at = datetime.utcnow()
            else:
                # Create new entry
                artist_image = ArtistImage(
                    artist_name=entry.artist_name,
                    image_url=entry.image_url,
                    mbid=entry.mbid,
                    artist_lastfm=entry.artist_lastfm,
                    fetch_success=entry.success,
                    error_message=entry.error_message
                )
                session.add(artist_image)
            
            session.commit()
            
            logger.debug("Artist image cached", extra={
                'artist': entry.artist_name,
                'success': entry.success,
                'has_url': bool(entry.image_url)
            })
            
        except Exception as e:
            logger.error("Error caching artist image", extra={
                'artist': entry.artist_name,
                'error': str(e)
            })
            if 'session' in locals():
                session.rollback()
        finally:
            if 'session' in locals():
                session.close()
    
    def cache_track_info(self, entry: TrackCacheEntry) -> None:
        """Cache track information result in database using dataclass"""
        try:
            session = self.Session()
            
            # Check if entry already exists
            existing = session.query(TrackInfo).filter_by(
                artist_clean=entry.artist_clean,
                song_clean=entry.song_clean
            ).first()
            
            if existing:
                # Update existing entry
                existing.artist_lastfm = entry.artist_lastfm
                existing.song_lastfm = entry.song_lastfm
                existing.duration = entry.duration
                existing.track_mbid = entry.track_mbid
                existing.fetch_success = entry.success
                existing.error_message = entry.error_message
                existing.updated_at = datetime.utcnow()
            else:
                # Create new entry
                track_info = TrackInfo(
                    artist_clean=entry.artist_clean,
                    song_clean=entry.song_clean,
                    artist_lastfm=entry.artist_lastfm,
                    song_lastfm=entry.song_lastfm,
                    duration=entry.duration,
                    track_mbid=entry.track_mbid,
                    fetch_success=entry.success,
                    error_message=entry.error_message
                )
                session.add(track_info)
            
            session.commit()
            
            logger.debug("Track info cached", extra={
                'artist': entry.artist_clean,
                'song': entry.song_clean,
                'success': entry.success
            })
            
        except Exception as e:
            logger.error("Error caching track info", extra={
                'artist': entry.artist_clean,
                'song': entry.song_clean,
                'error': str(e)
            })
            if 'session' in locals():
                session.rollback()
        finally:
            if 'session' in locals():
                session.close()
    
    def get_database_statistics(self) -> DatabaseStats:
        """Get comprehensive database statistics"""
        try:
            engine = self.get_database_engine()
            
            with engine.connect() as conn:
                # Count music records
                music_count = conn.execute(text("SELECT COUNT(*) FROM music_records")).scalar()
                
                # Count records with parsed unix timestamps  
                unix_count = conn.execute(text("SELECT COUNT(*) FROM music_records WHERE time_unix IS NOT NULL")).scalar()
                
                # Count cached artists
                artist_count = conn.execute(text("SELECT COUNT(*) FROM artist_images")).scalar()
                
                # Count successful images
                success_count = conn.execute(text("SELECT COUNT(*) FROM artist_images WHERE fetch_success = 1")).scalar()
                
                # Count cached tracks
                track_cache_count = conn.execute(text("SELECT COUNT(*) FROM track_info")).scalar()
                
                return DatabaseStats(
                    music_records_count=music_count or 0,
                    records_with_timestamps=unix_count or 0,
                    cached_artists=artist_count or 0,
                    successful_images=success_count or 0,
                    cached_tracks=track_cache_count or 0
                )
                
        except Exception as e:
            logger.error("Error getting database statistics", extra={'error': str(e)})
            return DatabaseStats(0, 0, 0, 0, 0)


# Global repository instance
db_repo = DatabaseRepository()