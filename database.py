#!/usr/bin/env python3
"""
Database models, operations, and statistics for the music data pipeline
"""

import os
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text, Column, Integer, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

try:
    from rapidfuzz import fuzz, process
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False
    logging.warning("rapidfuzz not available - fuzzy matching disabled")

logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'type': 'sqlite',  # Change to 'postgresql' for postgres
    'sqlite_path': './music_data.db',
    # PostgreSQL config (uncomment when switching):
    # 'host': os.getenv('DB_HOST', 'localhost'),
    # 'port': os.getenv('DB_PORT', 5432),
    # 'database': os.getenv('DB_NAME', 'music_data'),
    # 'username': os.getenv('DB_USER', 'postgres'),
    # 'password': os.getenv('DB_PASSWORD', ''),
}

# SQLAlchemy Base
Base = declarative_base()

# Database Models
class MusicRecord(Base):
    """SQLAlchemy model for music records"""
    __tablename__ = 'music_records'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    raw_song = Column(String(500))
    raw_artist = Column(String(200))
    song_clean = Column(String(500))  # Cleaned version of song title
    artist_clean = Column(String(200))  # Cleaned version of artist name
    artist_lastfm = Column(String(200))  # Canonical artist name from Last.fm API
    song_lastfm = Column(String(500))  # Canonical song name from Last.fm API
    callsign = Column(String(50))
    time = Column(String(50))  # Store as string initially, can convert to DateTime later, probably unix timestamp
    time_unix = Column(Integer)  # Parsed unix timestamp in seconds
    unique_id = Column(String(100))
    combined = Column(String(700))
    first_play = Column(String(10))
    artist_image_url = Column(String(1000))
    artist_mbid = Column(String(100))  # MusicBrainz ID from Last.fm
    track_duration = Column(Integer)  # Track duration in seconds from Last.fm
    track_mbid = Column(String(100))  # Track MusicBrainz ID from Last.fm
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class ArtistImage(Base):
    """SQLAlchemy model for artist image cache"""
    __tablename__ = 'artist_images'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    artist_name = Column(String(200), unique=True, index=True)  # Cache key (cleaned name)
    artist_lastfm = Column(String(200))  # Canonical artist name from Last.fm
    image_url = Column(String(1000))
    mbid = Column(String(100))  # MusicBrainz ID
    fetch_success = Column(Boolean, default=True)
    error_message = Column(String(500))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class TrackInfo(Base):
    """SQLAlchemy model for track information cache"""
    __tablename__ = 'track_info'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    artist_clean = Column(String(200), index=True)  # Cache key (cleaned name)
    song_clean = Column(String(500), index=True)    # Cache key (cleaned name)
    artist_lastfm = Column(String(200))  # Canonical artist name from Last.fm
    song_lastfm = Column(String(500))    # Canonical song name from Last.fm
    duration = Column(Integer)  # Duration in seconds
    track_mbid = Column(String(100))  # Track MusicBrainz ID
    fetch_success = Column(Boolean, default=True)
    error_message = Column(String(500))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


def get_database_engine():
    """Get SQLAlchemy database engine with optimizations for large transactions"""
    if DB_CONFIG['type'] == 'sqlite':
        db_url = f"sqlite:///{DB_CONFIG['sqlite_path']}"
        # SQLite optimizations for large datasets
        engine = create_engine(
            db_url, 
            echo=False,
            connect_args={
                'timeout': 30,  # 30 second timeout for database locks
                'check_same_thread': False  # Allow sharing across threads
            },
            pool_pre_ping=True,  # Verify connections before use
            pool_recycle=3600,  # Recycle connections after 1 hour
        )
    else:
        # PostgreSQL connection
        db_url = f"postgresql://{DB_CONFIG['username']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(
            db_url, 
            echo=False,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
    
    return engine


def init_database():
    """Initialize database and create tables"""
    try:
        engine = get_database_engine()
        logger.info("Creating database tables if they don't exist")
        Base.metadata.create_all(engine)
        
        # Test that we can connect and query
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            logger.info("Database initialization and connection test successful")
            
        return engine
    except Exception as e:
        logger.error("Database initialization failed", extra={'error': str(e)})
        raise


def find_fuzzy_artist_matches(artists: List[Tuple[str, int]], threshold: float = 90.0) -> Dict[str, str]:
    """
    Find fuzzy matches between artist names using rapidfuzz.
    
    Args:
        artists: List of tuples (artist_name, play_count)
        threshold: Minimum similarity score to consider a match
        
    Returns:
        Dict mapping artist names to their preferred canonical form
    """
    if not RAPIDFUZZ_AVAILABLE:
        logger.warning("rapidfuzz not available - skipping fuzzy matching")
        return {}
    
    normalizations = {}
    processed = set()
    
    # Sort by play count (descending) so popular artists become canonical
    sorted_artists = sorted(artists, key=lambda x: x[1], reverse=True)
    
    for i, (artist, count) in enumerate(sorted_artists):
        if artist.lower() in processed:
            continue
            
        # Find similar artists
        matches = []
        for j, (other_artist, other_count) in enumerate(sorted_artists):
            if i == j or other_artist.lower() in processed:
                continue
                
            # Calculate similarity
            similarity = fuzz.ratio(artist.lower(), other_artist.lower())
            
            if similarity >= threshold:
                matches.append((other_artist, other_count, similarity))
        
        if matches:
            # This artist has fuzzy matches
            all_variants = [(artist, count)] + matches
            
            # Choose canonical form using smart rules
            canonical = choose_canonical_artist_name(all_variants)
            
            # Mark all variants for normalization to canonical form
            for variant in all_variants:
                variant_name = variant[0]
                if variant_name.lower() != canonical.lower():
                    normalizations[variant_name.lower()] = canonical
                processed.add(variant_name.lower())
            
            processed.add(canonical.lower())
            
            logger.info("Fuzzy match group found", extra={
                'canonical': canonical,
                'variants': [v[0] for v in all_variants],
                'similarities': [v[2] if len(v) > 2 else 100.0 for v in all_variants]
            })
    
    return normalizations


def choose_canonical_artist_name(variants: List[Tuple]) -> str:
    """
    Choose the canonical form from a list of artist name variants.
    
    Args:
        variants: List of tuples (name, count, similarity_score?) - similarity_score is optional
        
    Returns:
        The preferred canonical name
    """
    # Well-known bands that should have "The" prefix
    well_known_the_bands = {
        'rolling stones', 'beatles', 'who', 'police', 'doors', 
        'cars', 'guess who', 'doobie brothers', 'black crowes',
        'eagles', 'kinks', 'clash', 'cure', 'smiths'
    }
    
    # Check if any variant should have "The" prefix
    for variant in variants:
        name = variant[0]
        count = variant[1]
        # Handle both 2-tuple (name, count) and 3-tuple (name, count, similarity) cases
        
        base_name = name.lower().replace('the ', '', 1) if name.lower().startswith('the ') else name.lower()
        if base_name in well_known_the_bands:
            # Find the "The" version if it exists, otherwise create it
            for other_variant in variants:
                variant_name = other_variant[0]
                if variant_name.lower().startswith('the ') and variant_name.lower().replace('the ', '', 1) == base_name:
                    return variant_name
            # Create "The" version
            return f"The {name.title()}" if not name.lower().startswith('the ') else name
    
    # No special "The" handling needed, use the version with most plays
    return max(variants, key=lambda x: x[1])[0]


def normalize_artist_names_in_db(fuzzy_threshold: float = 90.0):
    """
    Normalize artist names by checking if 'The' versions exist and using fuzzy matching.
    
    Logic:
    1. Find all artist_clean names and get their play counts
    2. Use fuzzy matching to find similar artist names that might be duplicates
    3. Handle "The" prefix variants with smart rules
    4. Update all records to use the preferred version consistently
    
    Args:
        fuzzy_threshold (float): Minimum similarity score (0-100) to consider artists as matches
    """
    engine = get_database_engine()
    
    try:
        with engine.connect() as conn:
            logger.info("Starting enhanced artist name normalization", extra={
                'fuzzy_threshold': fuzzy_threshold,
                'fuzzy_available': RAPIDFUZZ_AVAILABLE
            })
            
            # Get all distinct artist_clean names with counts
            result = conn.execute(text("""
                SELECT DISTINCT artist_clean, COUNT(*) as count 
                FROM music_records 
                WHERE artist_clean IS NOT NULL AND artist_clean != ''
                GROUP BY artist_clean
                ORDER BY COUNT(*) DESC
            """))
            
            artists = [(row[0], row[1]) for row in result.fetchall()]
            logger.info("Found artists to analyze", extra={'count': len(artists)})
            
            # Step 1: Handle "The" prefix normalizations (exact matches)
            the_normalizations = handle_the_prefix_variants(artists)
            
            # Step 2: Apply fuzzy matching for remaining artists
            fuzzy_normalizations = {}
            if RAPIDFUZZ_AVAILABLE and fuzzy_threshold > 0:
                # Remove artists already handled by "The" logic
                remaining_artists = []
                processed_names = set()
                
                for name, count in artists:
                    # Check if this artist was already processed in "The" normalization
                    already_processed = False
                    for old_name, new_name in the_normalizations.items():
                        if name.lower() == old_name or name.lower() == new_name.lower():
                            already_processed = True
                            processed_names.add(name.lower())
                            break
                    
                    if not already_processed:
                        remaining_artists.append((name, count))
                
                logger.info("Running fuzzy matching", extra={
                    'remaining_artists': len(remaining_artists),
                    'threshold': fuzzy_threshold
                })
                
                fuzzy_normalizations = find_fuzzy_artist_matches(remaining_artists, fuzzy_threshold)
            
            # Combine all normalizations
            all_normalizations = {**the_normalizations, **fuzzy_normalizations}
            
            # Step 3: Apply all normalizations to database
            total_updated = apply_artist_normalizations(conn, all_normalizations)
            
            conn.commit()
            
            logger.info("Artist name normalization completed", extra={
                'the_normalizations': len(the_normalizations),
                'fuzzy_normalizations': len(fuzzy_normalizations),
                'total_normalizations': len(all_normalizations),
                'total_records_updated': total_updated
            })
            
            return len(all_normalizations)
            
    except Exception as e:
        logger.error("Artist name normalization failed", extra={'error': str(e)})
        return 0


def handle_the_prefix_variants(artists: List[Tuple[str, int]]) -> Dict[str, str]:
    """Handle exact 'The' prefix variants (e.g., 'Beatles' vs 'The Beatles')"""
    normalizations = {}
    artist_dict = {name.lower(): (name, count) for name, count in artists}
    
    for artist_name, count in artists:
        if artist_name.lower().startswith('the '):
            base_name = artist_name[4:].strip()
            
            # Check if non-"The" version exists
            if base_name.lower() in artist_dict:
                base_count = artist_dict[base_name.lower()][1]
                
                # Apply "The" prefix rules
                preferred = choose_the_prefix_version(artist_name, count, base_name, base_count)
                to_update = base_name if preferred == artist_name else artist_name
                
                if to_update.lower() not in normalizations:
                    normalizations[to_update.lower()] = preferred
                    logger.info("The prefix normalization", extra={
                        'preferred': preferred, 'to_update': to_update,
                        'the_count': count, 'base_count': base_count
                    })
    
    return normalizations


def choose_the_prefix_version(the_name: str, the_count: int, base_name: str, base_count: int) -> str:
    """Choose between 'The Artist' and 'Artist' versions based on smart rules"""
    well_known_the_bands = {
        'rolling stones', 'beatles', 'who', 'police', 'doors', 
        'cars', 'guess who', 'doobie brothers', 'black crowes',
        'eagles', 'kinks', 'clash', 'cure', 'smiths'
    }
    
    if base_name.lower() in well_known_the_bands:
        return the_name  # Always use "The" version for well-known bands
    else:
        # Use version with more records
        return the_name if the_count >= base_count else base_name


def apply_artist_normalizations(conn, normalizations: Dict[str, str]) -> int:
    """Apply artist name normalizations to the database"""
    total_updated = 0
    
    for old_name_key, new_name in normalizations.items():
        # Update all records with this artist name (case insensitive)
        result = conn.execute(text("""
            UPDATE music_records 
            SET artist_clean = :new_name
            WHERE LOWER(artist_clean) = :old_name_key
        """), {'new_name': new_name, 'old_name_key': old_name_key})
        
        updated_count = result.rowcount
        total_updated += updated_count
        
        if updated_count > 0:
            logger.info("Updated artist records", extra={
                'from': old_name_key, 'to': new_name, 'records_updated': updated_count
            })
    
    return total_updated


def save_dataframe_to_db(df_pandas: pd.DataFrame, table_name: str = 'music_records') -> bool:
    """
    Save pandas DataFrame to database using SQLAlchemy
    
    Args:
        df_pandas: DataFrame to save
        table_name: Target table name
        
    Returns:
        bool: Success status
    """
    try:
        # Validate inputs
        if df_pandas is None:
            logger.error("Cannot save None DataFrame to database")
            return False
            
        if df_pandas.empty:
            logger.warning("Attempting to save empty DataFrame to database", extra={'table': table_name})
            # Still proceed - might be intentional to clear the table
            
        engine = get_database_engine()
        
        # Log the columns we're about to save
        logger.debug("Saving DataFrame to database", extra={
            'table': table_name, 'shape': str(df_pandas.shape), 'columns': list(df_pandas.columns)
        })
        
        # Check if we can connect to the database
        with engine.connect() as conn:
            logger.debug("Database connection successful")
        
        # Log data types for debugging
        logger.debug("DataFrame dtypes", extra={'dtypes': str(df_pandas.dtypes.to_dict())})
        
        # Clean up data types to avoid SQLite issues
        df_cleaned = df_pandas.copy()
        
        # Convert object columns that should be strings
        string_columns = ['raw_song', 'raw_artist', 'song_clean', 'artist_clean', 'artist_lastfm', 'song_lastfm', 
                         'callsign', 'time', 'unique_id', 'combined', 'first_play', 'artist_image_url', 'artist_mbid', 'track_mbid']
        
        for col in string_columns:
            if col in df_cleaned.columns:
                df_cleaned[col] = df_cleaned[col].astype(str)
                # Replace 'nan' string with actual None
                df_cleaned[col] = df_cleaned[col].replace('nan', None)
        
        # Ensure integer columns are proper integers
        int_columns = ['time_unix', 'track_duration']
        for col in int_columns:
            if col in df_cleaned.columns:
                # Convert to numeric, errors='coerce' will turn invalid values to NaN
                df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')
        
        # Check for any problematic data types or values
        for col in df_cleaned.columns:
            null_count = df_cleaned[col].isnull().sum()
            if null_count > 0:
                logger.debug(f"Column {col} has {null_count} null values")
        
        # Handle large datasets with batching to avoid transaction size limits
        total_rows = len(df_cleaned)
        
        # Adjust batch size based on dataset size and number of columns
        num_columns = len(df_cleaned.columns)
        if total_rows > 30000 or num_columns > 15:
            batch_size = 500  # Smaller batches for large/wide datasets
        elif total_rows > 10000:
            batch_size = 1000  # Medium batches
        else:
            batch_size = 5000  # Larger batches for small datasets
        
        logger.info("Starting batched database save", extra={
            'total_rows': total_rows, 'columns': num_columns, 'batch_size': batch_size, 
            'estimated_batches': (total_rows + batch_size - 1) // batch_size
        })
        
        # For the music_records table, we need to handle schema evolution
        if table_name == 'music_records':
            # Drop and recreate table first
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS music_records"))
                conn.commit()
            
            # Recreate table with proper schema
            Base.metadata.tables['music_records'].create(engine)
            logger.info("Recreated music_records table with SQLAlchemy schema")
            
            # Insert data in batches with progress tracking
            successful_batches = 0
            for i in range(0, total_rows, batch_size):
                batch_end = min(i + batch_size, total_rows)
                batch_df = df_cleaned.iloc[i:batch_end].copy()  # Create a copy to avoid view warnings
                batch_num = (i // batch_size) + 1
                total_batches = (total_rows + batch_size - 1) // batch_size
                
                logger.debug("Processing batch", extra={
                    'batch': f"{batch_num}/{total_batches}", 'rows': f"{i+1}-{batch_end}", 'size': len(batch_df)
                })
                
                try:
                    batch_df.to_sql(
                        name=table_name,
                        con=engine,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                    successful_batches += 1
                    
                    # Progress update every 10 batches or for large datasets
                    if batch_num % 10 == 0 or total_batches <= 5:
                        logger.info("Batch progress", extra={
                            'completed': successful_batches, 'total': total_batches,
                            'progress': f"{(successful_batches/total_batches)*100:.1f}%"
                        })
                    
                except Exception as batch_error:
                    logger.error("Failed to save batch", extra={
                        'batch': batch_num, 'error': str(batch_error), 'batch_size': len(batch_df)
                    })
                    raise batch_error
                finally:
                    # Clean up batch DataFrame to free memory
                    del batch_df
        else:
            # For other tables, use batched approach as well
            if total_rows > batch_size:
                # Clear table first
                with engine.connect() as conn:
                    conn.execute(text(f"DELETE FROM {table_name}"))
                    conn.commit()
                
                # Insert data in batches
                for i in range(0, total_rows, batch_size):
                    batch_end = min(i + batch_size, total_rows)
                    batch_df = df_cleaned.iloc[i:batch_end].copy()
                    batch_num = (i // batch_size) + 1
                    total_batches = (total_rows + batch_size - 1) // batch_size
                    
                    logger.debug("Processing batch", extra={
                        'table': table_name, 'batch': f"{batch_num}/{total_batches}", 'rows': f"{i+1}-{batch_end}"
                    })
                    
                    try:
                        batch_df.to_sql(
                            name=table_name,
                            con=engine,
                            if_exists='append',
                            index=False,
                            method='multi'
                        )
                    finally:
                        del batch_df
            else:
                # Small dataset, use normal approach
                df_cleaned.to_sql(
                    name=table_name,
                    con=engine,
                    if_exists='replace',
                    index=False,
                    method='multi'
                )
        
        logger.info("DataFrame saved to database successfully", extra={
            'table': table_name, 'records': len(df_cleaned)
        })
        return True
        
    except Exception as e:
        # Get more detailed error information
        error_details = {
            'table': table_name, 
            'error_type': type(e).__name__,
            'error_message': str(e),
            'dataframe_shape': str(df_pandas.shape) if df_pandas is not None else 'None',
            'dataframe_columns': list(df_pandas.columns) if df_pandas is not None else 'None'
        }
        
        # Check for common issues
        if df_pandas is None:
            error_details['issue'] = 'DataFrame is None'
        elif df_pandas.empty:
            error_details['issue'] = 'DataFrame is empty'
        elif 'no such table' in str(e).lower():
            error_details['issue'] = 'Database table does not exist - need to run init_database()'
        elif 'locked' in str(e).lower():
            error_details['issue'] = 'Database is locked - another process may be using it'
        elif 'column' in str(e).lower():
            error_details['issue'] = 'Column mismatch between DataFrame and database schema'
        
        logger.error("Failed to save DataFrame to database", extra=error_details)
        return False


def get_artist_image_from_cache(artist_clean_name: str) -> Optional[str]:
    """Get artist image URL from cache"""
    engine = get_database_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        cached_image = session.query(ArtistImage).filter(
            ArtistImage.artist_name == artist_clean_name.strip()
        ).first()
        
        return cached_image.image_url if cached_image else None
    except Exception as e:
        logger.error("Error retrieving artist image from cache", extra={
            'artist': artist_clean_name, 'error': str(e)
        })
        return None
    finally:
        session.close()


def get_track_info_from_cache(artist_clean: str, song_clean: str) -> Optional[Dict[str, Any]]:
    """Get track information from cache"""
    engine = get_database_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        cached_track = session.query(TrackInfo).filter(
            TrackInfo.artist_clean == artist_clean.strip(),
            TrackInfo.song_clean == song_clean.strip()
        ).first()
        
        if cached_track and cached_track.fetch_success:
            return {
                'duration': cached_track.duration,
                'track_mbid': cached_track.track_mbid,
                'artist_lastfm': cached_track.artist_lastfm,
                'song_lastfm': cached_track.song_lastfm
            }
        return None
    except Exception as e:
        logger.error("Error retrieving track info from cache", extra={
            'artist': artist_clean, 'song': song_clean, 'error': str(e)
        })
        return None
    finally:
        session.close()


def cache_artist_image(artist_name: str, image_url: str, success: bool = True, 
                      error_message: Optional[str] = None, mbid: Optional[str] = None, 
                      artist_lastfm: Optional[str] = None) -> None:
    """Cache artist image result in database"""
    engine = get_database_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Check if already exists
        existing = session.query(ArtistImage).filter(
            ArtistImage.artist_name == artist_name.strip()
        ).first()
        
        if existing:
            # Update existing record
            existing.image_url = image_url
            existing.mbid = mbid
            existing.artist_lastfm = artist_lastfm
            existing.fetch_success = success
            existing.error_message = error_message
            existing.updated_at = datetime.utcnow()
        else:
            # Create new record
            new_image = ArtistImage(
                artist_name=artist_name.strip(),
                artist_lastfm=artist_lastfm,
                image_url=image_url,
                mbid=mbid,
                fetch_success=success,
                error_message=error_message
            )
            session.add(new_image)
        
        session.commit()
        logger.debug("Cached artist image", extra={
            'artist': artist_name.strip(),
            'success': success,
            'has_image': bool(image_url),
            'has_mbid': bool(mbid)
        })
    except Exception as e:
        logger.error("Failed to cache artist image", extra={
            'artist': artist_name, 'error': str(e)
        })
        session.rollback()
    finally:
        session.close()


def cache_track_info(artist_clean: str, song_clean: str, duration: Optional[int] = None, 
                    track_mbid: Optional[str] = None, success: bool = True, 
                    error_message: Optional[str] = None, artist_lastfm: Optional[str] = None, 
                    song_lastfm: Optional[str] = None) -> None:
    """Cache track information result in database"""
    engine = get_database_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Check if already exists
        existing = session.query(TrackInfo).filter(
            TrackInfo.artist_clean == artist_clean.strip(),
            TrackInfo.song_clean == song_clean.strip()
        ).first()
        
        if existing:
            # Update existing record
            existing.duration = duration
            existing.track_mbid = track_mbid
            existing.artist_lastfm = artist_lastfm
            existing.song_lastfm = song_lastfm
            existing.fetch_success = success
            existing.error_message = error_message
            existing.updated_at = datetime.utcnow()
        else:
            # Create new record
            new_track = TrackInfo(
                artist_clean=artist_clean.strip(),
                song_clean=song_clean.strip(),
                artist_lastfm=artist_lastfm,
                song_lastfm=song_lastfm,
                duration=duration,
                track_mbid=track_mbid,
                fetch_success=success,
                error_message=error_message
            )
            session.add(new_track)
        
        session.commit()
        logger.debug("Cached track info", extra={
            'artist': artist_clean.strip(),
            'song': song_clean.strip(),
            'success': success,
            'has_duration': duration is not None,
            'duration': duration,
            'has_mbid': bool(track_mbid)
        })
    except Exception as e:
        logger.error("Failed to cache track info", extra={
            'artist': artist_clean, 'song': song_clean, 'error': str(e)
        })
        session.rollback()
    finally:
        session.close()


def print_database_statistics():
    """Print comprehensive database statistics"""
    logger.info("Database Statistics")
    engine = get_database_engine()
    
    try:
        with engine.connect() as conn:
            # Count music records
            music_count = conn.execute(text("SELECT COUNT(*) FROM music_records")).scalar()
            logger.info("Music records in database", extra={'count': music_count})
            
            # Count records with parsed unix timestamps
            unix_count = conn.execute(text("SELECT COUNT(*) FROM music_records WHERE time_unix IS NOT NULL")).scalar()
            logger.info("Records with parsed timestamps", extra={'count': unix_count})
            
            # Count cached artists
            artist_count = conn.execute(text("SELECT COUNT(*) FROM artist_images")).scalar()
            logger.info("Cached artists", extra={'count': artist_count})
            
            # Count successful images
            success_count = conn.execute(text("SELECT COUNT(*) FROM artist_images WHERE fetch_success = 1")).scalar()
            logger.info("Successful image fetches", extra={'count': success_count})
            
            # Count cached tracks
            track_cache_count = conn.execute(text("SELECT COUNT(*) FROM track_info")).scalar()
            logger.info("Cached tracks", extra={'count': track_cache_count})
            
            # Count tracks with duration
            duration_count = conn.execute(text("SELECT COUNT(*) FROM track_info WHERE duration IS NOT NULL")).scalar()
            logger.info("Tracks with duration", extra={'count': duration_count})
            
            # Count records with track duration
            records_duration_count = conn.execute(text("SELECT COUNT(*) FROM music_records WHERE track_duration IS NOT NULL")).scalar()
            logger.info("Records with track duration", extra={'count': records_duration_count})
            
            # Show sample cached artists
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
            
            # Show sample tracks with duration and normalized titles
            if track_cache_count > 0:
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
            
            # Show top 10 artists by cumulative listening time
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
                logger.warning("No artists with duration data found")
            
            # Show bottom 10 artists by cumulative listening time
            logger.info("Bottom 10 Artists by Cumulative Listening Time")
            bottom_artists = conn.execute(text("""
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
                ORDER BY SUM(mr.track_duration) ASC
                LIMIT 10
            """)).fetchall()
            
            if bottom_artists:
                for i, (display_name, artist_clean, artist_lastfm, plays, avg_dur, total_sec, total_time) in enumerate(bottom_artists, 1):
                    avg_duration_str = f"{int(avg_dur // 60)}:{int(avg_dur % 60):02d}" if avg_dur else "N/A"
                    logger.info("Bottom artist by listening time", extra={
                        'rank': i, 'artist_display': display_name, 'artist_clean': artist_clean,
                        'artist_lastfm': artist_lastfm, 'plays': plays,
                        'avg_duration': avg_duration_str, 'total_time': total_time
                    })
            else:
                logger.warning("No bottom artists with duration data found")
                
    except Exception as e:
        logger.error("Error querying database", extra={'error': str(e)})


def demonstrate_pandas_integration():
    """Demonstrate pandas integration with database"""
    logger.info("Pandas Integration Demo")
    engine = get_database_engine()
    
    try:
        # Read data back from database using pandas
        df_from_db = pd.read_sql("SELECT * FROM music_records LIMIT 3", engine)
        logger.debug("Sample records from database")
        logger.debug(df_from_db[['raw_artist', 'raw_song', 'callsign']].to_string(index=False))
        
        # Show artist image cache
        artist_cache_df = pd.read_sql("SELECT * FROM artist_images", engine)
        logger.info("Artist cache summary", extra={'entries': len(artist_cache_df)})
        
    except Exception as e:
        logger.error("Error with pandas database operations", extra={'error': str(e)})