
#!/usr/bin/env python3
"""
PySpark application for processing music data with Last.fm API integration
"""

import json
import os
import re
from datetime import datetime

import pandas as pd
import requests
import sqlalchemy as sa
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, IntegerType
from sqlalchemy import create_engine, text, Column, Integer, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# print user of os
print(f"Running as user: {os.getenv('USER')}")

# Database configuration - easily changeable for PostgreSQL later
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
    artist_name = Column(String(200), unique=True, index=True)
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
    artist_clean = Column(String(200), index=True)
    song_clean = Column(String(500), index=True)
    duration = Column(Integer)  # Duration in seconds
    track_mbid = Column(String(100))  # Track MusicBrainz ID
    fetch_success = Column(Boolean, default=True)
    error_message = Column(String(500))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Create a composite index for faster lookups
    __table_args__ = (
        sa.Index('ix_track_lookup', 'artist_clean', 'song_clean'),
    )

def get_database_engine():
    """Create database engine - easily configurable for different DB types"""
    if DB_CONFIG['type'] == 'sqlite':
        # For local development, use current directory
        engine = create_engine(f"sqlite:///{DB_CONFIG['sqlite_path']}", echo=False)
    elif DB_CONFIG['type'] == 'postgresql':
        # PostgreSQL connection string for prod later
        connection_string = (
            f"postgresql://{DB_CONFIG['username']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        engine = create_engine(connection_string, echo=False)
    else:
        raise ValueError(f"Unsupported database type: {DB_CONFIG['type']}")
    
    return engine

def init_database():
    """Initialize database tables"""
    engine = get_database_engine()
    Base.metadata.create_all(engine)
    return engine

def save_dataframe_to_db(df_pandas, table_name='music_records'):
    """Save pandas DataFrame to database"""
    engine = get_database_engine()
    
    try:
        # Clear existing data in the table to avoid schema conflicts
        if table_name == 'music_records':
            with engine.connect() as conn:
                conn.execute(text(f"DELETE FROM {table_name}"))
                conn.commit()
                print(f"🧹 Cleared existing data from {table_name} table")
        
        # Debug: Print DataFrame info
        print(f"📊 DataFrame columns before save: {list(df_pandas.columns)}")
        print(f"📊 DataFrame shape: {df_pandas.shape}")
        
        # Ensure we have the artist_clean column
        if 'artist_clean' in df_pandas.columns:
            sample_clean = df_pandas['artist_clean'].iloc[0] if len(df_pandas) > 0 else "N/A"
            print(f"📊 Sample artist_clean value: '{sample_clean}'")
        else:
            print("⚠️  Warning: artist_clean column not found in DataFrame!")
        
        # Debug: Show pandas DataFrame columns and sample data before save
        print(f"📊 Pandas DataFrame columns before to_sql: {list(df_pandas.columns)}")
        print(f"📊 Sample pandas data:")
        for i in range(min(3, len(df_pandas))):
            row = df_pandas.iloc[i]
            print(f"   Row {i}: raw_artist='{row['raw_artist']}', artist_clean='{row['artist_clean']}'")
        
        # Save to database - use 'append' since we cleared the data above
        print(f"🔄 About to call to_sql with method='multi', chunksize=1000...")
        df_pandas.to_sql(table_name, engine, if_exists='append', index=False, method='multi', chunksize=1000)
        print(f"✅ to_sql completed - saved {len(df_pandas)} records to {table_name} table")
        
        # Force commit by creating a new connection
        print(f"🔄 Creating new connection to verify data...")
        with engine.connect() as conn:
            # Commit any pending transaction
            conn.commit()
            result = conn.execute(text("SELECT raw_artist, artist_clean FROM music_records LIMIT 3")).fetchall()
            print(f"🔍 Immediate verification with new connection - first 3 records:")
            for i, (raw, clean) in enumerate(result, 1):
                print(f"   {i}. Raw: '{raw}' → Clean: '{clean}' (type: {type(clean)})")
        
        return True
    except Exception as e:
        print(f"❌ Error saving to database: {str(e)}")
        print(f"📊 DataFrame columns during error: {list(df_pandas.columns)}")
        return False

def get_artist_image_from_cache(artist_clean_name):
    """
    Check if artist image is already cached in database using cleaned artist name as key
    
    Args:
        artist_clean_name (str): Cleaned/standardized artist name used as cache key
        
    Returns:
        str: Cached image URL if found and successful, None otherwise
    """
    engine = get_database_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        cached_image = session.query(ArtistImage).filter(
            ArtistImage.artist_name == artist_clean_name.strip()
        ).first()
        
        if cached_image:
            return cached_image.image_url if cached_image.fetch_success else None
        return None
    finally:
        session.close()

def parse_time_to_unix(time_str):
    """
    Safely parse time string to unix timestamp
    
    Args:
        time_str (str): Time string (expected to be unix timestamp)
        
    Returns:
        int: Unix timestamp in seconds, or None if parsing fails
    """
    if not time_str or pd.isna(time_str):
        return None
    
    try:
        # Convert to string and strip whitespace
        time_str = str(time_str).strip()
        
        # If empty after stripping
        if not time_str:
            return None
        
        # Try to parse as integer (unix timestamp)
        unix_timestamp = int(float(time_str))
        
        # Basic sanity check: unix timestamp should be reasonable
        # (between 1970 and 2050 approximately)
        if 0 <= unix_timestamp <= 2524608000:  # Jan 1, 2050
            return unix_timestamp
        else:
            return None
            
    except (ValueError, TypeError, OverflowError) as e:
        # Return None for any parsing errors
        return None

def clean_artist_name(raw_artist):
    """
    Clean and standardize artist names for better grouping and API calls
    
    Args:
        raw_artist (str): Raw artist name from the data
        
    Returns:
        str: Cleaned artist name
    """
    if not raw_artist or pd.isna(raw_artist):
        return ""
    
    # Convert to string and strip whitespace
    artist = str(raw_artist).strip()
    
    # Handle empty or very short strings
    if len(artist) < 1:
        return ""
    
    # Remove extra whitespace (multiple spaces, tabs, newlines)
    artist = re.sub(r'\s+', ' ', artist)
    
    # Fix common abbreviations and symbols
    replacements = {
        # Ampersand variations
        r'\s*&\s*': ' AND ',
        r'\s*\+\s*': ' AND ',
        
        # Common abbreviations
        r'\bFT\.\s*': 'FEAT. ',
        r'\bFT\s+': 'FEAT. ',
        r'\bFEAT\s+': 'FEAT. ',
        r'\bFEATURING\s+': 'FEAT. ',
        
        # Remove or standardize special characters
        r'["""]': '"',  # Standardize quotes
        r"['']": "'",  # Standardize apostrophes
        r'[\u2013\u2014]': '-',  # Em/en dashes to hyphen
        r'\.{2,}': '.',  # Multiple dots to single dot
        
        # Remove leading/trailing periods and hyphens
        r'^[\.\-\s]+': '',
        r'[\.\-\s]+$': '',
        
        # Fix spacing around punctuation
        r'\s*\.\s*': '. ',
        r'\s*,\s*': ', ',
        r'\s*;\s*': '; ',
        r'\s*:\s*': ': ',
        r'\s*!\s*': '! ',
        r'\s*\?\s*': '? ',
        
        # Remove extra spaces again after punctuation fixes
        r'\s+': ' ',
    }
    
    # Apply replacements
    for pattern, replacement in replacements.items():
        artist = re.sub(pattern, replacement, artist, flags=re.IGNORECASE)
    
    # Title case handling - be smart about it
    # First, handle common words that should stay lowercase
    artist = artist.strip()
    
    # Split into words and process each
    words = artist.split()
    processed_words = []
    
    # Words that should typically stay lowercase (unless at start)
    lowercase_words = {
        'and', 'or', 'the', 'a', 'an', 'of', 'in', 'on', 'at', 'to', 'for', 
        'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'feat.'
    }
    
    for i, word in enumerate(words):
        # Handle special cases
        if word.lower() in lowercase_words and i > 0:
            processed_words.append(word.lower())
        elif word.isupper() and len(word) > 1:
            # Handle all caps words - make them title case unless they're abbreviations
            if len(word) <= 4 and not any(c in word for c in '.&'):
                # Likely an abbreviation, keep uppercase
                processed_words.append(word)
            else:
                # Long all-caps word, convert to title case
                processed_words.append(word.title())
        elif word.islower() and len(word) > 1:
            # All lowercase word, convert to title case
            processed_words.append(word.title())
        else:
            # Mixed case or single character, keep as is
            processed_words.append(word)
    
    # Join back together
    artist = ' '.join(processed_words)
    
    # Final cleanup
    artist = artist.strip()
    
    # Handle some special cases
    # Fix common artist name patterns
    artist = re.sub(r'\bDj\b', 'DJ', artist, flags=re.IGNORECASE)
    artist = re.sub(r'\bMc([A-Z])', r'Mc\1', artist)
    artist = re.sub(r'\bMac([A-Z])', r'Mac\1', artist)
    
    # Fix ordinal numbers
    artist = re.sub(r'\b(\d+)(st|nd|rd|th)\b', r'\1\2', artist, flags=re.IGNORECASE)

    # @Todo this method is quite cumbersome, consider improving later using library
    # @todo clean " (Live)"
    # @Todo log if artist can not be found, probably useful for debugging later and cleanup steps
    
    return artist

def clean_song_title(raw_song):
    """
    Clean and standardize song titles for better matching and API calls
    
    Args:
        raw_song (str): Raw song title from the data
        
    Returns:
        str: Cleaned song title
    """
    if not raw_song or pd.isna(raw_song):
        return ""
    
    # Convert to string and strip whitespace
    song = str(raw_song).strip()
    
    # Handle empty or very short strings
    if len(song) < 1:
        return ""
    
    # Remove extra whitespace (multiple spaces, tabs, newlines)
    song = re.sub(r'\s+', ' ', song)
    
    # Common song title cleanup
    replacements = {
        # Remove common suffixes/annotations
        r'\s*\(live\)\s*$': '',
        r'\s*\(Live\)\s*$': '',
        r'\s*\(LIVE\)\s*$': '',
        r'\s*\(acoustic\)\s*$': '',
        r'\s*\(Acoustic\)\s*$': '',
        r'\s*\(ACOUSTIC\)\s*$': '',
        r'\s*\(radio edit\)\s*$': '',
        r'\s*\(Radio Edit\)\s*$': '',
        r'\s*\(RADIO EDIT\)\s*$': '',
        r'\s*\(clean\)\s*$': '',
        r'\s*\(Clean\)\s*$': '',
        r'\s*\(CLEAN\)\s*$': '',
        r'\s*\(explicit\)\s*$': '',
        r'\s*\(Explicit\)\s*$': '',
        r'\s*\(EXPLICIT\)\s*$': '',
        
        # Standardize featuring patterns
        r'\s*\bfeat\.\s*': ' feat. ',
        r'\s*\bfeat\s+': ' feat. ',
        r'\s*\bft\.\s*': ' feat. ',
        r'\s*\bft\s+': ' feat. ',
        r'\s*\bfeaturing\s+': ' feat. ',
        
        # Remove or standardize special characters
        r'["""]': '"',  # Standardize quotes
        r"['']": "'",  # Standardize apostrophes
        r'[\u2013\u2014]': '-',  # Em/en dashes to hyphen
        r'\.{2,}': '.',  # Multiple dots to single dot
        
        # Fix spacing around punctuation
        r'\s*\.\s*': '. ',
        r'\s*,\s*': ', ',
        r'\s*;\s*': '; ',
        r'\s*:\s*': ': ',
        r'\s*!\s*': '! ',
        r'\s*\?\s*': '? ',
        
        # Remove extra spaces
        r'\s+': ' ',
    }
    
    # Apply replacements
    for pattern, replacement in replacements.items():
        song = re.sub(pattern, replacement, song, flags=re.IGNORECASE)
    
    # Title case the song, but preserve some common patterns
    song = song.strip()
    
    # Split into words and process each
    words = song.split()
    processed_words = []
    
    # Words that should typically stay lowercase (unless at start)
    lowercase_words = {
        'and', 'or', 'the', 'a', 'an', 'of', 'in', 'on', 'at', 'to', 'for', 
        'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'feat.'
    }
    
    for i, word in enumerate(words):
        if word.lower() in lowercase_words and i > 0:
            processed_words.append(word.lower())
        elif word.isupper() and len(word) > 1:
            # Handle all caps words
            if len(word) <= 4 and not any(c in word for c in '.&'):
                # Likely an abbreviation, keep uppercase  
                processed_words.append(word)
            else:
                # Long all-caps word, convert to title case
                processed_words.append(word.title())
        elif word.islower() and len(word) > 1:
            # All lowercase word, convert to title case
            processed_words.append(word.title())
        else:
            # Mixed case or single character, keep as is
            processed_words.append(word)
    
    # Join back together
    song = ' '.join(processed_words)
    
    # Final cleanup
    song = song.strip()
    
    return song

def cache_artist_image(artist_name, image_url, success=True, error_message=None, mbid=None):
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
            existing.fetch_success = success
            existing.error_message = error_message
            existing.updated_at = datetime.utcnow()
        else:
            # Create new record
            new_image = ArtistImage(
                artist_name=artist_name.strip(),
                image_url=image_url,
                mbid=mbid,
                fetch_success=success,
                error_message=error_message
            )
            session.add(new_image)
        
        session.commit()
    except Exception as e:
        print(f"Error caching artist image: {str(e)}")
        session.rollback()
    finally:
        session.close()

def get_track_info_from_cache(artist_clean, song_clean):
    """
    Check if track info is already cached in database
    
    Args:
        artist_clean (str): Cleaned artist name
        song_clean (str): Cleaned song title
        
    Returns:
        dict: Cached track info if found and successful, None otherwise
    """
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
                'track_mbid': cached_track.track_mbid
            }
        return None
    finally:
        session.close()

def cache_track_info(artist_clean, song_clean, duration=None, track_mbid=None, success=True, error_message=None):
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
            existing.fetch_success = success
            existing.error_message = error_message
            existing.updated_at = datetime.utcnow()
        else:
            # Create new record
            new_track = TrackInfo(
                artist_clean=artist_clean.strip(),
                song_clean=song_clean.strip(),
                duration=duration,
                track_mbid=track_mbid,
                fetch_success=success,
                error_message=error_message
            )
            session.add(new_track)
        
        session.commit()
    except Exception as e:
        print(f"Error caching track info: {str(e)}")
        session.rollback()
    finally:
        session.close()

def init_spark():
  # connection
  spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
  sc = spark.sparkContext
  return spark,sc

def fetch_musicbrainz_image(mbid):
    """
    Fetch artist image from MusicBrainz using MBID
    
    Args:
        mbid (str): MusicBrainz ID
        
    Returns:
        str: Image URL if found, None otherwise
    """
    if not mbid or not mbid.strip():
        return None
        
    try:
        # MusicBrainz API endpoint for artist relations
        url = f"https://musicbrainz.org/ws/2/artist/{mbid}?inc=url-rels&fmt=json"
        
        # Make the API request with timeout
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        # Look for image relations
        if 'relations' in data:
            relations = data['relations']
            for relation in relations:
                if relation.get('type') == 'image' and 'url' in relation:
                    image_url = relation['url']['resource']
                    
                    # Handle Wikimedia URLs specially
                    if 'https://commons.wikimedia.org/wiki/File:' in image_url:
                        filename = image_url.split('/')[-1]
                        image_url = f"https://commons.wikimedia.org/wiki/Special:Redirect/file/{filename}"
                    
                    print(f"🎨 Found MusicBrainz image for MBID {mbid}: {image_url}")
                    return image_url
        
        print(f"🔍 No image found in MusicBrainz for MBID {mbid}")
        return None
        
    except requests.exceptions.RequestException as e:
        print(f"❌ MusicBrainz API error for MBID {mbid}: {str(e)[:100]}")
        return None

def fetch_lastfm_artist_info(artist_clean_name, artist_full_name=None):
    """
    Fetch artist image URL and MBID from Last.fm API with MusicBrainz fallback
    
    Args:
        artist_clean_name (str): Cleaned artist name used as cache key
        artist_full_name (str): Full/original artist name used for API lookup (optional)
        
    Returns:
        tuple: (image_url, mbid) where either can be None
    """
    if not artist_clean_name or artist_clean_name.strip() == "":
        return ("No artist name provided", None)
    
    # Use full name for API lookup if provided, otherwise use clean name
    lookup_name = artist_full_name if artist_full_name else artist_clean_name
    
    # Check cache first using cleaned name as key
    cached_result = get_artist_image_from_cache(artist_clean_name)
    if cached_result is not None:
        print(f"🎯 Cache hit for artist: {artist_clean_name}")
        # Get MBID from cache too
        engine = get_database_engine()
        Session = sessionmaker(bind=engine)
        session = Session()
        try:
            cached_record = session.query(ArtistImage).filter(
                ArtistImage.artist_name == artist_clean_name.strip()
            ).first()
            cached_mbid = cached_record.mbid if cached_record else None
            return (cached_result, cached_mbid)
        finally:
            session.close()
    
    print(f"🌐 Fetching from Last.fm API for artist: {lookup_name} (cache key: {artist_clean_name})")
    
    # Last.fm API key
    api_key = "9b38e4cb99aa0de263aa854e1582fad4" # @todo move to env var
    
    # @todo retry and backoff logic could be added here for robustness
    try:
        # Last.fm API endpoint for artist.getInfo
        url = "https://ws.audioscrobbler.com/2.0/"
        params = {
            'method': 'artist.getinfo',
            'artist': lookup_name.strip(),
            'api_key': api_key,
            'format': 'json'
        }
        
        # Make the API request with timeout
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        # Debug: Log the full API response structure to see available fields
        mbid = None
        if 'artist' in data:
            artist_data = data['artist']
            print(f"🔍 Last.fm API fields for '{lookup_name}': {list(artist_data.keys())}")
            
            # Check for MBID (MusicBrainz ID)
            if 'mbid' in artist_data and artist_data['mbid']:
                mbid = artist_data['mbid']
                print(f"🎵 Found MBID for '{lookup_name}': {mbid}")
                
                # Try to fetch image from MusicBrainz first
                mb_image_url = fetch_musicbrainz_image(mbid)
                if mb_image_url:
                    cache_artist_image(artist_clean_name, mb_image_url, success=True, mbid=mbid)
                    return (mb_image_url, mbid)
                else:
                    print(f"🔄 MusicBrainz had no image, falling back to Last.fm for '{lookup_name}'")
            else:
                print(f"❌ No MBID found for '{lookup_name}'")
        
        # Check if artist data exists and try Last.fm images as fallback
        if 'artist' in data and 'image' in data['artist']:
            images = data['artist']['image']
            # Get the largest image (usually the last one)
            for img in reversed(images):
                if img.get('#text') and img.get('#text').strip():
                    image_url = img['#text']
                    # Cache successful result using cleaned name as key
                    cache_artist_image(artist_clean_name, image_url, success=True, mbid=mbid)
                    return (image_url, mbid)
            
            # No image found but artist exists
            no_image_msg = "No image found"
            cache_artist_image(artist_clean_name, no_image_msg, success=False, error_message="No image available", mbid=mbid)
            return (no_image_msg, mbid)
        else:
            # Artist not found
            not_found_msg = f"Artist not found: {lookup_name}"
            cache_artist_image(artist_clean_name, not_found_msg, success=False, error_message="Artist not found", mbid=mbid)
            return (not_found_msg, mbid)
            
    except requests.exceptions.Timeout:
        timeout_msg = f"Timeout for artist: {lookup_name}"
        cache_artist_image(artist_clean_name, timeout_msg, success=False, error_message="API timeout")
        return (timeout_msg, None)
    except requests.exceptions.RequestException as e:
        error_msg = f"Request error for {lookup_name}: {str(e)[:100]}"
        cache_artist_image(artist_clean_name, error_msg, success=False, error_message=str(e)[:100])
        return (error_msg, None)
    except json.JSONDecodeError:
        json_error_msg = f"Invalid JSON response for {lookup_name}"
        cache_artist_image(artist_clean_name, json_error_msg, success=False, error_message="Invalid JSON response")
        return (json_error_msg, None)

def fetch_lastfm_track_info(artist_clean, song_clean, raw_artist=None, raw_song=None):
    """
    Fetch track information from Last.fm API including duration and MBID
    
    Args:
        artist_clean (str): Cleaned artist name used as cache key
        song_clean (str): Cleaned song title used as cache key
        raw_artist (str): Original artist name for API lookup (optional)
        raw_song (str): Original song title for API lookup (optional)
        
    Returns:
        dict: Track info with duration and track_mbid, or None if failed
    """
    if not artist_clean or not song_clean or artist_clean.strip() == "" or song_clean.strip() == "":
        return None
    
    # Check cache first
    cached_result = get_track_info_from_cache(artist_clean, song_clean)
    if cached_result is not None:
        print(f"🎯 Track cache hit for: {artist_clean} - {song_clean}")
        return cached_result
    
    # Use raw names for API lookup if provided, otherwise use clean names
    lookup_artist = raw_artist if raw_artist else artist_clean
    lookup_song = raw_song if raw_song else song_clean
    
    print(f"🎵 Fetching track info from Last.fm: {lookup_artist} - {lookup_song}")
    
    # Last.fm API key
    api_key = "9b38e4cb99aa0de263aa854e1582fad4" # @todo move to env var
    
    try:
        # Last.fm API endpoint for track.getInfo
        url = "https://ws.audioscrobbler.com/2.0/"
        params = {
            'method': 'track.getinfo',
            'artist': lookup_artist.strip(),
            'track': lookup_song.strip(),
            'api_key': api_key,
            'format': 'json'
        }
        
        # Make the API request with timeout
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if 'track' in data:
            track_data = data['track']
            
            # Extract duration (in seconds)
            duration = None
            if 'duration' in track_data and track_data['duration']:
                try:
                    # Last.fm duration is in milliseconds, convert to seconds
                    duration_ms = int(track_data['duration'])
                    duration = duration_ms // 1000 if duration_ms > 0 else None
                except (ValueError, TypeError):
                    duration = None
            
            # Extract track MBID
            track_mbid = None
            if 'mbid' in track_data and track_data['mbid']:
                track_mbid = track_data['mbid']
            
            # Log what we found
            if duration:
                minutes = duration // 60
                seconds = duration % 60
                print(f"🕒 Found track duration: {minutes}:{seconds:02d} ({duration}s)")
            if track_mbid:
                print(f"🎵 Found track MBID: {track_mbid}")
            
            # Cache the result
            result = {
                'duration': duration,
                'track_mbid': track_mbid
            }
            cache_track_info(artist_clean, song_clean, duration=duration, 
                           track_mbid=track_mbid, success=True)
            return result
        else:
            print(f"❌ Track not found: {lookup_artist} - {lookup_song}")
            cache_track_info(artist_clean, song_clean, success=False, 
                           error_message="Track not found")
            return None
            
    except requests.exceptions.Timeout:
        print(f"⏰ Timeout for track: {lookup_artist} - {lookup_song}")
        cache_track_info(artist_clean, song_clean, success=False, 
                       error_message="API timeout")
        return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Request error for track {lookup_artist} - {lookup_song}: {str(e)[:100]}")
        cache_track_info(artist_clean, song_clean, success=False, 
                       error_message=str(e)[:100])
        return None
    except json.JSONDecodeError:
        print(f"❌ Invalid JSON response for track: {lookup_artist} - {lookup_song}")
        cache_track_info(artist_clean, song_clean, success=False, 
                       error_message="Invalid JSON response")
        return None

def main():
  # Initialize database first
  print("🗄️  Initializing database...")
  engine = init_database()
  print("✅ Database initialized successfully")
  
  spark,sc = init_spark()

  # Register UDFs
  clean_artist_udf = udf(clean_artist_name, StringType())
  clean_song_udf = udf(clean_song_title, StringType())
  parse_time_udf = udf(parse_time_to_unix, IntegerType())

  # Read testdata.txt using PySpark
  # Read as DataFrame with semicolon separator
  df = spark.read.option("delimiter", ";").option("header", "true").csv("/app/testdata.txt")
  
  print("Schema of the data:")
  df.printSchema()
  
  print("\nFirst 10 rows:")
  df.show(10)
  
  print(f"\nTotal number of records: {df.count()}")
  
  # Add cleaned columns and unix timestamp to Spark DataFrame
  print("\n🧹 Adding cleaned artist names, song titles, and parsing timestamps...")
  df = df.withColumn("ARTIST_CLEAN", clean_artist_udf(col("RAW_ARTIST")))
  df = df.withColumn("SONG_CLEAN", clean_song_udf(col("RAW_SONG")))
  df = df.withColumn("TIME_UNIX", parse_time_udf(col("TIME")))
  
  # Convert Spark DataFrame to Pandas for initial database save
  print("\n📊 Converting Spark DataFrame to Pandas...")
  df_pandas = df.toPandas()
  
  # Clean up column names for database compatibility
  df_pandas.columns = [col.lower().replace('?', '_question').replace(' ', '_') for col in df_pandas.columns]
  
  # Keep only the columns we need for our database model
  required_columns = ['raw_song', 'raw_artist', 'song_clean', 'artist_clean', 'callsign', 'time', 'time_unix', 'unique_id', 'combined', 'first_question']
  
  # Filter to only required columns that exist
  available_columns = [col for col in required_columns if col in df_pandas.columns]
  df_pandas = df_pandas[available_columns].copy()
  
  # Rename columns to match our database model
  column_mapping = {
      'first_question': 'first_play'  # first? -> first_question -> first_play
  }
  
  # Apply column renames
  for old_col, new_col in column_mapping.items():
      if old_col in df_pandas.columns:
          df_pandas = df_pandas.rename(columns={old_col: new_col})
  
  # Show some examples of cleaned names and timestamp parsing
  print("🧹 Data processing results:")
  if 'artist_clean' in df_pandas.columns and 'raw_artist' in df_pandas.columns:
      print(f"   • Cleaned {len(df_pandas)} artist names")
      sample_artists = df_pandas[['raw_artist', 'artist_clean']].drop_duplicates().head(5)
      for _, row in sample_artists.iterrows():
          print(f"     '{row['raw_artist']}' → '{row['artist_clean']}'")
  else:
      print("   • Artist cleaning columns not found")
      
  # Show song cleaning results
  if 'song_clean' in df_pandas.columns and 'raw_song' in df_pandas.columns:
      print(f"   • Cleaned {len(df_pandas)} song titles")
      sample_songs = df_pandas[['raw_song', 'song_clean']].drop_duplicates().head(5)
      for _, row in sample_songs.iterrows():
          print(f"     '{row['raw_song']}' → '{row['song_clean']}'")
  else:
      print("   • Song cleaning columns not found")
  
  # Show timestamp parsing results
  if 'time_unix' in df_pandas.columns and 'time' in df_pandas.columns:
      parsed_count = df_pandas['time_unix'].notna().sum()
      print(f"   • Parsed {parsed_count}/{len(df_pandas)} timestamps successfully")
      if parsed_count > 0:
          # Show a few examples
          sample_times = df_pandas[df_pandas['time_unix'].notna()][['time', 'time_unix']].head(3)
          for _, row in sample_times.iterrows():
              print(f"     '{row['time']}' → {row['time_unix']}")
  else:
      print("   • Timestamp parsing columns not found")
  
  # Add placeholder columns for additional metadata
  df_pandas['artist_image_url'] = None
  df_pandas['artist_mbid'] = None
  df_pandas['track_duration'] = None
  df_pandas['track_mbid'] = None
  
  print(f"📋 Pandas DataFrame shape: {df_pandas.shape}")
  print(f"📋 Pandas DataFrame columns: {list(df_pandas.columns)}")
  
  # Save initial data to database
  print("\n💾 Saving initial data to database...")
  save_success = save_dataframe_to_db(df_pandas, 'music_records')
  
  # Show unique cleaned artists (first 10)
  print("\nUnique artists (first 10):")
  unique_artists_df = df.select("RAW_ARTIST", "ARTIST_CLEAN").distinct()
  unique_artists_df.show(10)
  
  print("\n🎵 Fetching artist images from Last.fm API...")
  
  # Get unique cleaned artists to avoid duplicate API calls
  # Use cleaned names as cache keys for deduplication
  # unique_artists_for_api = df.select("RAW_ARTIST", "ARTIST_CLEAN").distinct()
  unique_artists_for_api = df.select("RAW_ARTIST", "ARTIST_CLEAN").distinct().limit(10) # @todo remove for production purposes, this prevents API rate limits issues
  
  # Get count of unique artists for logging
  unique_count = unique_artists_for_api.count()
  print(f"\n🖼️  Processing {unique_count} unique artists for image URLs:")
  unique_artists_for_api.show(10, truncate=False)
  
  # Convert to pandas to handle API calls with proper caching
  artists_pandas = unique_artists_for_api.toPandas()
  artists_pandas.columns = [col.lower() for col in artists_pandas.columns]
  
  # Add columns for image URL and MBID
  artists_pandas['artist_image_url'] = None
  artists_pandas['artist_mbid'] = None
  
  print("\n💾 Fetching and caching artist images...")
  
  # Process each unique artist with optimized caching
  # Use cleaned name as cache key, but raw name for API lookup
  for _, row in artists_pandas.iterrows():
      artist_clean = row['artist_clean']
      artist_raw = row['raw_artist']
      
      # Fetch both image URL and MBID
      print(f"🌐 Fetching for '{artist_raw}' (cache key: '{artist_clean}')")
      image_url, mbid = fetch_lastfm_artist_info(artist_clean, artist_raw)
      
      # Update the dataframe with both results
      artists_pandas.loc[artists_pandas['artist_clean'] == artist_clean, 'artist_image_url'] = image_url
      if mbid:  # Only update MBID if we have one
          artists_pandas.loc[artists_pandas['artist_clean'] == artist_clean, 'artist_mbid'] = mbid
  
  # Fetch track information from Last.fm
  print("\n🎵 Fetching track information from Last.fm API...")
  
  # Get unique artist/song combinations for track info
  unique_tracks = df.select("RAW_ARTIST", "RAW_SONG", "ARTIST_CLEAN", "SONG_CLEAN").distinct().limit(20)  # Limit for testing
  
  # Get count of unique tracks for logging
  track_count = unique_tracks.count()
  print(f"\n🎶 Processing {track_count} unique tracks for duration and metadata:")
  unique_tracks.show(10, truncate=False)
  
  # Convert to pandas for track processing
  tracks_pandas = unique_tracks.toPandas()
  tracks_pandas.columns = [col.lower() for col in tracks_pandas.columns]
  
  # Process each unique track
  for _, row in tracks_pandas.iterrows():
      artist_clean = row['artist_clean']
      song_clean = row['song_clean']
      artist_raw = row['raw_artist']
      song_raw = row['raw_song']
      
      # Fetch track information
      track_info = fetch_lastfm_track_info(artist_clean, song_clean, artist_raw, song_raw)
      
      if track_info:
          # Update main dataframe with track information
          mask = (df_pandas['artist_clean'] == artist_clean) & (df_pandas['song_clean'] == song_clean)
          
          if track_info.get('duration'):
              df_pandas.loc[mask, 'track_duration'] = track_info['duration']
          
          if track_info.get('track_mbid'):
              df_pandas.loc[mask, 'track_mbid'] = track_info['track_mbid']
  
  # Save updated DataFrame with track durations back to database
  print("\n💾 Saving updated track duration data to database...")
  save_success = save_dataframe_to_db(df_pandas, 'music_records')
  
  if save_success:
      print("✅ Track duration data saved successfully")
  else:
      print("❌ Failed to save track duration data")
  
  # Query database to show cached results
  print("\n📊 Database Statistics:")
  try:
      with engine.connect() as conn:
          # Count music records
          music_count = conn.execute(text("SELECT COUNT(*) FROM music_records")).scalar()
          print(f"   📀 Music records in database: {music_count}")
          
          # Count records with parsed unix timestamps
          unix_count = conn.execute(text("SELECT COUNT(*) FROM music_records WHERE time_unix IS NOT NULL")).scalar()
          print(f"   ⏰ Records with parsed timestamps: {unix_count}")
          
          # Count cached artists
          artist_count = conn.execute(text("SELECT COUNT(*) FROM artist_images")).scalar()
          print(f"   🎭 Cached artists: {artist_count}")
          
          # Count successful images
          success_count = conn.execute(text("SELECT COUNT(*) FROM artist_images WHERE fetch_success = 1")).scalar()
          print(f"   ✅ Successful image fetches: {success_count}")
          
          # Count cached tracks
          track_cache_count = conn.execute(text("SELECT COUNT(*) FROM track_info")).scalar()
          print(f"   🎵 Cached tracks: {track_cache_count}")
          
          # Count tracks with duration
          duration_count = conn.execute(text("SELECT COUNT(*) FROM track_info WHERE duration IS NOT NULL")).scalar()
          print(f"   ⏱️  Tracks with duration: {duration_count}")
          
          # Count records with track duration
          records_duration_count = conn.execute(text("SELECT COUNT(*) FROM music_records WHERE track_duration IS NOT NULL")).scalar()
          print(f"   🕒 Records with track duration: {records_duration_count}")
          
          # Show sample cached artists
          print("\n🎭 Sample cached artists:")
          cached_artists = conn.execute(text("""
              SELECT artist_name, 
                     CASE WHEN fetch_success = 1 THEN 'Success' ELSE 'Failed' END as status,
                     SUBSTR(image_url, 1, 50) || '...' as image_preview
              FROM artist_images 
              LIMIT 5
          """)).fetchall()
          
          for artist in cached_artists:
              print(f"   • {artist[0]}: {artist[1]} - {artist[2]}")
          
          # Show sample tracks with duration and normalized titles
          if track_cache_count > 0:
              print(f"\n  Sample Tracks with Duration:")
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
                  print(f"   • {artist_clean} - {song_clean}")
                  print(f"     Duration: {duration_str} | MBID: {mbid_preview}")
          
          # Show top 10 artists by cumulative listening time
          print(f"\n🎵 Top 10 Artists by Cumulative Listening Time:")
          top_artists = conn.execute(text("""
              SELECT 
                  mr.artist_clean,
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
              GROUP BY mr.artist_clean
              ORDER BY SUM(mr.track_duration) DESC
              LIMIT 10
          """)).fetchall()
          
          if top_artists:
              for i, (artist, plays, avg_dur, total_sec, total_time) in enumerate(top_artists, 1):
                  avg_duration_str = f"{int(avg_dur // 60)}:{int(avg_dur % 60):02d}" if avg_dur else "N/A"
                  print(f"   {i:2d}. {artist}")
                  print(f"       Plays: {plays:,} | Avg Duration: {avg_duration_str} | Total Time: {total_time}")
          else:
              print("   No artists with duration data found")
              
  except Exception as e:
      print(f"Error querying database: {str(e)}")
  
  # Demonstrate pandas integration
  print("\n🐼 Pandas Integration Demo:")
  try:
      # Read data back from database using pandas
      df_from_db = pd.read_sql("SELECT * FROM music_records LIMIT 3", engine)
      print("Sample records from database:")
      print(df_from_db[['raw_artist', 'raw_song', 'callsign']].to_string(index=False))
      
      # Show artist image cache
      artist_cache_df = pd.read_sql("SELECT * FROM artist_images", engine)
      print(f"\nArtist cache contains {len(artist_cache_df)} entries")
      
  except Exception as e:
      print(f"Error with pandas database operations: {str(e)}")
  
  # hello world test (keeping original for reference)
  nums = sc.parallelize([1,2,3,4])
  print(f"\n🔢 Original test - squares: {nums.map(lambda x: x*x).collect()}")
  
  print("\n🎉 Application completed successfully!")


if __name__ == '__main__':
  main()
