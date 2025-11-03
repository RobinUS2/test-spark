#!/usr/bin/env python3
"""
Main application for processing music data with Last.fm API integration
"""

import logging
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, IntegerType

from utils import setup_logging, parse_time_to_unix, clean_artist_name, clean_song_title, init_spark
from repositories.database_repository import db_repo
from services.artist_normalization_service import artist_service
from services.statistics_service import stats_service
from lastfm_api import fetch_lastfm_artist_info, fetch_lastfm_track_info

# Set up logging
logger = setup_logging()


def main():
    """Main application entry point"""
    # Initialize database first
    logger.info("Initializing database")
    engine = db_repo.init_database()
    logger.info("Database initialized successfully")
    
    # Test database connection
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            logger.info("Database connection test successful")
    except Exception as e:
        logger.error("Database connection test failed", extra={'error': str(e)})
        return
    
    spark, sc = init_spark()

    # Register UDFs
    clean_artist_udf = udf(clean_artist_name, StringType())
    clean_song_udf = udf(clean_song_title, StringType())
    parse_time_udf = udf(parse_time_to_unix, IntegerType())

    # Read testdata.txt using PySpark
    # Read as DataFrame with semicolon separator
    df = spark.read.option("delimiter", ";").option("header", "true").csv("/app/testdata.txt")
    
    logger.info("Schema of the data:")
    df.printSchema()
    
    logger.info("Column names:")
    logger.info(str(df.columns))
    
    # Debug: Check if specific columns exist
    if "RAW_ARTIST" in df.columns:
        logger.info("RAW_ARTIST column found")
    else:
        logger.warning("RAW_ARTIST column not found")
        
    if "RAW_SONG" in df.columns:
        logger.info("RAW_SONG column found") 
    else:
        logger.warning("RAW_SONG column not found")
    
    logger.info("First 10 rows:")
    df.show(10)
    
    logger.info(f"Total number of records: {df.count()}")
    
    # Clean artist and song names using UDFs
    df = df.withColumn("ARTIST_CLEAN", clean_artist_udf(col("RAW_ARTIST")))
    df = df.withColumn("SONG_CLEAN", clean_song_udf(col("RAW_SONG")))
    df = df.withColumn("TIME_UNIX", parse_time_udf(col("TIME")))
    
    # Convert to pandas for easier handling and database operations
    df_pandas = df.toPandas()
    
    # Normalize column names to match our database model
    df_pandas.columns = [column_name.lower().replace('?', '_question').replace(' ', '_') for column_name in df_pandas.columns]
    
    # Keep only the columns we need for our database model
    required_columns = ['raw_song', 'raw_artist', 'song_clean', 'artist_clean', 'callsign', 'time', 'time_unix', 'unique_id', 'combined', 'first_question']
    
    # Filter to only required columns that exist
    available_columns = [column_name for column_name in required_columns if column_name in df_pandas.columns]
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
    logger.info("Data processing results")
    if 'artist_clean' in df_pandas.columns and 'raw_artist' in df_pandas.columns:
        artist_count = len(df_pandas)
        logger.info("Artist name cleaning completed", extra={'processed_count': artist_count})
        sample_artists = df_pandas[['raw_artist', 'artist_clean']].drop_duplicates().head(5)
        for _, row in sample_artists.iterrows():
            logger.debug("Artist name cleaned", extra={
                'raw_artist': row['raw_artist'], 'clean_artist': row['artist_clean']
            })
    else:
        logger.warning("Artist cleaning columns not found")
        
    # Show song cleaning results
    if 'song_clean' in df_pandas.columns and 'raw_song' in df_pandas.columns:
        song_count = len(df_pandas)
        logger.info("Song title cleaning completed", extra={'processed_count': song_count})
        sample_songs = df_pandas[['raw_song', 'song_clean']].drop_duplicates().head(5)
        for _, row in sample_songs.iterrows():
            logger.debug("Song title cleaned", extra={
                'raw_song': row['raw_song'], 'clean_song': row['song_clean']
            })
    else:
        logger.warning("Song cleaning columns not found")
    
    # Show timestamp parsing results
    if 'time_unix' in df_pandas.columns and 'time' in df_pandas.columns:
        parsed_count = df_pandas['time_unix'].notna().sum()
        total_count = len(df_pandas)
        logger.info("Timestamp parsing completed", extra={
            'parsed_count': parsed_count, 'total_count': total_count
        })
        if parsed_count > 0:
            # Show a few examples
            sample_times = df_pandas[df_pandas['time_unix'].notna()][['time', 'time_unix']].head(3)
            for _, row in sample_times.iterrows():
                logger.debug("Timestamp parsed", extra={
                    'raw_time': row['time'], 'unix_time': row['time_unix']
                })
    else:
        logger.warning("Timestamp parsing columns not found")
    
    # Add placeholder columns for additional metadata
    df_pandas['artist_image_url'] = None
    df_pandas['artist_mbid'] = None
    df_pandas['artist_lastfm'] = None
    df_pandas['track_duration'] = None
    df_pandas['track_mbid'] = None
    df_pandas['song_lastfm'] = None
    
    logger.info("Pandas DataFrame created", extra={
        'shape': str(df_pandas.shape), 'columns': list(df_pandas.columns)
    })
    
    # Save initial data to database
    logger.info("Saving initial data to database")
    save_success = db_repo.save_dataframe_to_db(df_pandas, 'music_records')
    
    # Show unique cleaned artists (first 10)
    logger.debug("Showing unique artists sample")
    unique_artists_df = df.select("RAW_ARTIST", "ARTIST_CLEAN").distinct()
    unique_artists_df.show(10)
    
    logger.info("Starting artist image fetching from Last.fm API")
    
    # Get unique cleaned artists to avoid duplicate API calls
    # Use cleaned names as cache keys for deduplication
    # unique_artists_for_api = df.select("RAW_ARTIST", "ARTIST_CLEAN").distinct()
    unique_artists_for_api = df.select("RAW_ARTIST", "ARTIST_CLEAN").distinct().limit(10)  # Limit for demo purposes
    
    # Get count of unique artists for logging
    unique_count = unique_artists_for_api.count()
    logger.info("Processing unique artists for image URLs", extra={'unique_count': unique_count})
    unique_artists_for_api.show(10, truncate=False)
    
    # Convert to pandas to handle API calls with proper caching
    artists_pandas = unique_artists_for_api.toPandas()
    artists_pandas.columns = [col.lower() for col in artists_pandas.columns]
    
    # Add columns for image URL and MBID
    artists_pandas['artist_image_url'] = None
    artists_pandas['artist_mbid'] = None
    
    logger.info("Fetching and caching artist images")
    
    # Process each unique artist with optimized caching
    # Use cleaned name as cache key, but raw name for API lookup
    for _, row in artists_pandas.iterrows():
        artist_clean = row['artist_clean']
        artist_raw = row['raw_artist']
        
        # Fetch image URL, MBID, and canonical name
        logger.debug("Processing artist", extra={'raw_artist': artist_raw, 'cache_key': artist_clean})
        image_url, mbid, artist_lastfm = fetch_lastfm_artist_info(artist_clean, artist_raw)
        
        # Update the dataframe with all results
        artists_pandas.loc[artists_pandas['artist_clean'] == artist_clean, 'artist_image_url'] = image_url
        if mbid:  # Only update MBID if we have one
            artists_pandas.loc[artists_pandas['artist_clean'] == artist_clean, 'artist_mbid'] = mbid
            
        # Update main dataframe with canonical artist name from Last.fm
        if artist_lastfm:
            mask = df_pandas['artist_clean'] == artist_clean
            df_pandas.loc[mask, 'artist_lastfm'] = artist_lastfm
    
    # Fetch track information from Last.fm
    logger.info("Starting track information fetching from Last.fm API")
    
    # Get unique artist/song combinations for track info
    unique_tracks = df.select("RAW_ARTIST", "RAW_SONG", "ARTIST_CLEAN", "SONG_CLEAN").distinct().limit(20)  # Limit for testing
    
    # Get count of unique tracks for logging
    track_count = unique_tracks.count()
    logger.info("Processing unique tracks for duration and metadata", extra={'track_count': track_count})
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
                
            if track_info.get('artist_lastfm'):
                df_pandas.loc[mask, 'artist_lastfm'] = track_info['artist_lastfm']
                
            if track_info.get('song_lastfm'):
                df_pandas.loc[mask, 'song_lastfm'] = track_info['song_lastfm']
    
    # Save updated DataFrame with track durations back to database
    logger.info("Saving updated track duration data to database")
    save_success = db_repo.save_dataframe_to_db(df_pandas, 'music_records')
    
    if save_success:
        logger.info("Track duration data saved successfully")
        
        # Normalize artist names to handle "The" variants and fuzzy matches
        logger.info("Normalizing artist names for consistency (including fuzzy matching)")
        fuzzy_threshold = 90.0  # Adjust this value to be more/less strict (0-100)
        normalizations = artist_service.normalize_artist_names_in_db(fuzzy_threshold=fuzzy_threshold)
        logger.info("Artist normalization completed", extra={'normalizations': normalizations})
    else:
        logger.error("Failed to save track duration data")
    
    # Query database to show cached results
    stats_service.print_database_statistics()
    
    # Demonstrate pandas integration
    stats_service.demonstrate_pandas_integration()
    
    # hello world test (keeping original for reference)
    nums = sc.parallelize([1, 2, 3, 4])
    logger.debug("Original test - squares", extra={'result': nums.map(lambda x: x*x).collect()})
    
    logger.info("Application completed successfully")


if __name__ == "__main__":
    main()