#!/usr/bin/env python3
"""
Last.fm API integration for artist and track information
"""

import json
import os
import time
import logging
from typing import Optional, Tuple, Dict, Any

import requests

from database import cache_artist_image, cache_track_info, get_artist_image_from_cache, get_track_info_from_cache, get_database_engine, ArtistImage
from sqlalchemy.orm import sessionmaker
from musicbrainz_api import fetch_musicbrainz_image

logger = logging.getLogger(__name__)

# Last.fm API configuration
LASTFM_API_KEY = os.getenv('LASTFM_API_KEY', '')  # Fallback for demo purposes

if not LASTFM_API_KEY or LASTFM_API_KEY == 'your_api_key_here':
    logger.warning("LASTFM_API_KEY not properly configured. Set the LASTFM_API_KEY environment variable for production use.")


def fetch_lastfm_artist_info(artist_clean_name: str, artist_full_name: Optional[str] = None) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Fetch artist image URL, MBID, and canonical name from Last.fm API with MusicBrainz fallback
    
    Args:
        artist_clean_name (str): Cleaned artist name used as cache key
        artist_full_name (str): Full/original artist name used for API lookup (optional)
        
    Returns:
        tuple: (image_url, mbid, artist_lastfm) where any can be None
    """
    if not artist_clean_name or artist_clean_name.strip() == "":
        return ("No artist name provided", None, None)
    
    # Use full name for API lookup if provided, otherwise use clean name
    lookup_name = artist_full_name if artist_full_name else artist_clean_name
    
    # Check cache first using cleaned name as key
    cached_result = get_artist_image_from_cache(artist_clean_name)
    if cached_result is not None:
        logger.debug("Artist cache hit", extra={'artist': artist_clean_name})
        # Get MBID and canonical name from cache too        
        engine = get_database_engine()
        Session = sessionmaker(bind=engine)
        session = Session()
        try:
            cached_record = session.query(ArtistImage).filter(
                ArtistImage.artist_name == artist_clean_name.strip()
            ).first()
            cached_mbid = cached_record.mbid if cached_record else None
            cached_artist_lastfm = cached_record.artist_lastfm if cached_record else None
            return (cached_result, cached_mbid, cached_artist_lastfm)
        finally:
            session.close()
    
    logger.info("Fetching from Last.fm API", extra={'artist': lookup_name, 'cache_key': artist_clean_name})
    
    # @todo retry and backoff logic could be added here for robustness
    try:
        # Last.fm API endpoint for artist.getInfo
        url = "https://ws.audioscrobbler.com/2.0/"
        params = {
            'method': 'artist.getinfo',
            'artist': lookup_name.strip(),
            'api_key': LASTFM_API_KEY,
            'format': 'json'
        }
        
        # Make the API request with timeout
        time.sleep(0.25)  # Be nice to the API
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        # Debug: Log the full API response structure to see available fields
        mbid = None
        artist_lastfm = None
        if 'artist' in data:
            artist_data = data['artist']
            logger.debug("Last.fm API response fields", extra={'artist': lookup_name, 'fields': list(artist_data.keys())})
            
            # Extract canonical artist name from Last.fm
            if 'name' in artist_data and artist_data['name']:
                artist_lastfm = artist_data['name'].strip()
                logger.info("Found canonical artist name from Last.fm", extra={
                    'original': lookup_name, 'canonical': artist_lastfm
                })
            
            # Check for MBID (MusicBrainz ID)
            if 'mbid' in artist_data and artist_data['mbid']:
                mbid = artist_data['mbid']
                logger.info("Found MBID from Last.fm", extra={'artist': lookup_name, 'mbid': mbid})
                
                # Try to fetch image from MusicBrainz first
                mb_image_url = fetch_musicbrainz_image(mbid)
                if mb_image_url:
                    cache_artist_image(artist_clean_name, mb_image_url, success=True, mbid=mbid, artist_lastfm=artist_lastfm)
                    return (mb_image_url, mbid, artist_lastfm)
                else:
                    logger.debug("MusicBrainz had no image, falling back to Last.fm", extra={'artist': lookup_name})
            else:
                logger.debug("No MBID found in Last.fm response", extra={'artist': lookup_name})
        
        # Check if artist data exists and try Last.fm images as fallback
        if 'artist' in data and 'image' in data['artist']:
            images = data['artist']['image']
            # Get the largest image (usually the last one)
            for img in reversed(images):
                if img.get('#text') and img.get('#text').strip():
                    image_url = img['#text']
                    # Cache successful result using cleaned name as key
                    cache_artist_image(artist_clean_name, image_url, success=True, mbid=mbid, artist_lastfm=artist_lastfm)
                    return (image_url, mbid, artist_lastfm)
            
            # No image found but artist exists
            no_image_msg = "No image found"
            cache_artist_image(artist_clean_name, no_image_msg, success=False, error_message="No image available", mbid=mbid, artist_lastfm=artist_lastfm)
            logger.warning("No image found for artist", extra={'artist': lookup_name})
            return (no_image_msg, mbid, artist_lastfm)
        else:
            # Artist not found
            not_found_msg = f"Artist not found: {lookup_name}"
            cache_artist_image(artist_clean_name, not_found_msg, success=False, error_message="Artist not found", mbid=mbid, artist_lastfm=artist_lastfm)
            logger.warning("Artist not found in Last.fm", extra={'artist': lookup_name})
            return (not_found_msg, mbid, artist_lastfm)
            
    except requests.exceptions.Timeout:
        timeout_msg = f"Timeout for artist: {lookup_name}"
        cache_artist_image(artist_clean_name, timeout_msg, success=False, error_message="API timeout")
        logger.error("API timeout", extra={'artist': lookup_name})
        return (timeout_msg, None, None)
    except requests.exceptions.RequestException as e:
        error_msg = f"Request error for {lookup_name}: {str(e)[:100]}"
        cache_artist_image(artist_clean_name, error_msg, success=False, error_message=str(e)[:100])
        logger.error("API request error", extra={'artist': lookup_name, 'error': str(e)[:100]})
        return (error_msg, None, None)
    except json.JSONDecodeError:
        json_error_msg = f"Invalid JSON response for {lookup_name}"
        cache_artist_image(artist_clean_name, json_error_msg, success=False, error_message="Invalid JSON response")
        logger.error("Invalid JSON response", extra={'artist': lookup_name})
        return (json_error_msg, None, None)


def fetch_lastfm_track_info(artist_clean: str, song_clean: str, raw_artist: Optional[str] = None, raw_song: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Fetch track information from Last.fm API including duration, MBID, and canonical names
    
    Args:
        artist_clean (str): Cleaned artist name used as cache key
        song_clean (str): Cleaned song title used as cache key
        raw_artist (str): Original artist name for API lookup (optional)
        raw_song (str): Original song title for API lookup (optional)
        
    Returns:
        dict: Track info with duration, track_mbid, artist_lastfm, song_lastfm or None if failed
    """
    if not artist_clean or not song_clean or artist_clean.strip() == "" or song_clean.strip() == "":
        return None
    
    # Check cache first
    cached_result = get_track_info_from_cache(artist_clean, song_clean)
    if cached_result is not None:
        logger.debug("Track cache hit", extra={'artist': artist_clean, 'song': song_clean})
        return cached_result
    
    # Use raw names for API lookup if provided, otherwise use clean names
    lookup_artist = raw_artist if raw_artist else artist_clean
    lookup_song = raw_song if raw_song else song_clean
    
    logger.info("Fetching track info from Last.fm", extra={'artist': lookup_artist, 'song': lookup_song})
    
    try:
        # Last.fm API endpoint for track.getInfo
        url = "https://ws.audioscrobbler.com/2.0/"
        params = {
            'method': 'track.getinfo',
            'artist': lookup_artist.strip(),
            'track': lookup_song.strip(),
            'api_key': LASTFM_API_KEY,
            'format': 'json'
        }
        
        # Make the API request with timeout
        time.sleep(0.25)  # Be nice to the API
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if 'track' in data:
            track_data = data['track']
            
            # Extract canonical names from Last.fm
            artist_lastfm = None
            song_lastfm = None
            if 'artist' in track_data and 'name' in track_data['artist']:
                artist_lastfm = track_data['artist']['name'].strip()
            if 'name' in track_data:
                song_lastfm = track_data['name'].strip()
            
            if artist_lastfm or song_lastfm:
                logger.info("Found canonical names from Last.fm", extra={
                    'original_artist': lookup_artist, 'canonical_artist': artist_lastfm,
                    'original_song': lookup_song, 'canonical_song': song_lastfm
                })
            
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
                logger.info("Found track duration", extra={
                    'artist': lookup_artist, 'song': lookup_song,
                    'duration_seconds': duration, 'duration_display': f"{minutes}:{seconds:02d}"
                })
            if track_mbid:
                logger.info("Found track MBID", extra={
                    'artist': lookup_artist, 'song': lookup_song, 'track_mbid': track_mbid
                })
            
            # Cache the result
            result = {
                'duration': duration,
                'track_mbid': track_mbid,
                'artist_lastfm': artist_lastfm,
                'song_lastfm': song_lastfm
            }
            cache_track_info(artist_clean, song_clean, duration=duration, 
                           track_mbid=track_mbid, artist_lastfm=artist_lastfm, 
                           song_lastfm=song_lastfm, success=True)
            return result
        else:
            logger.warning("Track not found in Last.fm", extra={'artist': lookup_artist, 'song': lookup_song})
            cache_track_info(artist_clean, song_clean, success=False, 
                           error_message="Track not found")
            return None
            
    except requests.exceptions.Timeout:
        logger.error("API timeout for track", extra={'artist': lookup_artist, 'song': lookup_song})
        cache_track_info(artist_clean, song_clean, success=False, 
                       error_message="API timeout")
        return None
    except requests.exceptions.RequestException as e:
        logger.error("API request error for track", extra={
            'artist': lookup_artist, 'song': lookup_song, 'error': str(e)[:100]
        })
        cache_track_info(artist_clean, song_clean, success=False, 
                       error_message=str(e)[:100])
        return None
    except json.JSONDecodeError:
        logger.error("Invalid JSON response for track", extra={'artist': lookup_artist, 'song': lookup_song})
        cache_track_info(artist_clean, song_clean, success=False, 
                       error_message="Invalid JSON response")
        return None