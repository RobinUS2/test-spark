#!/usr/bin/env python3
"""
Data classes for API cache entries and parameters

These dataclasses replace long parameter lists and improve code maintainability
by providing type safety and clear structure for complex data.
"""

from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class ArtistCacheEntry:
    """Data class for artist image cache entries
    
    Replaces the long parameter list in cache_artist_image function
    """
    artist_name: str
    image_url: str
    success: bool = True
    error_message: Optional[str] = None
    mbid: Optional[str] = None
    artist_lastfm: Optional[str] = None


@dataclass
class TrackCacheEntry:
    """Data class for track information cache entries
    
    Replaces the long parameter list in cache_track_info function
    """
    artist_clean: str
    song_clean: str
    duration: Optional[int] = None
    track_mbid: Optional[str] = None
    success: bool = True
    error_message: Optional[str] = None
    artist_lastfm: Optional[str] = None
    song_lastfm: Optional[str] = None


@dataclass
class ArtistMatch:
    """Data class for fuzzy artist matching results"""
    original_name: str
    canonical_name: str
    similarity_score: float
    play_count: int


@dataclass
class DatabaseStats:
    """Data class for database statistics"""
    music_records_count: int
    records_with_timestamps: int
    cached_artists: int
    successful_images: int
    cached_tracks: int
    timestamp: datetime = datetime.utcnow()