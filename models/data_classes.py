#!/usr/bin/env python3
"""Data classes for API cache entries and parameters"""

from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class ArtistCacheEntry:
    artist_name: str
    image_url: str
    success: bool = True
    error_message: Optional[str] = None
    mbid: Optional[str] = None
    artist_lastfm: Optional[str] = None


@dataclass
class TrackCacheEntry:
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
    original_name: str
    canonical_name: str
    similarity_score: float
    play_count: int


@dataclass
class DatabaseStats:
    music_records_count: int
    records_with_timestamps: int
    cached_artists: int
    successful_images: int
    cached_tracks: int
    timestamp: datetime = datetime.utcnow()