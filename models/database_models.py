#!/usr/bin/env python3
"""SQLAlchemy database models"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class MusicRecord(Base):
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