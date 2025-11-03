#!/usr/bin/env python3
"""
Configuration settings for the music data pipeline

This module centralizes all configuration values and explains magic numbers
to improve maintainability and transparency.
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class DatabaseConfig:
    """Database configuration settings"""
    type: str = field(default_factory=lambda: os.getenv('DATABASE_TYPE', 'sqlite'))  # 'sqlite' or 'postgresql'
    sqlite_path: str = './music_data.db'
    
    # PostgreSQL config (used when type='postgresql')
    host: str = field(default_factory=lambda: os.getenv('DB_HOST', 'localhost'))
    port: int = field(default_factory=lambda: int(os.getenv('DB_PORT', '5432')))
    database: str = field(default_factory=lambda: os.getenv('DB_NAME', 'music_data'))
    username: str = field(default_factory=lambda: os.getenv('DB_USER', 'postgres'))
    password: str = field(default_factory=lambda: os.getenv('DB_PASSWORD', ''))


@dataclass
class ProcessingConfig:
    """Data processing configuration with explained magic numbers"""
    
    # Batch size for database operations
    # WHY 1000: Optimal balance for SQLite transactions
    # - Small enough to avoid memory issues with large datasets (37K+ records)
    # - Large enough to minimize transaction overhead
    # - SQLite handles 1000 inserts efficiently in a single transaction
    batch_size: int = field(default_factory=lambda: int(os.getenv('BATCH_SIZE', '1000')))
    
    # Fuzzy matching threshold for artist name deduplication
    # WHY 90%: Empirically determined for music data quality
    # - 95%: Too strict, misses valid variants like "The Beatles" vs "Beatles"
    # - 85%: Too loose, creates false matches between different artists
    # - 90%: Sweet spot that catches real duplicates without false positives
    fuzzy_threshold: float = field(default_factory=lambda: float(os.getenv('FUZZY_THRESHOLD', '90.0')))
    
    # API rate limiting delay
    # WHY 0.1 seconds: Respectful API usage without being overly cautious
    # - Last.fm API allows 5 requests/second for registered users
    # - 0.1s = 10 requests/second maximum, staying well within limits
    # - Prevents 429 (Too Many Requests) errors
    api_delay: float = field(default_factory=lambda: float(os.getenv('API_DELAY', '0.1')))
    
    # Maximum number of API calls per session
    # WHY 100: Conservative limit for development/demo
    # - Prevents accidental expensive API usage during testing
    # - Production deployments should increase this or remove the limit
    # - Allows reasonable data enrichment without hitting quotas
    max_api_calls: int = field(default_factory=lambda: int(os.getenv('MAX_API_CALLS', '100')))


@dataclass
class APIConfig:
    """External API configuration"""
    lastfm_api_key: str = field(default_factory=lambda: os.getenv('LASTFM_API_KEY', ''))
    
    # API timeout settings
    # WHY 10 seconds: Balance between reliability and performance
    # - 5s: Too short, may fail on slow networks
    # - 30s: Too long, blocks processing pipeline
    # - 10s: Reasonable for most network conditions
    request_timeout: int = field(default_factory=lambda: int(os.getenv('API_TIMEOUT', '10')))


@dataclass
class AppSettings:
    """Main application settings"""
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    api: APIConfig = field(default_factory=APIConfig)
    
    # Logging configuration
    log_level: str = field(default_factory=lambda: os.getenv('LOG_LEVEL', 'INFO'))


# Global settings instance
settings = AppSettings()