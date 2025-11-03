#!/usr/bin/env python3
"""Configuration settings for the music data pipeline"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class DatabaseConfig:
    type: str = field(default_factory=lambda: os.getenv('DATABASE_TYPE', 'sqlite'))
    sqlite_path: str = './music_data.db'
    
    # PostgreSQL config
    host: str = field(default_factory=lambda: os.getenv('DB_HOST', 'localhost'))
    port: int = field(default_factory=lambda: int(os.getenv('DB_PORT', '5432')))
    database: str = field(default_factory=lambda: os.getenv('DB_NAME', 'music_data'))
    username: str = field(default_factory=lambda: os.getenv('DB_USER', 'postgres'))
    password: str = field(default_factory=lambda: os.getenv('DB_PASSWORD', ''))


@dataclass
class ProcessingConfig:
    """Data processing configuration with explained magic numbers"""
    
    # SQLite transaction batch size (1000 = optimal for 37K+ records)
    batch_size: int = field(default_factory=lambda: int(os.getenv('BATCH_SIZE', '1000')))
    
    # Fuzzy matching threshold (90% catches duplicates without false matches, see test test_fuzzy_matching_accuracy)
    fuzzy_threshold: float = field(default_factory=lambda: float(os.getenv('FUZZY_THRESHOLD', '90.0')))
    
    # API rate limit (0.1s = 10 req/sec, under Last.fm limit)
    api_delay: float = field(default_factory=lambda: float(os.getenv('API_DELAY', '0.1')))
    
    # Conservative API call limit for dev/demo
    max_api_calls: int = field(default_factory=lambda: int(os.getenv('MAX_API_CALLS', '100')))


@dataclass
class APIConfig:
    lastfm_api_key: str = field(default_factory=lambda: os.getenv('LASTFM_API_KEY', ''))
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