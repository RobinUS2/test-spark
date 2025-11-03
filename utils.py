#!/usr/bin/env python3
"""
Utility functions for data cleaning, parsing, and logging configuration
"""

import re
import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from pyspark.sql import SparkSession


# Configure structured logging with custom formatter
class StructuredFormatter(logging.Formatter):
    def format(self, record):
        # Start with the basic format
        log_message = super().format(record)
        
        # Add extra data if present
        extra_data = {}
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 'filename',
                          'module', 'exc_info', 'exc_text', 'stack_info', 'lineno', 'funcName',
                          'created', 'msecs', 'relativeCreated', 'thread', 'threadName',
                          'processName', 'process', 'message', 'asctime']:
                extra_data[key] = value
        
        if extra_data:
            extra_str = " | ".join([f"{k}={v}" for k, v in extra_data.items()])
            log_message += f" | {extra_str}"
            
        return log_message


def setup_logging():
    """Set up logging with custom structured formatter"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create console handler with structured formatter
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = StructuredFormatter(
        fmt='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Prevent duplicate logs from root logger
    logger.propagate = False
    
    return logger


def parse_time_to_unix(time_str: str) -> Optional[int]:
    """
    Parse various time formats to Unix timestamp
    
    Args:
        time_str (str): Time string in various formats
        
    Returns:
        int: Unix timestamp in seconds, or None if parsing fails
    """
    if not time_str or pd.isna(time_str):
        return None
    
    # Convert to string and strip
    time_str = str(time_str).strip()
    
    # Try different time formats
    time_formats = [
        "%Y-%m-%d %H:%M:%S",     # 2023-01-01 12:30:45
        "%m/%d/%Y %H:%M:%S",     # 01/01/2023 12:30:45
        "%m/%d/%Y %I:%M:%S %p",  # 01/01/2023 12:30:45 PM
        "%Y-%m-%d %H:%M",        # 2023-01-01 12:30
        "%m/%d/%Y %H:%M",        # 01/01/2023 12:30
        "%Y-%m-%d",              # 2023-01-01
        "%m/%d/%Y",              # 01/01/2023
    ]
    
    for fmt in time_formats:
        try:
            dt = datetime.strptime(time_str, fmt)
            return int(dt.timestamp())
        except ValueError:
            continue
    
    # If no format worked, try to parse as Unix timestamp
    try:
        return int(float(time_str))
    except ValueError:
        return None


def clean_artist_name(raw_artist: str) -> str:
    """
    Clean and standardize artist names for better matching and API calls
    
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
    
    # Trim surrounding quotes (single and double)
    artist = artist.strip('\'"')
    
    # Remove extra whitespace (multiple spaces, tabs, newlines)
    artist = re.sub(r'\s+', ' ', artist)
    
    # Common artist name cleanup
    replacements = {
        # Remove or standardize common patterns
        r'\s*\(.*?\)\s*': '',  # Remove content in parentheses
        r'\s*\[.*?\]\s*': '',  # Remove content in brackets
        r'\s*\{.*?\}\s*': '',  # Remove content in braces
        
        # Standardize featuring patterns
        r'\s+FEAT\.?\s+': ' FEAT. ',
        r'\s+FEATURING\s+': ' FEAT. ',
        r'\s+FT\.?\s+': ' FEAT. ',
        r'\s+W/\s+': ' FEAT. ',
        r'\s+WITH\s+': ' FEAT. ',
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
    
    # Normalize "The" prefix for consistent grouping
    # For well-known bands, ensure consistent "The" usage
    base_name = artist.lower().replace('the ', '', 1) if artist.lower().startswith('the ') else artist.lower()
    
    # Bands that should always have "The" prefix
    bands_with_the = {
        'rolling stones': 'The Rolling Stones',
        'beatles': 'The Beatles', 
        'who': 'The Who',
        'police': 'The Police',
        'doors': 'The Doors',
        'cars': 'The Cars',
        'guess who': 'The Guess Who',
        'doobie brothers': 'The Doobie Brothers',
        'black crowes': 'The Black Crowes'
    }
    
    if base_name in bands_with_the:
        artist = bands_with_the[base_name]
    
    # Title case handling - be smart about it
    # First, handle common words that should stay lowercase
    lowercase_words = {
        'a', 'an', 'and', 'as', 'at', 'but', 'by', 'for', 'if', 'in', 'is',
        'it', 'nor', 'of', 'on', 'or', 'so', 'the', 'to', 'up', 'yet', 'ft',
        'feat', 'vs', 'v', 'vs.', 'with', 'w/', 'n', 'da', 'de', 'del', 'la',
        'le', 'les', 'los', 'las', 'el', 'il', 'lo'
    }
    
    # Split into words and apply title case rules
    words = artist.split()
    result_words = []
    
    for i, word in enumerate(words):
        # Always capitalize first and last word
        if i == 0 or i == len(words) - 1:
            result_words.append(word.capitalize())
        # Check if it's a lowercase word (but not if it's all caps originally)
        elif word.lower() in lowercase_words and not word.isupper():
            result_words.append(word.lower())
        else:
            result_words.append(word.capitalize())
    
    artist = ' '.join(result_words)
    
    # Handle special cases
    # Fix common abbreviations and proper nouns
    artist = re.sub(r'\bDj\b', 'DJ', artist, flags=re.IGNORECASE)
    artist = re.sub(r'\bMc([A-Z])', r'Mc\1', artist)
    artist = re.sub(r'\bMac([A-Z])', r'Mac\1', artist)
    
    # Fix ordinal numbers
    artist = re.sub(r'\b(\d+)(st|nd|rd|th)\b', r'\1\2', artist, flags=re.IGNORECASE)

    # Note: Consider using a dedicated name parsing library for more sophisticated cleaning
    # Additional cleanup patterns like " (Live)" could be added as needed
    
    return artist


def clean_song_title(raw_song: str) -> str:
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
    
    # Trim surrounding quotes (single and double)
    song = song.strip('\'"')
    
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
        r'\s*\(clean\)\s*$': '',
        r'\s*\(Clean\)\s*$': '',
        r'\s*\(explicit\)\s*$': '',
        r'\s*\(Explicit\)\s*$': '',
        r'\s*\(remix\)\s*$': '',
        r'\s*\(Remix\)\s*$': '',
        r'\s*\(REMIX\)\s*$': '',
        
        # Standardize featuring patterns
        r'\s+FEAT\.?\s+': ' FEAT. ',
        r'\s+FEATURING\s+': ' FEAT. ',
        r'\s+FT\.?\s+': ' FEAT. ',
        r'\s+W/\s+': ' FEAT. ',
        r'\s+WITH\s+': ' FEAT. ',
        r'\bFEAT\s+': 'FEAT. ',
        r'\bFEATURING\s+': 'FEAT. ',
        
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
        
        # Remove leading/trailing punctuation and spaces
        r'^[\.\-\s]+': '',
        r'[\.\-\s]+$': '',
    }
    
    # Apply replacements
    for pattern, replacement in replacements.items():
        song = re.sub(pattern, replacement, song, flags=re.IGNORECASE)
    
    # Title case handling
    # First word and words after certain punctuation should be capitalized
    def title_case_song(text):
        # Split on spaces but keep track of punctuation
        words = text.split()
        result = []
        
        for i, word in enumerate(words):
            # Always capitalize first word
            if i == 0:
                result.append(word.capitalize())
            # Capitalize words after certain punctuation in previous words
            elif any(punct in (result[i-1] if result else '') for punct in [':', '!', '?', '.']):
                result.append(word.capitalize())
            # Keep small connecting words lowercase unless they're first
            elif word.lower() in ['a', 'an', 'and', 'as', 'at', 'but', 'by', 'for', 'if', 'in', 'is',
                                'it', 'nor', 'of', 'on', 'or', 'so', 'the', 'to', 'up', 'yet', 'with']:
                result.append(word.lower())
            else:
                result.append(word.capitalize())
        
        return ' '.join(result)
    
    song = title_case_song(song)
    
    # Handle special cases and abbreviations
    song = re.sub(r'\bDj\b', 'DJ', song, flags=re.IGNORECASE)
    song = re.sub(r'\bVs\b', 'vs', song, flags=re.IGNORECASE)
    song = re.sub(r'\bFeat\b', 'feat', song, flags=re.IGNORECASE)
    
    return song


def init_spark():
    """Initialize Spark session and context"""
    spark = SparkSession.builder \
        .appName("Music Data Processing") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    sc = spark.sparkContext
    sc.setLogLevel("WARN")  # Reduce Spark logging noise
    
    return spark, sc