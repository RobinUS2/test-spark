#!/usr/bin/env python3
"""
MusicBrainz API integration for artist images
"""

import logging
from typing import Optional

import requests

logger = logging.getLogger(__name__)


def fetch_musicbrainz_image(mbid: str) -> Optional[str]:
    """
    Fetch artist image from MusicBrainz Cover Art Archive using MBID
    
    Args:
        mbid (str): MusicBrainz artist ID
        
    Returns:
        str: Image URL or None if not found
    """
    if not mbid:
        return None
    
    logger.debug("Fetching image from MusicBrainz", extra={'mbid': mbid})
    
    try:
        # MusicBrainz Cover Art Archive API
        # Note: This gets album covers, not artist photos
        # For artist photos, we'd need to use different services
        url = f"https://coverartarchive.org/artist/{mbid}"
        
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            
            # Look for images in the response
            if 'images' in data and len(data['images']) > 0:
                # Get the first image (usually the primary one)
                image_data = data['images'][0]
                if 'image' in image_data:
                    image_url = image_data['image']
                    logger.info("Found image in MusicBrainz", extra={'mbid': mbid, 'url': image_url[:50]})
                    return image_url
        
        logger.debug("No image found in MusicBrainz", extra={'mbid': mbid})
        return None
        
    except requests.exceptions.RequestException as e:
        logger.warning("MusicBrainz API error", extra={
            'mbid': mbid,
            'error': str(e)[:100]
        })
        return None