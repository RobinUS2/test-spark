#!/usr/bin/env python3
"""
Package initialization for services module
"""

from .artist_normalization_service import ArtistNormalizationService, artist_service
from .statistics_service import StatisticsService, stats_service

__all__ = ['ArtistNormalizationService', 'artist_service', 'StatisticsService', 'stats_service']