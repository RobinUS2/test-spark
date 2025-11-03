#!/usr/bin/env python3
"""Artist name normalization and fuzzy matching service"""

import logging
from typing import List, Tuple, Dict
from sqlalchemy import text

from config.settings import settings
from repositories.database_repository import db_repo
from models.data_classes import ArtistMatch

try:
    from rapidfuzz import fuzz
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False
    logging.warning("rapidfuzz not available - fuzzy matching disabled")

logger = logging.getLogger(__name__)


class ArtistNormalizationService:
    
    def __init__(self):
        self.well_known_the_bands = {
            'rolling stones', 'beatles', 'who', 'police', 'doors', 
            'cars', 'guess who', 'doobie brothers', 'black crowes',
            'eagles', 'kinks', 'clash', 'cure', 'smiths'
        }
    
    def normalize_artist_names_in_db(self, fuzzy_threshold: float = None) -> int:
        """Normalize artist names using 'The' prefix rules and fuzzy matching"""
        if fuzzy_threshold is None:
            fuzzy_threshold = settings.processing.fuzzy_threshold
            
        engine = db_repo.get_database_engine()
        
        try:
            with engine.connect() as conn:
                logger.info("Starting enhanced artist name normalization", extra={
                    'fuzzy_threshold': fuzzy_threshold,
                    'fuzzy_available': RAPIDFUZZ_AVAILABLE
                })
                
                result = conn.execute(text("""
                    SELECT DISTINCT artist_clean, COUNT(*) as count 
                    FROM music_records 
                    WHERE artist_clean IS NOT NULL AND artist_clean != ''
                    GROUP BY artist_clean
                    ORDER BY COUNT(*) DESC
                """))
                
                artists = [(row[0], row[1]) for row in result.fetchall()]
                logger.info("Found artists to analyze", extra={'count': len(artists)})
                
                # Step 1: Handle "The" prefix normalizations (exact matches)
                the_normalizations = self._handle_the_prefix_variants(artists)
                
                # Step 2: Apply fuzzy matching for remaining artists
                fuzzy_normalizations = {}
                if RAPIDFUZZ_AVAILABLE and fuzzy_threshold > 0:
                    # Remove artists already handled by "The" logic
                    remaining_artists = []
                    processed_names = set()
                    
                    for name, count in artists:
                        # Check if this artist was already processed in "The" normalization
                        already_processed = False
                        for old_name, new_name in the_normalizations.items():
                            if name.lower() == old_name or name.lower() == new_name.lower():
                                already_processed = True
                                processed_names.add(name.lower())
                                break
                        
                        if not already_processed:
                            remaining_artists.append((name, count))
                    
                    logger.info("Running fuzzy matching", extra={
                        'remaining_artists': len(remaining_artists),
                        'threshold': fuzzy_threshold
                    })
                    
                    fuzzy_normalizations = self._find_fuzzy_artist_matches(remaining_artists, fuzzy_threshold)
                
                # Combine all normalizations
                all_normalizations = {**the_normalizations, **fuzzy_normalizations}
                
                # Step 3: Apply all normalizations to database
                total_updated = self._apply_artist_normalizations(conn, all_normalizations)
                
                conn.commit()
                
                logger.info("Artist name normalization completed", extra={
                    'the_normalizations': len(the_normalizations),
                    'fuzzy_normalizations': len(fuzzy_normalizations),
                    'total_normalizations': len(all_normalizations),
                    'total_records_updated': total_updated
                })
                
                return len(all_normalizations)
                
        except Exception as e:
            logger.error("Artist name normalization failed", extra={'error': str(e)})
            return 0
    
    def _find_fuzzy_artist_matches(self, artists: List[Tuple[str, int]], threshold: float) -> Dict[str, str]:
        """
        Find fuzzy matches between artist names using rapidfuzz.
        
        Args:
            artists: List of tuples (artist_name, play_count)
            threshold: Minimum similarity score to consider a match
            
        Returns:
            Dict mapping artist names to their preferred canonical form
        """
        if not RAPIDFUZZ_AVAILABLE:
            logger.warning("rapidfuzz not available - skipping fuzzy matching")
            return {}
        
        normalizations = {}
        processed = set()
        
        # Sort by play count (descending) so popular artists become canonical
        sorted_artists = sorted(artists, key=lambda x: x[1], reverse=True)
        
        for i, (artist, count) in enumerate(sorted_artists):
            if artist.lower() in processed:
                continue
                
            # Find similar artists
            matches = []
            for j, (other_artist, other_count) in enumerate(sorted_artists):
                if i == j or other_artist.lower() in processed:
                    continue
                    
                # Calculate similarity
                similarity = fuzz.ratio(artist.lower(), other_artist.lower())
                
                if similarity >= threshold:
                    matches.append((other_artist, other_count, similarity))
            
            if matches:
                # This artist has fuzzy matches
                all_variants = [(artist, count)] + matches
                
                # Choose canonical form using smart rules
                canonical = self._choose_canonical_artist_name(all_variants)
                
                # Mark all variants for normalization to canonical form
                for variant in all_variants:
                    variant_name = variant[0]
                    if variant_name.lower() != canonical.lower():
                        normalizations[variant_name.lower()] = canonical
                    processed.add(variant_name.lower())
                
                processed.add(canonical.lower())
                
                logger.info("Fuzzy match group found", extra={
                    'canonical': canonical,
                    'variants': [v[0] for v in all_variants],
                    'similarities': [v[2] if len(v) > 2 else 100.0 for v in all_variants]
                })
        
        return normalizations
    
    def _choose_canonical_artist_name(self, variants: List[Tuple]) -> str:
        """
        Choose the canonical form from a list of artist name variants.
        
        Args:
            variants: List of tuples (name, count, similarity_score?) - similarity_score is optional
            
        Returns:
            The preferred canonical name
        """
        # Check if any variant should have "The" prefix
        for variant in variants:
            name = variant[0]
            count = variant[1]
            # Handle both 2-tuple (name, count) and 3-tuple (name, count, similarity) cases
            
            base_name = name.lower().replace('the ', '', 1) if name.lower().startswith('the ') else name.lower()
            if base_name in self.well_known_the_bands:
                # Find the "The" version if it exists, otherwise create it
                for other_variant in variants:
                    variant_name = other_variant[0]
                    if variant_name.lower().startswith('the ') and variant_name.lower().replace('the ', '', 1) == base_name:
                        return variant_name
                # Create "The" version
                return f"The {name.title()}" if not name.lower().startswith('the ') else name
        
        # No special "The" handling needed, use the version with most plays
        return max(variants, key=lambda x: x[1])[0]
    
    def _handle_the_prefix_variants(self, artists: List[Tuple[str, int]]) -> Dict[str, str]:
        """Handle exact 'The' prefix variants (e.g., 'Beatles' vs 'The Beatles')"""
        normalizations = {}
        artist_dict = {name.lower(): (name, count) for name, count in artists}
        
        for artist_name, count in artists:
            if artist_name.lower().startswith('the '):
                base_name = artist_name[4:].strip()
                
                # Check if non-"The" version exists
                if base_name.lower() in artist_dict:
                    base_count = artist_dict[base_name.lower()][1]
                    
                    # Apply "The" prefix rules
                    preferred = self._choose_the_prefix_version(artist_name, count, base_name, base_count)
                    to_update = base_name if preferred == artist_name else artist_name
                    
                    if to_update.lower() not in normalizations:
                        normalizations[to_update.lower()] = preferred
                        logger.info("The prefix normalization", extra={
                            'preferred': preferred, 'to_update': to_update,
                            'the_count': count, 'base_count': base_count
                        })
        
        return normalizations
    
    def _choose_the_prefix_version(self, the_name: str, the_count: int, base_name: str, base_count: int) -> str:
        """Choose between 'The Artist' and 'Artist' versions based on smart rules"""
        if base_name.lower() in self.well_known_the_bands:
            return the_name  # Always use "The" version for well-known bands
        else:
            # Use version with more records
            return the_name if the_count >= base_count else base_name
    
    def _apply_artist_normalizations(self, conn, normalizations: Dict[str, str]) -> int:
        """Apply artist name normalizations to the database"""
        total_updated = 0
        
        for old_name_key, new_name in normalizations.items():
            # Update all records with this artist name (case insensitive)
            result = conn.execute(text("""
                UPDATE music_records 
                SET artist_clean = :new_name
                WHERE LOWER(artist_clean) = :old_name_key
            """), {'new_name': new_name, 'old_name_key': old_name_key})
            
            updated_count = result.rowcount
            total_updated += updated_count
            
            if updated_count > 0:
                logger.info("Updated artist records", extra={
                    'from': old_name_key, 'to': new_name, 'records_updated': updated_count
                })
        
        return total_updated


# Global service instance
artist_service = ArtistNormalizationService()