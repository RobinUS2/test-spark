#!/usr/bin/env python3
"""
Simple CSV export script for the music database
"""

import sqlite3
import csv
import sys

def export_to_csv(db_path):
    """Export database tables to CSV files"""
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Export music_records table
        print("Exporting music_records to music_records.csv...")
        cursor.execute("SELECT * FROM music_records")
        
        with open('music_records.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            cursor.execute("PRAGMA table_info(music_records)")
            columns = [col[1] for col in cursor.fetchall()]
            writer.writerow(columns)
            
            # Write data
            cursor.execute("SELECT * FROM music_records")
            writer.writerows(cursor.fetchall())
        
        # Export artist_images table
        print("Exporting artist_images to artist_images.csv...")
        cursor.execute("SELECT * FROM artist_images")
        
        with open('artist_images.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            cursor.execute("PRAGMA table_info(artist_images)")
            columns = [col[1] for col in cursor.fetchall()]
            writer.writerow(columns)
            
            # Write data
            cursor.execute("SELECT * FROM artist_images")
            writer.writerows(cursor.fetchall())
        
        # Export track_info table
        print("Exporting track_info to track_info.csv...")
        cursor.execute("SELECT * FROM track_info")
        
        with open('track_info.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            cursor.execute("PRAGMA table_info(track_info)")
            columns = [col[1] for col in cursor.fetchall()]
            writer.writerow(columns)
            
            # Write data
            cursor.execute("SELECT * FROM track_info")
            writer.writerows(cursor.fetchall())
        
        # Export distinct artist names using COALESCE like other queries
        print("Exporting distinct artists to distinct_artists.csv...")
        
        with open('distinct_artists.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            writer.writerow(['artist_name', 'play_count', 'artist_clean', 'artist_lastfm'])
            
            # Write data - distinct artist names using COALESCE with play counts
            cursor.execute("""
                SELECT 
                    COALESCE(
                        CASE WHEN artist_lastfm IS NULL OR artist_lastfm = '' OR artist_lastfm = 'None' 
                             THEN NULL 
                             ELSE artist_lastfm 
                        END, 
                        artist_clean
                    ) as artist_name,
                    COUNT(*) as play_count,
                    MIN(artist_clean) as artist_clean,
                    MIN(artist_lastfm) as artist_lastfm
                FROM music_records 
                WHERE artist_clean IS NOT NULL 
                  AND artist_clean != ''
                  AND TRIM(artist_clean) != ''
                GROUP BY COALESCE(
                    CASE WHEN artist_lastfm IS NULL OR artist_lastfm = '' OR artist_lastfm = 'None' 
                         THEN NULL 
                         ELSE artist_lastfm 
                    END, 
                    artist_clean
                )
                ORDER BY COUNT(*) DESC, COALESCE(
                    CASE WHEN artist_lastfm IS NULL OR artist_lastfm = '' OR artist_lastfm = 'None' 
                         THEN NULL 
                         ELSE artist_lastfm 
                    END, 
                    artist_clean
                ) ASC
            """)
            writer.writerows(cursor.fetchall())
        
        conn.close()
        print("CSV export completed successfully!")
        print("   • music_records.csv")
        print("   • artist_images.csv")
        print("   • track_info.csv")
        print("   • distinct_artists.csv")
        
    except Exception as e:
        print(f"Export error: {e}")

if __name__ == "__main__":
    db_path = "music_data.db"
    
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    
    export_to_csv(db_path)