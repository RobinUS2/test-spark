#!/usr/bin/env python3
"""
Migrate data from backup database to new database with updated schema
"""

import sqlite3
import os

def migrate_data():
    """Migrate data from backup database to new database with updated schema"""
    
    backup_db = 'music_data_backup.db'
    new_db = 'music_data.db'
    
    if not os.path.exists(backup_db):
        print(f"❌ Backup database {backup_db} not found!")
        return
    
    if not os.path.exists(new_db):
        print(f"❌ New database {new_db} not found!")
        return
    
    print("🔄 Migrating data from backup to new database...")
    
    # Connect to both databases
    backup_conn = sqlite3.connect(backup_db)
    new_conn = sqlite3.connect(new_db)
    
    try:
        # Migrate music_records
        print("📀 Migrating music_records...")
        backup_cursor = backup_conn.cursor()
        new_cursor = new_conn.cursor()
        
        # Get data from backup (old schema)
        backup_cursor.execute("""
            SELECT raw_song, raw_artist, artist_clean, callsign, time, 
                   time_unix, unique_id, combined, first_play, 
                   artist_image_url, artist_mbid, created_at, updated_at
            FROM music_records
        """)
        
        records = backup_cursor.fetchall()
        
        # Insert into new database (with new columns as NULL for now)
        new_cursor.executemany("""
            INSERT INTO music_records 
            (raw_song, raw_artist, song_clean, artist_clean, callsign, time, 
             time_unix, unique_id, combined, first_play, artist_image_url, 
             artist_mbid, track_duration, track_mbid, created_at, updated_at)
            VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
        """, records)
        
        music_count = len(records)
        print(f"   ✅ Migrated {music_count} music records")
        
        # Migrate artist_images
        print("🎭 Migrating artist_images...")
        backup_cursor.execute("SELECT * FROM artist_images")
        artist_records = backup_cursor.fetchall()
        
        # Get column names for artist_images
        backup_cursor.execute("PRAGMA table_info(artist_images)")
        columns = [col[1] for col in backup_cursor.fetchall()]
        
        if artist_records:
            placeholders = ','.join(['?' for _ in columns])
            new_cursor.executemany(
                f"INSERT INTO artist_images ({','.join(columns)}) VALUES ({placeholders})",
                artist_records
            )
        
        artist_count = len(artist_records)
        print(f"   ✅ Migrated {artist_count} artist images")
        
        # Commit changes
        new_conn.commit()
        
        print(f"\n🎉 Migration completed successfully!")
        print(f"   📀 Music records: {music_count}")
        print(f"   🎭 Artist images: {artist_count}")
        print(f"   🆕 New columns (song_clean, track_duration, track_mbid) ready for processing")
        
    except Exception as e:
        print(f"❌ Migration error: {e}")
        new_conn.rollback()
    
    finally:
        backup_conn.close()
        new_conn.close()

if __name__ == "__main__":
    migrate_data()