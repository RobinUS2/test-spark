#!/usr/bin/env python3
"""
Script to read and explore the music_data.db SQLite database
"""

import sqlite3
import pandas as pd
import sys

def explore_database(db_path):
    """Explore the SQLite database and display its contents"""
    
    try:
        # Connect to database
        conn = sqlite3.connect(db_path)
        
        print("🗄️  DATABASE EXPLORATION")
        print("=" * 50)
        
        # Get all tables
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        print(f"📊 Tables found: {len(tables)}")
        for table in tables:
            print(f"   • {table[0]}")
        
        print("\n" + "=" * 50)
        
        # Explore music_records table
        print("\n🎵 MUSIC RECORDS TABLE")
        print("-" * 30)
        
        # Get table info
        cursor.execute("PRAGMA table_info(music_records);")
        columns = cursor.fetchall()
        print("Columns:")
        for col in columns:
            print(f"   • {col[1]} ({col[2]})")
        
        # Get count
        cursor.execute("SELECT COUNT(*) FROM music_records;")
        count = cursor.fetchone()[0]
        print(f"\nTotal records: {count}")
        
        # Show sample data using pandas
        df_music = pd.read_sql_query("SELECT * FROM music_records LIMIT 10", conn)
        print("\nSample records:")
        print(df_music.to_string(index=False))
        
        # Explore artist_images table
        print("\n" + "=" * 50)
        print("\n🎭 ARTIST IMAGES TABLE")
        print("-" * 30)
        
        # Get table info
        cursor.execute("PRAGMA table_info(artist_images);")
        columns = cursor.fetchall()
        print("Columns:")
        for col in columns:
            print(f"   • {col[1]} ({col[2]})")
        
        # Get count
        cursor.execute("SELECT COUNT(*) FROM artist_images;")
        count = cursor.fetchone()[0]
        print(f"\nTotal cached artists: {count}")
        
        # Show sample data using pandas
        df_artists = pd.read_sql_query("SELECT * FROM artist_images", conn)
        print("\nCached artists:")
        print(df_artists.to_string(index=False))
        
        # Some analytics
        print("\n" + "=" * 50)
        print("\n📈 ANALYTICS")
        print("-" * 30)
        
        # Unique artists in music records
        cursor.execute("SELECT DISTINCT raw_artist FROM music_records;")
        unique_artists = cursor.fetchall()
        print(f"Unique artists in music records: {len(unique_artists)}")
        for artist in unique_artists:
            print(f"   • {artist[0]}")
        
        # Records per callsign
        df_callsign = pd.read_sql_query("""
            SELECT callsign, COUNT(*) as record_count 
            FROM music_records 
            GROUP BY callsign 
            ORDER BY record_count DESC
        """, conn)
        print(f"\nRecords per callsign:")
        print(df_callsign.to_string(index=False))
        
        # Success rate of image fetching
        cursor.execute("SELECT fetch_success, COUNT(*) FROM artist_images GROUP BY fetch_success;")
        success_stats = cursor.fetchall()
        print(f"\nImage fetch success rate:")
        for stat in success_stats:
            status = "Success" if stat[0] else "Failed"
            print(f"   • {status}: {stat[1]}")
        
        conn.close()
        print(f"\n✅ Database exploration completed!")
        
    except Exception as e:
        print(f"❌ Error reading database: {e}")
        return False
    
    return True

def interactive_query(db_path):
    """Allow user to run custom SQL queries"""
    
    conn = sqlite3.connect(db_path)
    
    print("\n" + "=" * 50)
    print("🔍 INTERACTIVE QUERY MODE")
    print("-" * 30)
    print("Enter SQL queries (type 'exit' to quit):")
    print("Examples:")
    print("  SELECT * FROM music_records WHERE raw_artist LIKE '%.38%';")
    print("  SELECT artist_name, image_url FROM artist_images;")
    print("")
    
    while True:
        try:
            query = input("SQL> ").strip()
            
            if query.lower() == 'exit':
                break
                
            if not query:
                continue
                
            # Execute query and show results
            if query.upper().startswith('SELECT'):
                df = pd.read_sql_query(query, conn)
                print(df.to_string(index=False))
            else:
                cursor = conn.cursor()
                cursor.execute(query)
                print(f"Query executed. Rows affected: {cursor.rowcount}")
                
        except Exception as e:
            print(f"❌ Query error: {e}")
    
    conn.close()
    print("👋 Goodbye!")

if __name__ == "__main__":
    db_path = "music_data.db"
    
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    
    print(f"Reading database: {db_path}")
    
    if explore_database(db_path):
        # Ask if user wants interactive mode
        response = input("\nWould you like to run custom queries? (y/n): ").strip().lower()
        if response in ['y', 'yes']:
            interactive_query(db_path)