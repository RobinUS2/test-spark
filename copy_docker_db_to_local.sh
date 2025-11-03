#!/bin/bash
docker rm temp-container
rm music_data.db

# Check if LASTFM_API_KEY is set and pass it to Docker
if [ -n "$LASTFM_API_KEY" ]; then
    echo "Using Last.fm API key from environment variable"
    docker build -t spark-music . && docker run --name temp-container -e LASTFM_API_KEY="$LASTFM_API_KEY" spark-music:latest && docker cp temp-container:/app/music_data.db ./music_data.db && docker rm temp-container
else
    echo "No Last.fm API key found in environment - running without API features"
    docker build -t spark-music . && docker run --name temp-container spark-music:latest && docker cp temp-container:/app/music_data.db ./music_data.db && docker rm temp-container
fi

# inspect the copied database file
sqlite3 music_data.db ".tables"
sqlite3 music_data.db ".schema"
sqlite3 music_data.db "SELECT * FROM music_records LIMIT 5;"
sqlite3 music_data.db "SELECT * FROM artist_images;"