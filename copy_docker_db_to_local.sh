#!/bin/bash
docker rm temp-container
rm music_data.db
docker build -t spark-music . && docker run --name temp-container spark-music:latest && docker cp temp-container:/app/music_data.db ./music_data.db && docker rm temp-container

# inspect the copied database file
sqlite3 music_data.db ".tables"
sqlite3 music_data.db ".schema"
sqlite3 music_data.db "SELECT * FROM music_records LIMIT 5;"
sqlite3 music_data.db "SELECT * FROM artist_images;"