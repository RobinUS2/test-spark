#!/bin/bash
# Script to export database tables to CSV files (use copy_docker_db_to_local.sh to get local copy of the DB first)
echo "removing old stuff"
rm -f music_records.csv artist_images.csv track_info.csv distinct_artists.csv

.venv/bin/python export_db.py

# first few records
echo -e "\n---\n"
echo "music records.csv:" 
echo "$(( $(wc -l < music_records.csv) - 1 )) data rows (excluding header)"
head music_records.csv 
echo -e "\n---\n"

echo "artist images.csv:"
echo "$(( $(wc -l < artist_images.csv) - 1 )) data rows (excluding header)"
head artist_images.csv
echo -e "\n---\n"

echo "track info.csv:"
echo "$(( $(wc -l < track_info.csv) - 1 )) data rows (excluding header)"
head track_info.csv
echo -e "\n---\n"

echo "distinct artists.csv:"
echo "$(( $(wc -l < distinct_artists.csv) - 1 )) data rows (excluding header)"
head distinct_artists.csv
echo -e "\n---\n"