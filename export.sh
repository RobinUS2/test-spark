#!/bin/bash
# Script to export database tables to CSV files (use copy_docker_db_to_local.sh to get local copy of the DB first)
echo "removing old stuff"
rm -f music_records.csv artist_images.csv track_info.csv

.venv/bin/python export_db.py

# first few records
echo -e "\n---\n"
echo "music records.csv:" 
wc -l music_records.csv
head music_records.csv 
echo -e "\n---\n"

echo "artist images.csv:"
wc -l artist_images.csv
head artist_images.csv
echo -e "\n---\n"

echo "track info.csv:"
wc -l track_info.csv
head track_info.csv
echo -e "\n---\n"
