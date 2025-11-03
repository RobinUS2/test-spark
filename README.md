# random notes
# local testing
brew install openjdk@17 
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
python -c "import os; print(os.environ.get('JAVA_HOME'))"
python hello.py

# run locally
docker run -it apache/spark-py /opt/spark/bin/pyspark

# build and run in one go
docker run --rm -it $(docker build -q .)

## Usage
./setup_db.sh sqlite # to configure config for local usage
./copy_docker_db_to_local.sh
./export.sh will copy file to local for inspection
simple sqlite for now, given the filesize, switching to postgres would be simple

## Architecture
Local docker setup for portability, yet simplicity. Could've done something full cloud / docker compose, but given the data set size and time constraints would like to keep it relatively simple, yet prepare for later scale increase; hence Spark as distributed processing framework. 

Sqlalchemy for DB connection with pandas. Into a local SQLite given the amount of data again, yet a configuration tool to be able to switch to more persistent and scalable Postgresin a production (cloud) environment. 

Implemented the dual layer image lookup with caching via database (to reduce API calls, in production might want something like redis instead of SQL for caching; albeit easier to debug now, hence keeping that). 

## Image fetching
Via lastFm to MusicBrainz (LastFm stopped exposing images), bit of a little nightmare, but works here and there.
```
🌐 Fetching for 'Creed' (cache key: 'Creed')
🌐 Fetching from Last.fm API for artist: Creed (cache key: Creed)
🔍 Last.fm API fields for 'Creed': ['name', 'mbid', 'url', 'image', 'streamable', 'ontour', 'stats', 'similar', 'tags', 'bio']
🎵 Found MBID for 'Creed': 4b1a830b-0a1f-42e5-b8d5-1d6743912e99
🎨 Found MusicBrainz image for MBID 4b1a830b-0a1f-42e5-b8d5-1d6743912e99: https://commons.wikimedia.org/wiki/Special:Redirect/file/File:Creed_salt_lake_city.jpg
```

## Todos
- @todo check for unused functions
- @todo structured logger, update log verbosities, get rid of emojis, just show those as INFO records and set rest to DEBUG/TRACE
- @todo use the artist name from lastfm as normalized clean value and use that for display in stats
- @todo check output for errors
- @todo cleanup below
- @Todo remove lastfm api key into os env
- @todo more explanation on what and why
- @todo check if logical split versus regex in py vs pandas for cleaning? not entirely sure sound like should be pandas
- @todo cleanup read_db.py is duplicate probably, or maybe get rid of the copy bash script
- @todo structure project, file names (e.g. hello.py), split files, move scripts into a folder, split functions if needed etc
- @todo connect to gcp spark cluster + postgres maybe if time allows?
- @todo setup github CI (local stuff all should be fine)