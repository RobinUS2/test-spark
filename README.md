## Usage

### Environment Configuration
```bash
# Optional: Set Last.fm API key for enhanced artist metadata
export LASTFM_API_KEY="your_lastfm_api_key_here"
```

### Data Setup
```bash
./fetch_test_data.sh    # Download test data from GitHub repository
```

### Database Operations  
```bash
./setup_db.sh sqlite    # Configure for local SQLite usage
./copy_docker_db_to_local.sh  # Copy database from Docker container
./export.sh             # Export database tables to CSV files
# check files *.csv
```

Simple SQLite for now, given the filesize, switching to postgres would be simple

## Architecture
Local docker setup for portability, yet simplicity. Could've done something full cloud / docker compose, but given the data set size and time constraints would like to keep it relatively simple, yet prepare for later scale increase; hence Spark as distributed processing framework. Chose language and frameworks based on some research and Rockstars commonly used techniques to try this out, learn and show the case in the most relevant context. 

Sqlalchemy for DB connection with pandas. Into a local SQLite given the amount of data again, yet a configuration tool to be able to switch to more persistent and scalable Postgresin a production (cloud) environment. 

Implemented the dual layer image lookup with caching via database (to reduce API calls, in production might want something like redis instead of SQL for caching; albeit easier to debug now, hence keeping that). 

## Image fetching
Via lastFm to MusicBrainz (LastFm stopped exposing images), bit of a little nightmare, but works here and there. The code currently limits the amount of artists and tracks fetched from external APIs to prevent rate limits being hit.

```
Fetching for 'Creed' (cache key: 'Creed')
Fetching from Last.fm API for artist: Creed (cache key: Creed)
Last.fm API fields for 'Creed': ['name', 'mbid', 'url', 'image', 'streamable', 'ontour', 'stats', 'similar', 'tags', 'bio']
Found MBID for 'Creed': 4b1a830b-0a1f-42e5-b8d5-1d6743912e99
Found MusicBrainz image for MBID 4b1a830b-0a1f-42e5-b8d5-1d6743912e99: https://commons.wikimedia.org/wiki/Special:Redirect/file/File:Creed_salt_lake_city.jpg
```

## Other things
Simple structured logger implemented. Time felt relatively constrained to do all items properly for production grade code; chose to demonstrate here and there archicture over perfet implemenation of code sections. Didn't spend as much time on data validation (in the results), would like to do more manual cross checks to rule out remaining noise. More tests could help create stability in the future in the project to ensure we keep moving in the right direction; however with this rapid prototype and time constraints too early I believe. 


## Ideas to make it more production ready
- @todo connect to spark cluster + postgres, hosted GCP for example
- @todo setup (github) CI/CD (local stuff all should be fine)