#!/bin/bash
# Run the docker container for local testing

# Build the image
IMAGE_ID=$(docker build -q .)

# Check if LASTFM_API_KEY environment variable is set
if [ -n "$LASTFM_API_KEY" ]; then
    echo "Using Last.fm API key from environment variable"
    docker run --rm -it -e LASTFM_API_KEY="$LASTFM_API_KEY" $IMAGE_ID
else
    echo "No LASTFM_API_KEY found - using fallback (demo purposes only)"
    docker run --rm -it $IMAGE_ID
fi
