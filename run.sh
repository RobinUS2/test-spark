#!/bin/bash
# Run the docker container for local testing
docker run --rm -it $(docker build -q .)
