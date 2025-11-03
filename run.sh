#!/bin/bash
# run the docker container for local testing ease
# @todo cleanup
docker run --rm -it $(docker build -q .)
