#!/bin/bash -ex

# build cascading
docker build --tag experimental-cascading:latest -f docker/Dockerfile ./docker

# build scalding
SCALDING_HOST_ROOT=$PWD../../
MAVEN_HOST_CACHE=~/.m2
RESOURCE_ARGS="--cpus=0.000 --memory=8g --memory-swap=24g" # container will crash without enough memory
docker run -v $SCALDING_HOST_ROOT:/scalding -v $MAVEN_HOST_CACHE:/root/m2-host experimental-cascading:latest $RESOURCE_ARGS