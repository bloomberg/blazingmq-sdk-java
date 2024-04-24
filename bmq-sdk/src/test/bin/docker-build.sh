#!/bin/bash

# DESCRIPTION
# ===========
# This script builds docker image which is used to run containers in tests

# USAGE
# =====
#   docker-build.sh image_name
#   BMQ_DOCKER_IMAGE=image_name docker-build.sh

image=""

if [ -n "$1" ]; then
  image=$1
  echo "Use '$image' image from the script argument"
elif [ -n "$BMQ_DOCKER_IMAGE" ]; then
  image=$BMQ_DOCKER_IMAGE
  echo "Use '$image' image from the BMQ_DOCKER_IMAGE var"
else
  echo "No docker image was provided"

  cmd=`basename "$0"`
  echo "Script usage:"
  echo -e "\t$cmd image_name"
  echo -e "\tBMQ_DOCKER_IMAGE=image_name $cmd"

  exit 1
fi

cd "$(dirname "$0")"/../docker

docker --version
docker info
# Use --pull if docker registry is used
docker build --tag bmq-broker-java-it --build-arg "image=$image" .
