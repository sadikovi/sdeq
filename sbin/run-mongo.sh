#!/bin/bash

if [[ -z "$(which docker)" ]]; then
  echo "Docker is not found"
  exit 1
fi

# run Mongo DB container, this will run it in non-daemon mode
# you can run it as `docker -it ... bash`, start `mongod &` and inspect mongo collections
if [[ -n "$(docker ps --format '{{.Names}}' | grep mongo)" ]]; then
  echo "Container mongo is already running, stop it first"
  exit 1
fi

if [[ -n "$(docker ps -a --format '{{.Names}}' | grep mongo)" ]]; then
  echo "Removing old container first..."
  docker rm mongo
fi

echo "Launch Mongo docker container"
docker run -p 27017:27017 --name mongo mongo:3.2
