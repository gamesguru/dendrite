#!/usr/bin/env bash

cd $(git rev-parse --show-toplevel)

TAG=${1:-latest}

echo "Building tag '${TAG}'"

git submodule update --init rezzy

docker buildx build --load \
  --build-context rezzy=./rezzy \
  --build-context gmsl=../gmsl \
  -t pats22/dendrite:${TAG} .
