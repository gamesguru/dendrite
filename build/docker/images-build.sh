#!/usr/bin/env bash

cd $(git rev-parse --show-toplevel)

TAG=${1:-latest}

echo "Building tag '${TAG}'"

docker build -t pats22/dendrite:${TAG} .
