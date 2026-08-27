#!/usr/bin/env bash

TAG=${1:-latest}

echo "Pushing tag '${TAG}'"

docker push pats22/dendrite:${TAG}
