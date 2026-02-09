#!/usr/bin/env bash

TAG=${1:-latest}

echo "Pulling tag '${TAG}'"

docker pull pats22/dendrite:${TAG}
