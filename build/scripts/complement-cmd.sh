#!/bin/bash -e

# This script is intended to be used inside a docker container for Complement

export GOCOVERDIR=/tmp/covdatafiles
mkdir -p "${GOCOVERDIR}"
if [[ "${COVER}" -eq 1 ]]; then
  echo "Running with coverage"
  exec /zendrite/zendrite-cover \
    --really-enable-open-registration \
    --tls-cert server.crt \
    --tls-key server.key \
    --config zendrite.yaml
else
  echo "Not running with coverage"
  exec /zendrite/zendrite \
    --really-enable-open-registration \
    --tls-cert server.crt \
    --tls-key server.key \
    --config zendrite.yaml
fi
