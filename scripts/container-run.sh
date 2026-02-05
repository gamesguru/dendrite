#!/usr/bin/env bash
set -euo pipefail

TAG="${1:-dendrite:dev}"
DATA_DIR="${PWD}/.dendrite-dev"

mkdir -p "${DATA_DIR}"

# Generate config if it doesn't exist
if [[ ! -f "${DATA_DIR}/dendrite.yaml" ]]; then
    echo "Generating config..."
    docker run --rm --entrypoint /usr/bin/generate-config "${TAG}" \
        -dir /etc/dendrite -server localhost:8008 \
        > "${DATA_DIR}/dendrite.yaml"
fi

# Generate keys if they don't exist
if [[ ! -f "${DATA_DIR}/matrix_key.pem" ]]; then
    echo "Generating keys..."
    docker run --rm --entrypoint /usr/bin/generate-keys \
        -v "${DATA_DIR}:/etc/dendrite" "${TAG}" \
        -private-key /etc/dendrite/matrix_key.pem
fi

echo "Starting Dendrite..."
docker run --rm -d --name dendrite-dev \
    -v "${DATA_DIR}:/etc/dendrite" \
    -p 8008:8008 \
    "${TAG}"

# Wait for startup and check health
echo "Waiting for Dendrite to start..."
for i in {1..30}; do
    if curl -sf http://localhost:8008/_matrix/client/versions > /dev/null 2>&1; then
        echo "Dendrite is running and healthy!"
        curl -s http://localhost:8008/_matrix/client/versions | head -20
        echo ""
        echo "Container: dendrite-dev"
        echo "API: http://localhost:8008"
        echo "Stop with: just container-stop"
        exit 0
    fi
    sleep 1
done

echo "Dendrite failed to start. Logs:"
docker logs dendrite-dev
docker stop dendrite-dev 2>/dev/null || true
exit 1
