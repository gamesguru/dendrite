#!/usr/bin/env bash
set -euo pipefail

TAG="${1:-zendrite:dev}"
DATA_DIR="${PWD}/.zendrite-dev"

mkdir -p "${DATA_DIR}"

# Generate config if it doesn't exist
if [[ ! -f "${DATA_DIR}/zendrite.yaml" ]]; then
    echo "Generating config..."
    docker run --rm --entrypoint /usr/bin/generate-config "${TAG}" \
        -dir /etc/zendrite -server localhost:8008 \
        > "${DATA_DIR}/zendrite.yaml"
fi

# Generate keys if they don't exist
if [[ ! -f "${DATA_DIR}/matrix_key.pem" ]]; then
    echo "Generating keys..."
    docker run --rm --entrypoint /usr/bin/generate-keys \
        -v "${DATA_DIR}:/etc/zendrite" "${TAG}" \
        -private-key /etc/zendrite/matrix_key.pem
fi

echo "Starting Zendrite..."
docker run --rm -d --name zendrite-dev \
    -v "${DATA_DIR}:/etc/zendrite" \
    -p 8008:8008 \
    "${TAG}"

# Wait for startup and check health
echo "Waiting for Zendrite to start..."
for i in {1..30}; do
    if curl -sf http://localhost:8008/_matrix/client/versions > /dev/null 2>&1; then
        echo "Zendrite is running and healthy!"
        curl -s http://localhost:8008/_matrix/client/versions | head -20
        echo ""
        echo "Container: zendrite-dev"
        echo "API: http://localhost:8008"
        echo "Stop with: just container-stop"
        exit 0
    fi
    sleep 1
done

echo "Zendrite failed to start. Logs:"
docker logs zendrite-dev
docker stop zendrite-dev 2>/dev/null || true
exit 1
