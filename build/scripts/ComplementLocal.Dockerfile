#syntax=docker/dockerfile:1.28

# A local development Complement dockerfile, to be used with host mounts
# /cache -> Contains the entire zendrite code at Dockerfile build time. Builds binaries but only keeps the generate-* ones. Pre-compilation saves time.
# /zendrite -> Host-mounted sources
# /runtime -> Binaries and config go here and are run at runtime
# At runtime, zendrite is built from /zendrite and run in /runtime.
#
# Use these mounts to make use of this dockerfile:
# COMPLEMENT_HOST_MOUNTS='/your/local/zendrite:/zendrite:ro;/your/go/path:/go:ro'
FROM golang:1.27-trixie
RUN apt-get update && apt-get install -y sqlite3

ENV SERVER_NAME=localhost
ENV COVER=0
EXPOSE 8008 8448

WORKDIR /runtime
# This script compiles Zendrite for us.
RUN echo '\
    #!/bin/bash -eux \n\
    if test -f "/runtime/zendrite" && test -f "/runtime/zendrite-cover"; then \n\
    echo "Skipping compilation; binaries exist" \n\
    exit 0 \n\
    fi \n\
    cd /zendrite \n\
    go build -tags goolm,nodynamic -v -o /runtime /zendrite/cmd/zendrite \n\
    go build -tags goolm,nodynamic -cover -covermode=atomic -o /runtime/zendrite-cover -coverpkg "codefloe.com/pat-s/zendrite/..." /zendrite/cmd/zendrite \n\
    ' > compile.sh && chmod +x compile.sh

# This script runs Zendrite for us. Must be run in the /runtime directory.
RUN echo '\
    #!/bin/bash -eu \n\
    ./generate-keys --private-key matrix_key.pem \n\
    ./generate-keys -keysize 1024 --server $SERVER_NAME --tls-cert server.crt --tls-key server.key --tls-authority-cert /complement/ca/ca.crt --tls-authority-key /complement/ca/ca.key \n\
    ./generate-config -server $SERVER_NAME --ci > zendrite.yaml \n\
    cp /complement/ca/ca.crt /usr/local/share/ca-certificates/ && update-ca-certificates \n\
    [ ${COVER} -eq 1 ] && mkdir -p /tmp/covdatafiles && GOCOVERDIR=/tmp/covdatafiles exec ./zendrite-cover --really-enable-open-registration --tls-cert server.crt --tls-key server.key --config zendrite.yaml \n\
    exec ./zendrite --really-enable-open-registration --tls-cert server.crt --tls-key server.key --config zendrite.yaml \n\
    ' > run.sh && chmod +x run.sh


WORKDIR /cache
# Build the monolith in /cache - we won't actually use this but will rely on build artifacts to speed
# up the real compilation. Build the generate-* binaries in the true /runtime locations.
# If the generate-* source is changed, this dockerfile needs re-running.
RUN --mount=target=. \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -tags goolm,nodynamic -o /runtime ./cmd/generate-config ./cmd/generate-keys


WORKDIR /runtime
CMD /runtime/compile.sh && exec /runtime/run.sh
