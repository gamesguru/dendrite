#syntax=docker/dockerfile:1.28

# Complement image (PostgreSQL). Zendrite requires the native Rezzy library, so
# every image links it and the MSC4521 endpoints are always compiled in.
#
# Build from the repo root with the sibling checkouts as named contexts:
#
#   docker build -f build/scripts/ComplementPostgres.Dockerfile \
#     --build-context rezzy=../rezzy \
#     -t complement-dendrite:local .
#
# The Rust stage uses a glibc base (not Alpine/musl) because the Go build stage
# below is Debian; a musl static library would not link into it.
FROM docker.io/rust:1.89-trixie AS rezzy-build
WORKDIR /rezzy
COPY --from=rezzy Cargo.toml Cargo.lock /rezzy/
COPY --from=rezzy README.md /rezzy/README.md
COPY --from=rezzy src /rezzy/src
COPY --from=rezzy tests /rezzy/tests
COPY --from=rezzy rezzy-cli /rezzy/rezzy-cli
COPY --from=rezzy rezzy-ffi /rezzy/rezzy-ffi
COPY --from=rezzy rezzy-json /rezzy/rezzy-json
COPY --from=rezzy rezzy-keyfile /rezzy/rezzy-keyfile
COPY --from=rezzy rezzy-log /rezzy/rezzy-log
COPY --from=rezzy rezzy-recon /rezzy/rezzy-recon
# Builds librezzy.a and writes target/pkgconfig/rezzy-ffi.pc with the real version.
RUN apt-get update && apt-get install -y make python3 \
    && make -C rezzy-ffi native

FROM golang:1.27-trixie AS build
RUN apt-get update && apt-get install -y postgresql pkg-config
WORKDIR /build


ENV PKG_CONFIG_PATH=/rezzy/target/pkgconfig
# go.mod uses the Rezzy FFI from the checked-out submodule.
COPY --from=rezzy rezzy-ffi /rezzy/rezzy-ffi
COPY --from=rezzy rezzy-ffi/include /rezzy/include
COPY --from=rezzy-build /rezzy/target/release/librezzy.a /rezzy/target/release/librezzy.a
COPY --from=rezzy-build /rezzy/target/pkgconfig/rezzy-ffi.pc /rezzy/target/pkgconfig/rezzy-ffi.pc

# No password when connecting to Postgres
RUN sed -i "s%peer%trust%g" /etc/postgresql/17/main/pg_hba.conf && \
    # Bump up max conns for moar concurrency
    sed -i 's/max_connections = 100/max_connections = 2000/g' /etc/postgresql/17/main/postgresql.conf

# This entry script starts postgres, waits for it to be up then starts zendrite.
# It is deliberately PID 1 and supervises both processes so that we can shut
# postgres down cleanly when the container is stopped. Complement commits
# running containers into blueprint images; if postgres is killed by Docker
# (it does not receive SIGTERM when it is not PID 1) the committed data
# directory needs crash recovery on every subsequent start, which can exceed
# Complement's homeserver spawn timeout.
RUN cat > /build/run_complement.sh <<'EOF'
#!/bin/bash
set -eu

stop() {
    trap - TERM INT
    if [ -n "${zendrite_pid:-}" ]; then
        kill -TERM "${zendrite_pid}" 2>/dev/null || true
        wait "${zendrite_pid}" 2>/dev/null || true
    fi
    pg_ctlcluster 17 main stop || true
    exit 0
}
trap stop TERM INT

pg_lsclusters
pg_ctlcluster 17 main start

until pg_isready
do
    echo "Waiting for postgres"
    sleep 1
done

cd /zendrite

./generate-keys --keysize 1024 --server "$SERVER_NAME" --tls-cert server.crt --tls-key server.key --tls-authority-cert /complement/ca/ca.crt --tls-authority-key /complement/ca/ca.key
./generate-config -server "$SERVER_NAME" --ci --db "user=postgres database=postgres host=/var/run/postgresql/" > zendrite.yaml
# Bump max_open_conns up here in the global database config
sed -i 's/max_open_conns:.*$/max_open_conns: 1990/g' zendrite.yaml
cp /complement/ca/ca.crt /usr/local/share/ca-certificates/
update-ca-certificates

/complement-cmd.sh &
zendrite_pid=$!

status=0
wait "${zendrite_pid}" || status=$?

trap - TERM INT
pg_ctlcluster 17 main stop || true
exit "$status"
EOF
RUN chmod +x /build/run_complement.sh

# we will dump the binaries and config file to this location to ensure any local untracked files
# that come from the COPY . . file don't contaminate the build
RUN mkdir /zendrite

# Utilise Docker caching when downloading dependencies, this stops us needlessly
# downloading dependencies every time.
RUN --mount=target=. \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -buildvcs=false -tags goolm,nodynamic -o /zendrite ./cmd/generate-config ./cmd/generate-keys ./cmd/zendrite && \
    CGO_ENABLED=1 go build -buildvcs=false -tags goolm,nodynamic -cover -covermode=atomic -o /zendrite/zendrite-cover -coverpkg "codefloe.com/pat-s/zendrite/..." ./cmd/zendrite && \
    cp build/scripts/complement-cmd.sh /complement-cmd.sh

WORKDIR /zendrite
RUN ./generate-keys --private-key matrix_key.pem

ENV SERVER_NAME=localhost
ENV API=0
ENV COVER=0
EXPOSE 8008 8448


# At runtime, generate TLS cert based on the CA now mounted at /ca
# At runtime, replace the SERVER_NAME with what we are told
CMD ["/build/run_complement.sh"]
