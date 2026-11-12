#syntax=docker/dockerfile:1.28

FROM golang:1.27-trixie as build
RUN apt-get update && apt-get install -y postgresql
WORKDIR /build

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

pg_lsclusters
pg_ctlcluster 17 main start

until pg_isready
do
    echo "Waiting for postgres"
    sleep 1
done

cd /zendrite

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
ARG CGO=1
RUN --mount=target=. \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=${CGO} go build -tags goolm,nodynamic -o /zendrite ./cmd/generate-config ./cmd/generate-keys ./cmd/zendrite && \
    CGO_ENABLED=${CGO} go build -tags goolm,nodynamic -cover -covermode=atomic -o /zendrite/zendrite-cover -coverpkg "codefloe.com/pat-s/zendrite/..." ./cmd/zendrite && \
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
