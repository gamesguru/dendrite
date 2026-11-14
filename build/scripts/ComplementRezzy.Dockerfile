#syntax=docker/dockerfile:1.28

# Complement image with the native Rezzy library linked in (-tags rezzy), so
# the MSC4521 reconciliation endpoints are real rather than the stub.
#
# Build from the repo root with the sibling checkouts as named contexts:
#
#   docker build -f build/scripts/ComplementRezzy.Dockerfile \
#     --build-context rezzy=../rezzy --build-context gmsl=../gmsl \
#     -t complement-zendrite-rezzy .
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
COPY --from=rezzy rezzy-log /rezzy/rezzy-log
COPY --from=rezzy rezzy-recon /rezzy/rezzy-recon
RUN cargo build --release -p rezzy-ffi \
    && mkdir -p target/pkgconfig \
    && printf 'prefix=/rezzy\nlibdir=/rezzy/target/release\nincludedir=/rezzy/include\n\nName: rezzy-ffi\nDescription: Rezzy static C ABI\nVersion: 0.0.0\nLibs: /rezzy/target/release/librezzy_ffi.a -lpthread -ldl -lm\nCflags: -I/rezzy/include\n' \
        > target/pkgconfig/rezzy-ffi.pc

FROM golang:1.27-trixie AS build
RUN apt-get update && apt-get install -y sqlite3 pkg-config
WORKDIR /build

ENV PKG_CONFIG_PATH=/rezzy/target/pkgconfig
# go.rezzy.mod replaces rezzy-ffi and gomatrixserverlib with these checkouts.
COPY --from=rezzy rezzy-ffi /rezzy/rezzy-ffi
COPY --from=rezzy rezzy-ffi/include /rezzy/include
COPY --from=rezzy-build /rezzy/target/release/librezzy_ffi.a /rezzy/target/release/librezzy_ffi.a
COPY --from=rezzy-build /rezzy/target/pkgconfig/rezzy-ffi.pc /rezzy/target/pkgconfig/rezzy-ffi.pc
COPY --from=gmsl . /gmsl

# we will dump the binaries and config file to this location to ensure any local untracked files
# that come from the COPY . . file don't contaminate the build
RUN mkdir /zendrite

# Utilise Docker caching when downloading dependencies, this stops us needlessly
# downloading dependencies every time.
ARG CGO=1
RUN --mount=target=. \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=${CGO} go build -buildvcs=false -modfile=go.rezzy.mod -tags rezzy,goolm,nodynamic -o /zendrite ./cmd/generate-config ./cmd/generate-keys ./cmd/zendrite && \
    CGO_ENABLED=${CGO} go build -buildvcs=false -modfile=go.rezzy.mod -tags rezzy,goolm,nodynamic -cover -covermode=atomic -o /zendrite/zendrite-cover -coverpkg "codefloe.com/pat-s/zendrite/..." ./cmd/zendrite && \
    cp build/scripts/complement-cmd.sh /complement-cmd.sh

WORKDIR /zendrite
RUN ./generate-keys --private-key matrix_key.pem

ENV SERVER_NAME=localhost
ENV API=0
ENV COVER=0
EXPOSE 8008 8448

# At runtime, generate TLS cert based on the CA now mounted at /ca
# At runtime, replace the SERVER_NAME with what we are told
CMD ["/bin/sh", "-c", "./generate-keys -keysize 1024 --server $SERVER_NAME --tls-cert server.crt --tls-key server.key --tls-authority-cert /complement/ca/ca.crt --tls-authority-key /complement/ca/ca.key && \
    ./generate-config -server $SERVER_NAME --ci > zendrite.yaml && \
    cp /complement/ca/ca.crt /usr/local/share/ca-certificates/ && update-ca-certificates && \
    exec /complement-cmd.sh"]
