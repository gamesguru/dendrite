# syntax=docker/dockerfile:1

FROM golang:1.26-trixie AS build

WORKDIR /src
COPY . .

ARG BINARY
RUN go build -o /out/homeserver ./cmd/${BINARY} && \
    go build -o /out/generate-config ./cmd/generate-config && \
    go build -o /out/generate-keys ./cmd/generate-keys

FROM debian:trixie-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /out/ /usr/local/bin/
COPY build/scripts/dendrite-migration-entrypoint.sh /usr/local/bin/

EXPOSE 8008 8448
ENTRYPOINT ["dendrite-migration-entrypoint.sh"]
