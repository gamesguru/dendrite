#syntax=docker/dockerfile:1.21

#
# base installs required dependencies and runs go mod download to cache dependencies
#
FROM --platform=${BUILDPLATFORM} docker.io/golang:1.25-alpine AS base
RUN apk --update --no-cache add bash build-base curl git

#
# build creates all needed binaries
#
FROM --platform=${BUILDPLATFORM} base AS build
WORKDIR /src
ARG TARGETOS
ARG TARGETARCH
RUN --mount=target=. \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    USERARCH=`go env GOARCH` \
    GOARCH="$TARGETARCH" \
    GOOS="linux" \
    CGO_ENABLED=$([ "$TARGETARCH" = "$USERARCH" ] && echo "1" || echo "0") \
    go build -v -trimpath -o /out/ ./cmd/...

#
# Builds the Dendrite image containing all required binaries
#
FROM alpine:3.23

# Install runtime dependencies
RUN apk --update --no-cache add ca-certificates curl tzdata \
    && addgroup -g 1000 dendrite \
    && adduser -u 1000 -G dendrite -h /etc/dendrite -D dendrite

LABEL org.opencontainers.image.title="Dendrite"
LABEL org.opencontainers.image.description="Next-generation Matrix homeserver written in Go"
LABEL org.opencontainers.image.source="https://codefloe.com/pat-s/dendrite"
LABEL org.opencontainers.image.licenses="AGPL-3.0-only OR LicenseRef-Element-Commercial"

COPY --from=build /out/create-account /usr/bin/create-account
COPY --from=build /out/generate-config /usr/bin/generate-config
COPY --from=build /out/generate-keys /usr/bin/generate-keys
COPY --from=build /out/dendrite /usr/bin/dendrite

VOLUME /etc/dendrite
WORKDIR /etc/dendrite

# Run as non-root user
USER dendrite

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD curl -fSs http://localhost:8008/_matrix/client/versions || exit 1

ENTRYPOINT ["/usr/bin/dendrite"]
EXPOSE 8008 8448
