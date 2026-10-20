#syntax=docker/dockerfile:1.28

#
# base installs required dependencies and runs go mod download to cache dependencies
#
FROM --platform=${BUILDPLATFORM} docker.io/golang:1.27-alpine AS base
RUN apk --update --no-cache add bash build-base curl git

#
# build creates all needed binaries
#
FROM --platform=${BUILDPLATFORM} base AS build
WORKDIR /src
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ENV GONOSUMCHECK=codefloe.com/* \
    GONOSUMDB=codefloe.com/* \
    GONOPROXY=codefloe.com/*
# Disable WebP's dynamic backend, which requires glibc even with CGO_ENABLED=0.
RUN --mount=target=. \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    sh -c 'USERARCH=$(go env GOARCH); \
    if [ "$TARGETARCH" = "$USERARCH" ]; then CGO=1; else CGO=0; fi; \
    GOARCH="$TARGETARCH" GOOS="linux" CGO_ENABLED="$CGO" \
    go build -tags goolm,nodynamic -v -trimpath \
    -ldflags "-X codefloe.com/pat-s/zendrite/internal.version='"$VERSION"'" \
    -o /out/ ./cmd/...'

#
# Builds the Zendrite image containing all required binaries
#
FROM alpine:3.24

# Install runtime dependencies
RUN apk --update --no-cache add ca-certificates curl tzdata \
    && addgroup -g 1000 zendrite \
    && adduser -u 1000 -G zendrite -h /etc/zendrite -D zendrite

LABEL org.opencontainers.image.title="Zendrite"
LABEL org.opencontainers.image.description="Next-generation Matrix homeserver written in Go"
LABEL org.opencontainers.image.source="https://codefloe.com/pat-s/zendrite"
LABEL org.opencontainers.image.licenses="AGPL-3.0-only OR LicenseRef-Element-Commercial"

COPY --from=build /out/create-account /usr/bin/create-account
COPY --from=build /out/generate-config /usr/bin/generate-config
COPY --from=build /out/generate-keys /usr/bin/generate-keys
COPY --from=build /out/zendrite /usr/bin/zendrite

RUN mkdir -p /var/lib/zendrite && chown zendrite:zendrite /var/lib/zendrite

VOLUME /etc/zendrite
VOLUME /var/lib/zendrite
WORKDIR /etc/zendrite

# Run as non-root user
USER 1000:1000

# Verify that the binary and its dynamic loader work in the runtime image.
RUN /usr/bin/zendrite --version

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD ["curl", "-fSs", "http://localhost:8008/_matrix/client/versions"]

ENTRYPOINT ["/usr/bin/zendrite"]
EXPOSE 8008 8448
