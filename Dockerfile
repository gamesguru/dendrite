#syntax=docker/dockerfile:1.28

#
# base installs required dependencies and runs go mod download to cache dependencies
#
FROM docker.io/golang:1.27-alpine AS base
RUN apk --update --no-cache add bash build-base curl git pkgconf

#
# rezzy-build compiles the native library that the Go binaries link against.
# Build it for the target platform so the cgo toolchain in the build stage can
# use it without a cross compiler. The rezzy source comes from the "rezzy"
# named build context, for example:
#
#   docker build --build-context rezzy=../rezzy --build-context gmsl=../gmsl .
#
FROM --platform=${TARGETPLATFORM} docker.io/rust:1.89-alpine AS rezzy-build
RUN apk --update --no-cache add musl-dev
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

#
# build creates all needed binaries
#
FROM --platform=${TARGETPLATFORM} base AS build
WORKDIR /src
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ENV GONOSUMCHECK=codefloe.com/* \
    GONOSUMDB=codefloe.com/* \
    GONOPROXY=codefloe.com/* \
    CGO_ENABLED=1 \
    PKG_CONFIG_PATH=/rezzy/target/pkgconfig
# go.rezzy.mod replaces both modules with sibling checkouts supplied by the
# named build contexts above.
COPY --from=rezzy rezzy-ffi /rezzy/rezzy-ffi
COPY --from=rezzy include /rezzy/include
COPY --from=rezzy-build /rezzy/target/release/librezzy_ffi.a /rezzy/target/release/librezzy_ffi.a
COPY --from=rezzy-build /rezzy/target/pkgconfig/rezzy-ffi.pc /rezzy/target/pkgconfig/rezzy-ffi.pc
COPY --from=gmsl . /gmsl
# Disable WebP's dynamic backend, which requires glibc even with CGO_ENABLED=0.
RUN --mount=target=. \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    GOARCH="$TARGETARCH" GOOS="linux" CGO_ENABLED=1 \
    go build -modfile=go.rezzy.mod -tags rezzy,goolm,nodynamic -v -trimpath \
    -ldflags "-X codefloe.com/pat-s/zendrite/internal.version='"$VERSION"'" \
    -o /out/ ./cmd/...

#
# Builds the Zendrite image containing all required binaries
#
FROM alpine:3.24

# Install runtime dependencies
RUN apk --update --no-cache add ca-certificates curl tzdata libgcc \
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
