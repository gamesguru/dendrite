# syntax=docker/dockerfile:1

FROM golang:1.25-trixie AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/dendrite-migration-test ./cmd/dendrite-migration-test
RUN CGO_ENABLED=0 go build -o /out/dendrite-migration-test ./cmd/dendrite-migration-test

FROM scratch
COPY --from=build /out/dendrite-migration-test /
ENTRYPOINT ["/dendrite-migration-test"]
