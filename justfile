# Justfile for Dendrite development
#
# Prerequisites:
#   macOS: brew install libolm
#   Ubuntu: apt-get install libolm-dev libolm3 build-essential

# Default recipe
default:
    @just --list

install-dev-deps:
		# @hash golangci-lint > /dev/null 2>&1 || go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
		@hash gofumpt > /dev/null 2>&1 || go install mvdan.cc/gofumpt@latest
		@hash gci > /dev/null 2>&1 || go install github.com/daixiang0/gci@latest
		@hash mockery > /dev/null 2>&1 || go install github.com/vektra/mockery/v3@latest
		@hash addlicense > /dev/null 2>&1 || go install github.com/google/addlicense@latest

fmt: install-dev-deps
		prettier -w .
		gofumpt -w .
		golangci-lint run --fix
		gci write --skip-vendor --skip-generated -s standard -s default -s "prefix(codeberg.org/crowci/crow)" --custom-order .

# Run golangci-lint (with CGO flags for libolm on macOS)
lint:
    #!/usr/bin/env bash
    if [[ "$(uname)" == "Darwin" ]]; then
        export CGO_CFLAGS="-I$(brew --prefix libolm)/include"
        export CGO_LDFLAGS="-L$(brew --prefix libolm)/lib"
    fi
    golangci-lint run

# Run unit tests
test:
    #!/usr/bin/env bash
    if [[ "$(uname)" == "Darwin" ]]; then
        export CGO_CFLAGS="-I$(brew --prefix libolm)/include"
        export CGO_LDFLAGS="-L$(brew --prefix libolm)/lib"
    fi
    go test -v ./...

# Run unit tests with race detector
test-race:
    #!/usr/bin/env bash
    if [[ "$(uname)" == "Darwin" ]]; then
        export CGO_CFLAGS="-I$(brew --prefix libolm)/include"
        export CGO_LDFLAGS="-L$(brew --prefix libolm)/lib"
    fi
    go test -race -v ./...

# Run unit tests with coverage
test-coverage:
    #!/usr/bin/env bash
    if [[ "$(uname)" == "Darwin" ]]; then
        export CGO_CFLAGS="-I$(brew --prefix libolm)/include"
        export CGO_LDFLAGS="-L$(brew --prefix libolm)/lib"
    fi
    go test -race -v -coverpkg=./... -coverprofile=cover.out ./...

# Run unit tests for a specific package
test-pkg pkg:
    go test -v ./{{pkg}}/...

# Build all binaries
build:
    go build -trimpath -v -o bin/ ./cmd/...

# Build a specific binary
build-bin name:
    go build -trimpath -v -o bin/{{name}} ./cmd/{{name}}

# Run both lint and tests
check: lint test

# Clean build artifacts
clean:
    rm -rf bin/ cover.out

# Generate code (if applicable)
generate:
    go generate ./...

# Update dependencies
deps-update:
    go get -u ./...
    go mod tidy

# Verify dependencies
deps-verify:
    go mod verify

vendor:
    go mod tidy
    go mod vendor

# Build multi-arch container image (default: load to local docker)
container tag="dendrite:dev":
    docker buildx build --platform linux/amd64,linux/arm64 -t {{tag}} .

# Build container image and load to local docker (current arch only)
container-local tag="dendrite:dev":
    docker buildx build --load -t {{tag}} .

# Build and push container image to registry
container-push tag:
    docker buildx build --platform linux/amd64,linux/arm64 --push -t {{tag}} .

# Run local Dendrite container to verify image works
container-run tag="codefloe.com/pat-s/dendrite:dev":
    ./scripts/container-run.sh {{tag}}

# Stop local Dendrite container
container-stop:
    docker stop dendrite-dev 2>/dev/null || echo "Container not running"

# Clean up local Dendrite data
container-clean: container-stop
    rm -rf .dendrite-dev
