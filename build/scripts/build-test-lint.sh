#! /bin/bash

# Builds, tests and lints Zendrite, and should be run before pushing commits

set -eu

cd "$(dirname "$0")/../.."

# Check that all the packages can build.
# When `go build` is given multiple packages it won't output anything, and just
# checks that everything builds.
echo "Checking that it builds..."
go build -tags goolm ./cmd/...

prek run golangci-lint --all-files

echo "Testing..."
go test -tags goolm -race -v ./...
