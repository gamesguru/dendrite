SHELL := /bin/bash
.DEFAULT_GOAL := help

STYLE_CYAN := $(shell tput setaf 6 2>/dev/null || printf '\033[36m')
STYLE_RESET := $(shell tput sgr0 2>/dev/null || printf '\033[0m')

GO ?= go
STATICCHECK ?= staticcheck
GOLANGCI_LINT ?= golangci-lint
DOCKER ?= docker
VETFLAGS ?=
STATICCHECKFLAGS ?=
GOLANGCI_LINTFLAGS ?=
DENDRITE_TEST_SKIP_NODB ?= 1
DIFF_BASE ?= origin/main
PKGS := ./...
LIBPKGS = $(shell $(GO) list ./... | grep -v '/cmd/')
COVERPROFILE ?= coverage.out
COMPLEMENT_IMAGE ?= complement-dendrite
COMPLEMENT_POSTGRES ?=
COMPLEMENT_CGO ?= 0

.PHONY: help
help: ## Show available targets
	@grep -hE '^[a-zA-Z0-9_\/-]+:[[:space:]]*## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":[[:space:]]*## "}; {printf "$(STYLE_CYAN)%-18s$(STYLE_RESET) %s\n", $$1, $$2}'

.PHONY: format
format: ## Format Go source files
	$(GO) fmt $(PKGS)

.PHONY: test
test: ## Run SQLite-only unit tests with race detection
	DENDRITE_TEST_SKIP_NODB=$(DENDRITE_TEST_SKIP_NODB) $(GO) test --race $(PKGS)

.PHONY: test-all
test-all: ## Run all unit tests, including Postgres-backed ones if available
	$(GO) test --race $(PKGS)

.PHONY: test-lib
test-lib: ## Run library package tests only, excluding cmd/
	DENDRITE_TEST_SKIP_NODB=$(DENDRITE_TEST_SKIP_NODB) $(GO) test --race $(LIBPKGS)

.PHONY: cov
cov: ## Run library package tests with coverage and print a summary
	DENDRITE_TEST_SKIP_NODB=$(DENDRITE_TEST_SKIP_NODB) $(GO) test -covermode=atomic -coverpkg=./... -coverprofile=$(COVERPROFILE) $(LIBPKGS)
	$(GO) tool cover -func=$(COVERPROFILE)

.PHONY: lint
lint: ## Run vet, staticcheck, and golangci-lint; use `make lint diff=1` for changed packages only
	@set -euo pipefail; \
	if [ -n "$(strip $(diff))" ]; then \
		dirs="$$(git diff --name-only $(DIFF_BASE) -- '*.go' \
			| xargs -r dirname \
			| sort -u \
			| sed 's#^\.$$#./#; s#^[^.].*#./&#')"; \
		if [ -z "$$dirs" ]; then \
			echo "No changed Go packages against $(DIFF_BASE)"; \
			exit 0; \
		fi; \
		$(GO) vet $(VETFLAGS) $$dirs; \
		$(STATICCHECK) -checks=all $(STATICCHECKFLAGS) $$dirs || true; \
		$(GOLANGCI_LINT) run --no-config --new-from-rev=$(DIFF_BASE) $$dirs; \
	else \
		$(GO) vet $(VETFLAGS) $(PKGS); \
		$(STATICCHECK) -checks=all $(STATICCHECKFLAGS) $(PKGS); \
		$(GOLANGCI_LINT) run $(GOLANGCI_LINTFLAGS); \
	fi

.PHONY: build
build: ## Build all packages
	$(GO) build $(PKGS)

.PHONY: build-cmd
build-cmd: ## Build binaries under cmd/
	$(GO) build ./cmd/...

.PHONY: ci
ci: ## Run the documented local CI preflight
	./build/scripts/build-test-lint.sh

.PHONY: sytest
sytest: ## Run Sytest via the repo helper script
	./run-sytest.sh

.PHONY: complement-build
complement-build: ## Build the Complement Dendrite image
	$(DOCKER) build --build-arg=CGO=$(COMPLEMENT_CGO) -t $(COMPLEMENT_IMAGE):$(COMPLEMENT_POSTGRES)$(COMPLEMENT_CGO) -f build/scripts/Complement$(COMPLEMENT_POSTGRES).Dockerfile .

.PHONY: tidy
tidy: ## Tidy module dependencies
	$(GO) mod tidy

.PHONY: clean
clean: ## Remove generated coverage and build artifacts
	rm -f coverage.out cover.out
	rm -rf bin
	$(GO) clean -testcache
