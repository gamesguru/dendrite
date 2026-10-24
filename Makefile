SHELL := /bin/bash
.DEFAULT_GOAL := help

STYLE_CYAN := $(shell tput setaf 6 2>/dev/null || printf '\033[36m')
STYLE_RESET := $(shell tput sgr0 2>/dev/null || printf '\033[0m')

GO_TOOLCHAIN := $(HOME)/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.24.3.linux-amd64/bin/go
ifeq ($(wildcard $(GO_TOOLCHAIN)),)
GO ?= go
else
GO ?= $(GO_TOOLCHAIN)
export PATH := $(dir $(GO_TOOLCHAIN)):$(PATH)
endif
STATICCHECK ?= staticcheck
STATICCHECKCHECKS ?= all,-ST1000,-ST1003,-ST1016,-ST1020,-ST1021,-ST1022
GOLANGCI_LINT ?= golangci-lint
DOCKER ?= docker
VETFLAGS ?=
STATICCHECKFLAGS ?=
# Allow concurrent local lint runs to share the workspace cache without lock failures.
GOLANGCI_LINTFLAGS ?= --allow-parallel-runners
GOCACHE ?= $(CURDIR)/.cache/go-build
GOLANGCI_LINT_CACHE ?= $(CURDIR)/.cache/golangci-lint
DENDRITE_TEST_SKIP_NODB ?= 1
DIFF_BASE ?= origin/main
PKGS := ./...
LIBPKGS = $(shell $(GO) list ./... | grep -v '/cmd/')
COVERPROFILE ?= coverage.out
COMPLEMENT_IMAGE ?= complement-dendrite
COMPLEMENT_POSTGRES ?=
COMPLEMENT_CGO ?= 0
COMPLEMENT_TAG ?= $(COMPLEMENT_POSTGRES)$(COMPLEMENT_CGO)
COMPLEMENT_BASE_IMAGE ?= $(COMPLEMENT_IMAGE):$(COMPLEMENT_TAG)
COMPLEMENT_DIR ?= complement
COMPLEMENT_RESULTS ?= $(CURDIR)/.complement/results.json
COMPLEMENT_LOGS ?= $(CURDIR)/.complement/logs.jsonl
# COMPLEMENT_PACKAGES are the test packages to run. With COMPLEMENT_RUN set they are the
# ones that define a matching top-level test (the part before the first /), as go test
# -run matches it, so that the other packages are not compiled and started for nothing.
# It falls back to all of them when nothing matches. Set it to override.
COMPLEMENT_TOP = $(firstword $(subst /, ,$(COMPLEMENT_RUN)))
# An opening parenthesis for the pattern below, as make counts the ones in a call.
COMPLEMENT_OPEN := [(]
COMPLEMENT_MATCHING = $(shell cd $(COMPLEMENT_DIR) 2>/dev/null && grep -rlE '^func [A-Za-z0-9_]*($(COMPLEMENT_TOP))[A-Za-z0-9_]*$(COMPLEMENT_OPEN)' tests --include='*_test.go' | xargs -r -n1 dirname | sort -u | sed 's|^|./|')
COMPLEMENT_ALL_PACKAGES = $(shell cd $(COMPLEMENT_DIR) 2>/dev/null && go list ./tests/... | grep -vE '(^|/)msc(3389|4429|4500)$$')
COMPLEMENT_PACKAGES ?= $(or $(and $(COMPLEMENT_RUN),$(strip $(COMPLEMENT_MATCHING))),$(COMPLEMENT_ALL_PACKAGES))
COMPLEMENT_PARTIAL_RUN = $(if $(COMPLEMENT_RUN),1,$(if $(filter-out $(COMPLEMENT_ALL_PACKAGES),$(COMPLEMENT_PACKAGES)),1,0))
# Set COMPLEMENT_SKIP_BUILD=1 to reuse the image that is already built.
# COMPLEMENT_RUN selects tests by regex (go test -run). Naming tests explicitly
# also drops the zendrite_blacklist tag, which excludes packages such as msc3902.
COMPLEMENT_RUN ?=
COMPLEMENT_TAGS ?= $(if $(COMPLEMENT_RUN),,zendrite_blacklist)
# go test's default per-package timeout is 10m; a package that deploys many
# containers serially (e.g. csapi) can legitimately take longer than that and
# gets killed mid-test, which looks like a hang but isn't. Give it more room.
COMPLEMENT_TIMEOUT ?= 1h
DENDRITE_INSTALL_PATH ?= /usr/local/bin/dendrite
DENDRITE_SYSTEMD_SERVICE ?= dendrite
SUDO ?= sudo

export GOCACHE
export GOLANGCI_LINT_CACHE

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

LINT_DIRS_GO ?= $(shell git diff --name-only $(DIFF_BASE) -- '*.go' | xargs -r dirname | sort -u | sed 's#^\.$$#./#; s#^[^.].*#./&#')

.PHONY: lint
lint: ## Run vet, staticcheck, and golangci-lint
	# check that LINT_DIRS_GO is set
	@test -n "${LINT_DIRS_GO}" || echo "Empty \$LINT_DIRS_GO: ${LINT_DIRS_GO}"
	# ~~~~ go vet        ~~~~
	$(GO) vet $(VETFLAGS) $(LINT_DIRS_GO)
	# ~~~~ golang-ci     ~~~~
	$(GOLANGCI_LINT) run --no-config --new-from-rev=$(DIFF_BASE) $(LINT_DIRS_GO)
	# ~~~~ staticcheck   ~~~~
	$(STATICCHECK) -checks='$(STATICCHECKCHECKS)' $(STATICCHECKFLAGS) $(LINT_DIRS_GO)

.PHONY: build
build: ## Build all packages
	$(GO) build $(PKGS)

.PHONY: build-cmd
build-cmd: ## Build binaries under cmd/
	$(GO) build ./cmd/...

.PHONY: install
install: ## Build the dendrite binary and install it to DENDRITE_INSTALL_PATH (requires sudo)
	$(GO) build -o dendrite ./cmd/zendrite
	$(SUDO) install -m 0755 ./dendrite $(DENDRITE_INSTALL_PATH)

.PHONY: restart
restart: ## Restart the dendrite systemd service (DENDRITE_SYSTEMD_SERVICE)
	$(SUDO) systemctl restart $(DENDRITE_SYSTEMD_SERVICE)

.PHONY: ci
ci: ## Run the documented local CI preflight
	./build/scripts/build-test-lint.sh

.PHONY: sytest
sytest: ## Run Sytest via the repo helper script
	./run-sytest.sh

.PHONY: complement-build
complement-build: ## Build the Complement Dendrite image
	$(DOCKER) build --build-arg=CGO=$(COMPLEMENT_CGO) -t $(COMPLEMENT_BASE_IMAGE) -f build/scripts/Complement$(COMPLEMENT_POSTGRES).Dockerfile .

.PHONY: complement-run
complement-run: ## Run Complement, building the image first
	@test -d $(COMPLEMENT_DIR) || (echo "Complement directory not found at $(COMPLEMENT_DIR). Please clone matrix-org/complement." && exit 1)
	$(if $(COMPLEMENT_SKIP_BUILD),@echo 'Reusing $(COMPLEMENT_BASE_IMAGE)',$(MAKE) complement-build)
	@set -o pipefail; \
	results="$(abspath $(COMPLEMENT_RESULTS))"; logs="$(abspath $(COMPLEMENT_LOGS))"; \
	mkdir -p "$$(dirname "$$results")" "$$(dirname "$$logs")" || exit $$?; \
	run_results=$$(mktemp "$$(dirname "$$results")/.results.XXXXXX") || exit $$?; \
	run_logs=$$(mktemp "$$(dirname "$$logs")/.logs.XXXXXX") || exit $$?; \
	merged_results=$$(mktemp "$$(dirname "$$results")/.merged.XXXXXX") || exit $$?; \
	go_status=0; \
	(cd $(COMPLEMENT_DIR) && COMPLEMENT_BASE_IMAGE=$(COMPLEMENT_BASE_IMAGE) \
		go test -json -v -count=1 -timeout=$(COMPLEMENT_TIMEOUT) $(if $(COMPLEMENT_TAGS),-tags $(COMPLEMENT_TAGS)) $(if $(COMPLEMENT_RUN),-run '$(COMPLEMENT_RUN)') $(COMPLEMENT_PACKAGES); \
		test_status=$$?; \
		if [ "$(COMPLEMENT_PARTIAL_RUN)" = 0 ]; then \
			COMPLEMENT_BASE_IMAGE=$(COMPLEMENT_BASE_IMAGE) go test -json -v -count=1 -timeout=$(COMPLEMENT_TIMEOUT) $(if $(COMPLEMENT_TAGS),-tags $(COMPLEMENT_TAGS)) -run '^TestMSC3389RedactionStripsRelationInOlderVersions$$' ./tests/msc3389; \
			target_status=$$?; \
			if [ $$test_status -ne 0 ]; then exit $$test_status; fi; \
			exit $$target_status; \
		fi; \
		exit $$test_status) \
		| tee "$$run_logs" \
		| jq -c 'select((.Action == "pass" or .Action == "fail" or .Action == "skip") and .Test != null) | {Action, Test}' > "$$run_results"; \
	go_status=$$?; \
	if [ "$(COMPLEMENT_PARTIAL_RUN)" = 1 ] && [ -f "$$results" ]; then \
		jq -sc 'reduce .[] as $$r ({}; .[$$r.Test] = $$r) | [.[]] | sort_by(.Test)[]' "$$results" "$$run_results" > "$$merged_results"; \
	else \
		jq -sc 'reduce .[] as $$r ({}; .[$$r.Test] = $$r) | [.[]] | sort_by(.Test)[]' "$$run_results" > "$$merged_results"; \
	fi; \
	result_status=$$?; \
	if [ $$result_status -eq 0 ]; then mv "$$merged_results" "$$results"; else rm -f "$$merged_results"; fi; \
	mv "$$run_logs" "$$logs"; \
	if [ $$result_status -ne 0 ]; then echo "Failed to process Complement results" >&2; exit $$result_status; fi; \
	failed_records=$$(jq -sc '[.[] | select(.Action == "fail" and (.Test | type == "string"))] | length' "$$run_results"); \
	failed_leaves=$$(jq -sc '([.[] | select(.Action == "fail" and (.Test | type == "string"))]) as $$f | [$$f[] as $$x | select([$$f[] | select(.Test | startswith($$x.Test + "/"))] | length == 0)] | length' "$$run_results"); \
	failed_packages=$$(jq -sc '[.[] | select(.Action == "fail" and .Test == null)] | length' "$$logs"); \
	echo "Complement: $$failed_leaves failed leaf tests ($$failed_records failing test records, $$failed_packages failed packages)"; \
	echo "Results: $$results"; echo "Full log: $$logs"; \
	rm -f "$$run_results"; \
	exit $$go_status

.PHONY: tidy
tidy: ## Tidy module dependencies
	$(GO) mod tidy

.PHONY: clean
clean: ## Remove generated coverage and build artifacts
	rm -f coverage.out cover.out
	rm -rf bin
	$(GO) clean -testcache
	rm -rf contrib/dendrite-demo-*/data-dir-*/
