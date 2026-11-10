# Copyright 2024 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.DEFAULT_GOAL := help
.SUFFIXES: # remove legacy builtin suffixes to allow easier make debugging
SHELL = /usr/bin/env bash

# If GOARCH is not set in the env, find it
GOARCH ?= $(shell go env GOARCH)

## ==== ARGS =====
DOCKER            ?= docker                    # Container build tool compatible with `docker` API
PLATFORM          ?= linux/$(GOARCH)           # Platform for 'build'
BUILD_ARGS        ?=                           # Additional args for 'build'
SAMPLE_DRIVER_TAG ?= ontap-cosi-driver:latest # Image tag for controller image build

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: clean
clean:
	rm -rf $(LOCALBIN)

.PHONY: all
all: clean lint test

##@ Image

.PHONY: build
build: ## Build only the controller container image
	$(DOCKER) build --file Dockerfile --platform $(PLATFORM) $(BUILD_ARGS) --tag $(SAMPLE_DRIVER_TAG) .

##@ Release

.PHONY: release-tag
release-tag: ## Tag the repository for release.
	git tag $(GIT_TAG)
	git push origin $(GIT_TAG)

##@ Development

.PHONY: test
test: test-unit ## Run all tests.

.PHONY: test-unit
test-unit: ## Run unit tests.
	GO111MODULE=on GOARCH=$(ARCH) go test -cover -race ./pkg/... ./cmd/...

.PHONY: fmt
fmt: ## Run golangci-lint formatter.
	$(GOLANGCI_LINT) fmt

.PHONY: lint
lint: ## Run golangci-lint linter.
	$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: ## Run golangci-lint linter and perform fixes.
	$(GOLANGCI_LINT) run --fix

.PHONY: lint-logging
lint-logging: ## Run logcheck linter to verify the logging practices.
	$(LOGCHECK) ./... || (echo 'Fix structured logging' && exit 1)

.PHONY: lint-manifests
lint-manifests: ## Run kube-linter on Kubernetes manifests.
	$(KUSTOMIZE) build config/default |\
		$(KUBE_LINTER) lint --config=./config/.kube-linter.yaml -

.PHONY: verify-licenses
verify-licenses: ## Run addlicense to verify if files have license headers.
	find -type f -name "*.go" ! -path "*/vendor/*" | xargs $(ADDLICENSE) -check || (echo 'Run "make update"' && exit 1)

.PHONY: add-licenses
add-licenses: ## Run addlicense to append license headers to files missing one.
	find -type f -name "*.go" ! -path "*/vendor/*" | xargs $(ADDLICENSE) -c "The Kubernetes Authors."

##@ Dependencies

## Tool Binaries
GOTOOLCMD     := go tool -modfile=hack/tools/go.mod
ADDLICENSE    ?= $(GOTOOLCMD) github.com/google/addlicense
GOLANGCI_LINT ?= $(GOTOOLCMD) github.com/golangci/golangci-lint/v2/cmd/golangci-lint
KUBE_LINTER   ?= $(GOTOOLCMD) golang.stackrox.io/kube-linter/cmd/kube-linter
KUSTOMIZE     ?= $(GOTOOLCMD) sigs.k8s.io/kustomize/kustomize/v5
LOGCHECK      ?= $(GOTOOLCMD) sigs.k8s.io/logtools/logcheck
